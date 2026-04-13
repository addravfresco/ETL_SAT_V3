"""
Módulo orquestador del Pipeline ELT SAT.
"""

from __future__ import annotations

import os
import sys
import time
import traceback

import polars as pl
from pkg.preflight import detect_and_convert_to_utf8
from sqlalchemy import text

from pkg.checkpoint import eliminar_estado, guardar_estado, leer_estado
from pkg.config import get_engine
from pkg.consolidation import ConsolidationResult, consolidate_staging_to_target
from pkg.enforcer import aplicar_tipos_seguros
from pkg.extract import get_sat_reader
from pkg.globals import BATCH_SIZE, REGLAS_DINAMICAS, SAT_RAW_DIR, TABLES_CONFIG
from pkg.load import (
    build_staging_table_name,
    upload_cuarentena_sql,
    upload_to_sql_blindado,
)
from pkg.reports import ETLReport
from pkg.transform import transform_sat_batch

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass


def _validar_staging_residual(table_name: str) -> None:
    stg_table = build_staging_table_name(table_name)
    sql = text("SELECT 1 WHERE OBJECT_ID(:full_name, 'U') IS NOT NULL")

    with get_engine().connect() as conn:
        existe = conn.execute(sql, {"full_name": f"dbo.{stg_table}"}).scalar()

    if existe:
        raise RuntimeError(
            f"Se detectó una tabla staging residual de una corrida previa: dbo.{stg_table}. "
            "Revísala antes de continuar para evitar mezclar ejecuciones."
        )


def _existe_staging(table_name: str) -> bool:
    stg_table = build_staging_table_name(table_name)
    sql = text("SELECT 1 WHERE OBJECT_ID(:full_name, 'U') IS NOT NULL")

    with get_engine().connect() as conn:
        existe = conn.execute(sql, {"full_name": f"dbo.{stg_table}"}).scalar()

    return bool(existe)


def main() -> None:
    id_anexo = sys.argv[1].upper() if len(sys.argv) > 1 else "1A_2024"

    if id_anexo not in TABLES_CONFIG:
        print(f"[ERROR] El identificador '{id_anexo}' carece de configuración.")
        sys.exit(1)

    meta = TABLES_CONFIG[id_anexo]
    table_name: str = meta["table_name"]
    file_name: str = meta["file_name"]
    separator: str = meta["separator"]
    dedupe_enabled: bool = bool(meta.get("dedupe_enabled", False))
    dedupe_keys: list[str] | None = meta.get("dedupe_keys")
    order_by: list[str] | None = meta.get("order_by", ["FilaOrigen"])
    cast_warning_columns: list[str] | None = meta.get("cast_warning_columns")
    log_table_name: str | None = meta.get("log_table_name")

    print(f"\n[INFO] INICIANDO ORQUESTACIÓN ELT SAT: ANEXO {id_anexo}")
    print(f"[INFO] TABLA DESTINO: {table_name}")

    ruta_archivo_original = SAT_RAW_DIR / file_name
    if not ruta_archivo_original.exists():
        print(f"[ERROR] Recurso físico no localizado: {ruta_archivo_original}")
        sys.exit(1)

    file_size_gb = os.path.getsize(ruta_archivo_original) / (1024 ** 3)
    print(f"[INFO] Artefacto Origen: {file_name} | Volumen Físico: {file_size_gb:.2f} GB")
    print("-" * 60)

    ruta_segura = detect_and_convert_to_utf8(ruta_archivo_original)
    filas_procesadas_historicas = leer_estado(id_anexo)
    es_reanudacion = filas_procesadas_historicas > 0
    
    report = ETLReport(id_anexo=id_anexo, is_recovery=es_reanudacion)

    try:
        if _existe_staging(table_name) and not es_reanudacion:
            print(f"[INFO] Se detectó la tabla staging {build_staging_table_name(table_name)} ya presente en SQL Server.")
            print("[INFO] Saltando fase de ingesta CSV y pasando directamente a CONSOLIDACIÓN.")
        else:
            t0_seek = 0.0
            if not es_reanudacion:
                _validar_staging_residual(table_name)
                print("[INFO] Inicio de procesamiento desde el registro 0 (Cold Start).")
                print("[INFO] Plan de ejecución perezoso compilado. Iniciando ingesta en streaming...")
            else:
                print(f"[RECOVERY] Punto de control detectado. Reanudando en fila: {filas_procesadas_historicas:,.0f}")
                print("[INFO] Plan de ejecución perezoso compilado. Iniciando búsqueda (Fast-Forward)...")
                t0_seek = time.time()

            try:
                reader = get_sat_reader(
                    file_path=ruta_segura,
                    batch_size=BATCH_SIZE,
                    separator=separator,
                    skip_rows=filas_procesadas_historicas,
                )

                is_first_batch = True

                for df_batch in reader:
                    if es_reanudacion and is_first_batch:
                        seek_time = time.time() - t0_seek
                        print(f"\n[AVANCE RÁPIDO] Saltando {filas_procesadas_historicas:,.0f} filas... | (Tiempo de búsqueda: {seek_time:.2f}s)")
                        print(f"[DEBUG] Motor Polars apuntando a: {ruta_segura}")
                        print(f"[INFO] ¡Motor posicionado en la fila {filas_procesadas_historicas:,.0f}! Reanudando ETL a máxima velocidad...\n")
                        is_first_batch = False

                    filas_leidas_en_este_lote = len(df_batch)
                    
                    df_processed, df_quarantine = transform_sat_batch(df_batch)
                    
                    # Log al CSV general
                    report.log_quarantine(df_quarantine, id_anexo)
                    
                    # Subir los Hard Rejects a SQL
                    upload_cuarentena_sql(df_quarantine, table_name)

                    # El 100% de la data pasa a la tabla principal
                    df_final = aplicar_tipos_seguros(df_processed, REGLAS_DINAMICAS)
                    report.audit_batch(df_final)

                    upload_to_sql_blindado(df_final, table_name, id_anexo)
                    report.update_metrics(len(df_final))

                    filas_procesadas_historicas += filas_leidas_en_este_lote
                    guardar_estado(id_anexo, filas_procesadas_historicas)

            except pl.exceptions.NoDataError:
                if filas_procesadas_historicas > 0 and _existe_staging(table_name):
                    print("[WARN] No hay más filas pendientes por leer en el CSV.")
                else:
                    raise

        if not _existe_staging(table_name):
            raise RuntimeError(f"No existe la tabla staging para consolidar: dbo.{build_staging_table_name(table_name)}.")

        print("\n[INFO] Carga a staging finalizada. Iniciando consolidación set-based en SQL Server...")

        resultado_consolidacion: ConsolidationResult = consolidate_staging_to_target(
            table_name=table_name,
            dedupe_enabled=dedupe_enabled,
            dedupe_keys=dedupe_keys,
            order_by=order_by,
            cast_warning_columns=cast_warning_columns,
            log_table_name=log_table_name,
        )

        eliminar_estado(id_anexo)

        print(f"[INFO] Consolidación completada. Insertadas: {resultado_consolidacion.inserted_rows:,.0f}")
        if resultado_consolidacion.duplicate_rows > 0:
            print(f"[INFO] Duplicados descartados: {resultado_consolidacion.duplicate_rows:,.0f}")
        
        report.generate_final_report(id_anexo, file_name, status="SUCCESS")
        print("[INFO] Pipeline finalizado con éxito.")

    except KeyboardInterrupt:
        print("\n[WARN] Interrupción manual por el operador.")
        sys.exit(1)
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()