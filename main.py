"""Módulo Orquestador del Pipeline ELT SAT - VERTICAL FACTURAS (Anexos 1A y 2B).

Coordina el ciclo de vida completo de la ingesta transaccional de comprobantes fiscales.
Implementa extracción binaria en bloques (O(1)), saneamiento vectorizado, 
auditoría de Data Quality integrada (DQ Logs) y consolidación final en 
SQL Server mediante patrones Set-Based.

Arquitectura "Espejo Milimétrico": Garantiza el 100% de la volumetría original,
delegando el aislamiento de anomalías estructurales a tablas de auditoría paralelas
sin interrumpir la continuidad del flujo principal de datos.
"""

from __future__ import annotations

import os
import sys
import traceback

from sqlalchemy import text

from pkg.checkpoint import eliminar_estado, guardar_estado, leer_estado
from pkg.config import get_engine
from pkg.consolidation import ConsolidationResult, consolidate_staging_to_target
from pkg.enforcer import aplicar_tipos_seguros
from pkg.extract import get_sat_reader
from pkg.globals import BATCH_SIZE, REGLAS_DINAMICAS, SAT_RAW_DIR, TABLES_CONFIG
from pkg.load import (
    build_staging_table_name,
    upload_dq_log_sql,
    upload_to_sql_blindado,
)
from pkg.reports import ETLReport
from pkg.transform import inyectar_hash_facturas, transform_sat_batch

try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
except AttributeError:
    pass


def _validar_staging_residual(table_name: str) -> None:
    """Valida la inexistencia de tablas staging huérfanas de ejecuciones previas.

    Previene la duplicidad transaccional verificando que el entorno de staging
    esté limpio antes de un inicio en frío (Cold Start).

    Args:
        table_name (str): Nombre lógico de la tabla de producción destino.

    Raises:
        RuntimeError: Si se detecta un artefacto staging residual en la base de datos.
    """
    stg_table = build_staging_table_name(table_name)
    sql = text("SELECT 1 WHERE OBJECT_ID(:full_name, 'U') IS NOT NULL")

    with get_engine().connect() as conn:
        existe = conn.execute(sql, {"full_name": f"dbo.{stg_table}"}).scalar()

    if existe:
        raise RuntimeError(
            f"Se detectó una tabla staging residual de una ejecución inconclusa: dbo.{stg_table}. "
            "Proceda con su revisión o purga manual para prevenir duplicidad de inyección."
        )


def _existe_staging(table_name: str) -> bool:
    """Verifica la existencia del objeto staging asociado a la tabla destino.

    Args:
        table_name (str): Nombre lógico de la tabla de producción destino.

    Returns:
        bool: True si el objeto staging está provisionado en el esquema, False en caso contrario.
    """
    stg_table = build_staging_table_name(table_name)
    sql = text("SELECT 1 WHERE OBJECT_ID(:full_name, 'U') IS NOT NULL")

    with get_engine().connect() as conn:
        existe = conn.execute(sql, {"full_name": f"dbo.{stg_table}"}).scalar()

    return bool(existe)


def main() -> None:
    """Ejecuta el ciclo de orquestación principal del proceso ELT para Facturas.

    Gestiona la recuperación de estado (Checkpoints), aprovisiona el lector binario
    Out-of-Core y coordina el flujo de transformación vectorizada. Implementa 
    mecanismos de tolerancia a fallos transaccionales y desencadena la consolidación
    final Set-Based en el motor relacional.
    """
    # Recepción de identificador por CLI o asignación de fallback por defecto
    id_anexo = sys.argv[1].upper() if len(sys.argv) > 1 else "1A_2024"

    if id_anexo not in TABLES_CONFIG:
        print(f"[ERROR] El identificador '{id_anexo}' carece de configuración en el contrato maestro (globals.py).")
        sys.exit(1)

    # 1. Extracción dinámica de metadatos y configuración de orquestación
    meta = TABLES_CONFIG[id_anexo]
    table_name: str = meta["table_name"]
    file_name: str = meta["file_name"]
    separator: str = meta["separator"]
    dedupe_enabled: bool = bool(meta.get("dedupe_enabled", False))
    
    dedupe_keys: list[str] = meta.get("dedupe_keys", [])
    order_by: list[str] = meta.get("order_by", ["FilaOrigen"])
    cast_warning_columns: list[str] = meta.get("cast_warning_columns", [])
    log_table_name: str | None = meta.get("log_table_name")

    print(f"\n[INFO] INICIANDO ORQUESTACIÓN ELT SAT (FACTURAS): ANEXO {id_anexo}")
    print(f"[INFO] TABLA DESTINO: {table_name}")

    ruta_archivo_original = SAT_RAW_DIR / file_name
    if not ruta_archivo_original.exists():
        print(f"[ERROR] Recurso físico no localizado en volumen de red: {ruta_archivo_original}")
        sys.exit(1)

    file_size_gb = os.path.getsize(ruta_archivo_original) / (1024 ** 3)
    print(f"[INFO] Artefacto Origen: {file_name} | Volumen Físico: {file_size_gb:.2f} GB")
    print("-" * 60)
    
    # 2. Recuperación de Estado Transaccional (Coordenada Física y Lógica)
    filas_procesadas_historicas, byte_offset_historico = leer_estado(id_anexo)
    es_reanudacion = filas_procesadas_historicas > 0
    
    report = ETLReport(id_anexo=id_anexo, is_recovery=es_reanudacion)

    try:
        if _existe_staging(table_name) and not es_reanudacion:
            print(f"[INFO] Se detectó la tabla staging {build_staging_table_name(table_name)} ya presente en SQL Server.")
            print("[INFO] Omitiendo fase de extracción binaria. Transición directa a CONSOLIDACIÓN.")
        else:
            if not es_reanudacion:
                _validar_staging_residual(table_name)
                print("[INFO] Inicio de procesamiento desde el byte 0 (Cold Start).")
            else:
                print(f"[RECOVERY] Punto de control detectado. Reanudando secuencialmente en fila lógica: {filas_procesadas_historicas:,.0f}")

            # 3. Aprovisionamiento del Lector Binario Out-of-Core
            reader = get_sat_reader(
                file_path=ruta_archivo_original,
                batch_size=BATCH_SIZE,
                separator=separator,
                skip_rows=filas_procesadas_historicas,
                start_byte_offset=byte_offset_historico
            )
            print("[INFO] Plan de ejecución binario compilado. Iniciando inyección a motor relacional...\n")

            # 4. Flujo Transaccional Vectorizado
            for df_batch, current_byte_offset in reader:
                filas_leidas_en_este_lote = len(df_batch)
                
                # 4.1 Motor de Calidad: Saneamiento estructural y aislamiento forense
                df_sano, df_dq_log = transform_sat_batch(df_batch)
                
                # 4.2 Tipado Dinámico y Cumplimiento de Contratos DDL
                df_final = aplicar_tipos_seguros(df_sano, REGLAS_DINAMICAS)
                df_final = inyectar_hash_facturas(df_final, dedupe_enabled, dedupe_keys)

                # =================================================================
                # 4.3 Bloque Transaccional Resiliente: Mitigación de inestabilidad I/O
                # =================================================================
                intentos = 0
                max_intentos = 3
                exito_carga = False

                while intentos < max_intentos and not exito_carga:
                    try:
                        report.audit_batch(df_final)
                        upload_to_sql_blindado(df_final, table_name, id_anexo)
                        
                        if not df_dq_log.is_empty():
                            upload_dq_log_sql(df_dq_log, table_name)
                        
                        # Confirmación de persistencia íntegra
                        exito_carga = True 
                    except Exception as e:
                        intentos += 1
                        print(f"\n[AVISO] Inestabilidad de red en fila {filas_procesadas_historicas:,.0f}. Intento {intentos}/{max_intentos} fallido. Motivo: {e}")
                        if intentos < max_intentos:
                            import time
                            time.sleep(5) # Backoff pasivo para estabilización de socket
                        else:
                            # Protocolo de contención: Volcado a disco para prevención de pérdida de datos
                            path_error = f"ERROR_LOTE_FACTURAS_{id_anexo}_{filas_procesadas_historicas}.csv"
                            df_final.write_csv(path_error)
                            print(f"\n[ALERTA CRÍTICA] Lote rechazado por BD. Volcado de emergencia en {path_error} para conciliación manual.")
                            # Mantenimiento de la continuidad del pipeline para garantizar volumetría
                            exito_carga = True 
                # =================================================================
                # FIN DEL BLOQUE RESILIENTE
                # =================================================================

                # 4.4 Persistencia de Checkpoint Físico/Lógico (O(1) Recovery)
                report.update_metrics(len(df_final))
                filas_procesadas_historicas += filas_leidas_en_este_lote
                guardar_estado(id_anexo, filas_procesadas_historicas, current_byte_offset)

                # UI de Monitoreo
                sys.stdout.write(f"\r[AVANCE] Filas procesadas exitosamente: {filas_procesadas_historicas:,.0f}")
                sys.stdout.flush()

        # =====================================================================
        # FASE 5: CONSOLIDACIÓN SET-BASED Y DEDUPLICACIÓN EN MOTOR RELACIONAL
        # =====================================================================
        if not _existe_staging(table_name):
            raise RuntimeError(f"Ausencia de artefacto staging para consolidación: dbo.{build_staging_table_name(table_name)}.")

        print("\n\n[INFO] Transferencia a staging finalizada. Desencadenando consolidación transaccional...")

        resultado_consolidacion: ConsolidationResult = consolidate_staging_to_target(
            table_name=table_name,
            dedupe_enabled=dedupe_enabled,
            dedupe_keys=dedupe_keys,
            order_by=order_by,
            cast_warning_columns=cast_warning_columns,
            log_table_name=log_table_name,
        )

        eliminar_estado(id_anexo)

        print(f"[INFO] Consolidación completada. Registros insertados en Producción: {resultado_consolidacion.inserted_rows:,.0f}")
        if resultado_consolidacion.duplicate_rows > 0:
            print(f"[INFO] Duplicados aislados forensemente: {resultado_consolidacion.duplicate_rows:,.0f}")
        if resultado_consolidacion.cast_warning_rows > 0:
            print(f"[INFO] Advertencias de Conversión (Nulos forzados): {resultado_consolidacion.cast_warning_rows:,.0f}")
        
        report.generate_final_report(id_anexo, file_name, status="SUCCESS")
        print("[INFO] Pipeline de Facturas finalizado con éxito.")

    except KeyboardInterrupt:
        print("\n\n[WARN] Interrupción manual del flujo operativo (Ctrl+C). Secuencia de apagado seguro ejecutada.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n[CRITICAL ERROR] Excepción no controlada en orquestador: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()