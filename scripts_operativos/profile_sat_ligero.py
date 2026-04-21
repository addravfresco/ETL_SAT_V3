"""Módulo de perfilado estructural y volumétrico para esquemas masivos (Out-of-Core).

Provee herramientas analíticas de I/O optimizado para determinar la composición
topológica de columnas evadiendo el colapso de memoria (OOM) en archivos 
ultra-masivos (>100GB) alojados en unidades de red compartidas (SMB). Implementa 
estrategias de Streaming para evitar la descarga completa del recurso.
"""

import io
import sys
from typing import Any, Dict

import polars as pl

from pkg.globals import SAT_RAW_DIR


def profile_sat_table(nombre_archivo: str) -> None:
    """Ejecuta el escaneo de superficie mediante un pipeline de lectura optimizado.

    Implementa una extracción física limitada (Stream) utilizando un iterador nativo 
    de Python para aislar las primeras filas del archivo. Posteriormente, inyecta 
    dichas líneas en un objeto virtual (StringIO) permitiendo a Polars analizar la 
    estructura tabular directamente desde la memoria RAM, eliminando el overhead de 
    I/O sobre archivos compartidos por red y previniendo bloqueos transaccionales.

    Args:
        nombre_archivo (str): Nombre físico del archivo con extensión.

    Raises:
        SystemExit: Si el archivo objetivo no existe en la ruta de red configurada.
    """
    print("[INFO] INICIANDO PERFILADO FORENSE ESTRUCTURAL (STREAMING I/O)")
    print("=" * 135)

    # Resolución de la ruta de montaje y validación de disponibilidad
    ruta_completa = SAT_RAW_DIR / nombre_archivo
    
    if not ruta_completa.exists():
        print(f"[ERROR] Recurso no localizado en el directorio fuente: {ruta_completa}")
        sys.exit(1)

    print(f"\n[INFO] Analizando topología de: {nombre_archivo}...")
    print("[INFO] Aprovisionando lector de red (Stream) para protección de RAM...")

    try:
        # Aislamiento físico: Bypass de la lectura total del motor de DataFrames.
        # Se genera una tubería secuencial para extraer estrictamente el bloque de muestra.
        lineas_muestra = []
        
        with open(ruta_completa, "r", encoding="utf-8", errors="replace") as f:
            for _ in range(50):
                try:
                    lineas_muestra.append(next(f))
                except StopIteration:
                    break  
                    
        # Instanciación del objeto en memoria para emular acceso a disco local
        archivo_virtual = io.StringIO("".join(lineas_muestra))

        # El motor de inferencia de Polars opera exclusivamente sobre la porción en RAM
        df_batch = pl.read_csv(
            archivo_virtual,
            separator="|",
            infer_schema_length=0, 
            truncate_ragged_lines=True,
            ignore_errors=True,
            quote_char=None
        )

        # Conversión de artefactos de texto en nulos lógicos para métricas de perfilado
        df_batch = df_batch.with_columns(pl.all().replace("", None))

        columnas = df_batch.columns
        total_filas_global = len(df_batch)
        nulos_por_columna = {c: df_batch[c].null_count() for c in columnas}
        
        ejemplos: Dict[str, Any] = {}
        for col in columnas:
            ejemplo_df = df_batch[col].drop_nulls().head(1)
            if len(ejemplo_df) > 0:
                ejemplos[col] = str(ejemplo_df.item())

        print("\n[INFO] Escaneo de superficie (Streaming) finalizado exitosamente.")
        print(f"[RESULTADO] Muestra extraída: {total_filas_global:,} registros (Suficiente para Schema Drift).")
        print(f"[RESULTADO] Total de columnas detectadas: {len(columnas)}")
        
        # Consolidación del reporte tabular de estructura
        print(f"\n{'No.':<4} | {'Nombre de Columna':<35} | {'% Nulos':<10} | {'Muestra de Dato'}")
        print("-" * 135)

        for i, col in enumerate(columnas):
            pct_nulos = (nulos_por_columna[col] / total_filas_global * 100) if total_filas_global > 0 else 0
            
            ejemplo_str = ejemplos.get(col, "NULL")
            # Truncamiento defensivo para evitar la distorsión del renderizado en consola
            ejemplo_final = (ejemplo_str[:50] + '...') if len(ejemplo_str) > 50 else ejemplo_str

            print(f"{(i + 1):<4} | {col:<35} | {pct_nulos:>8.2f}% | {ejemplo_final}")

        # Sintetizador de catálogo estático para la configuración del contrato de datos (globals.py)
        print("\n" + "-" * 135)
        print("[INFO] CATÁLOGO ESTRUCTURAL PLANO (Compatible para auditoría de DDL/Diccionarios):")
        
        lista_formateada = ",\n    ".join([f'"{c}"' for c in columnas])
        print(f"[\n    {lista_formateada}\n]")
        print("-" * 135)    

    except Exception as e:
        print(f"[CRITICAL ERROR] Fallo irrecuperable de I/O al procesar {nombre_archivo}: {e}")

    print("\n" + "=" * 135)


if __name__ == "__main__":
    # Evaluación de la CLI para despliegue manual aislado
    if len(sys.argv) < 2:
        print("[WARN] Uso de CLI incorrecto.")
        print("[INFO] Sintaxis esperada: python scripts_operativos/profile_sat_ligero.py <nombre_del_archivo.csv>")
        print("[INFO] Ejemplo: python scripts_operativos/profile_sat_ligero.py AECF_0101_Anexo4.csv")
    else:
        archivo_objetivo = sys.argv[1]
        profile_sat_table(archivo_objetivo)