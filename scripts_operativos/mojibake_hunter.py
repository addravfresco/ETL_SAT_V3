"""
Módulo de perfilado de datos y aislamiento de anomalías de codificación (Mojibake).

Ejecuta un análisis forense sobre campos de texto libre para identificar patrones 
de caracteres corruptos. Emplea lectura fragmentada (Batched) e iteración controlada 
para procesar archivos masivos previniendo el desbordamiento de memoria (Out-of-Memory).
"""

import sys
from pathlib import Path
from typing import Dict, List

import polars as pl

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE
from pkg.globals import SAT_RAW_DIR


def cazar_mojibake(nombre_archivo: str) -> None:
    """
    Ejecuta el escaneo de anomalías de codificación sobre un archivo crudo.

    Filtra patrones previamente conocidos y consolida la frecuencia de nuevas
    palabras corruptas, exportando un artefacto CSV para su revisión forense.

    Args:
        nombre_archivo (str): Nombre físico del archivo con extensión.

    Raises:
        SystemExit: Si el archivo no es localizado o falla la inicialización del lector.
    """
    print("\n[INFO] Iniciando escaneo forense multi-columna (Modo Lotes)...")
    
    # 1. Resolución de ruta y validación de existencia
    archivo_sat = SAT_RAW_DIR / nombre_archivo
    if not archivo_sat.exists():
        print(f"[ERROR] Recurso no localizado en la ruta de red: {archivo_sat}")
        sys.exit(1)

    print(f"[INFO] Artefacto en análisis: {archivo_sat.name}")

    # Configuración de los campos susceptibles a texto libre no estructurado
    columnas_texto: List[str] = [
        "ConceptoNoIdentificacion", 
        "ConceptoUnidad",
        "ConceptoDescripcion",
        "EmisorNombre",
        "ReceptorNombre",
        "CondicionesDePago"
    ]

    # 2. Carga de patrones conocidos (Lista de exclusión)
    palabras_conocidas = [palabra.upper() for palabra in REEMPLAZOS_MOJIBAKE.keys()]
    print(f"[INFO] Se excluirán {len(palabras_conocidas):,} patrones preexistentes en cleaning_rules.py")

    # Expresión regular vectorizada para captura de caracteres anómalos
    regex_corrupcion = r"(?i)\b\w*[\ufffdÃÐðƑâÂ˜¨´™&¿½¡\?#]+\w*\b"

    # 3. Inicialización del lector en lotes para procesamiento Out-of-Core
    try:
        reader = pl.read_csv_batched(
            str(archivo_sat),
            separator="|",
            encoding="utf8-lossy",
            ignore_errors=True,
            infer_schema_length=0,
            quote_char=None,
            truncate_ragged_lines=True,
            batch_size=100_000
        )
    except Exception as e:
        print(f"[CRITICAL ERROR] Fallo al inicializar el lector de Polars: {e}")
        sys.exit(1)

    # Estructura de consolidación de frecuencias en memoria principal
    frecuencias_globales: Dict[str, int] = {}
    lotes_procesados = 0

    print("[INFO] Procesando bloques de 100,000 registros. Esta operación puede demorar...")

    # 4. Iteración y extracción de anomalías por bloque
    while True:
        batches = reader.next_batches(1)
        if not batches:
            break
            
        df_batch = batches[0]
        lotes_procesados += 1
        
        columnas_presentes = [col for col in columnas_texto if col in df_batch.columns]
        
        if not columnas_presentes:
            continue

        # Transformación relacional: Unpivot, filtrado Regex y agregación de frecuencias
        resultado_parcial = (
            df_batch
            .select(columnas_presentes)
            .unpivot(on=columnas_presentes, value_name="TextoLibre")
            .drop_nulls("TextoLibre")
            .with_columns(
                pl.col("TextoLibre").str.extract_all(regex_corrupcion).alias("Palabras_Rotas")
            )
            .explode("Palabras_Rotas")
            .drop_nulls("Palabras_Rotas")
            .with_columns(
                pl.col("Palabras_Rotas").str.to_uppercase()
            )
            .filter(~pl.col("Palabras_Rotas").is_in(palabras_conocidas))
            .group_by("Palabras_Rotas")
            .agg(pl.len().alias("Frecuencia"))
        )

        # Inyección de las frecuencias parciales al diccionario maestro
        for fila in resultado_parcial.iter_rows():
            palabra, cantidad = fila
            frecuencias_globales[palabra] = frecuencias_globales.get(palabra, 0) + cantidad
            
        # Telemetría básica para monitoreo de actividad
        if lotes_procesados % 10 == 0:
            print(f"   ... Lotes procesados: {lotes_procesados} ({(lotes_procesados * 100_000):,} filas)")

    # 5. Consolidación de resultados y exportación del artefacto forense
    if frecuencias_globales:
        df_final = pl.DataFrame({
            "Palabras_Rotas": list(frecuencias_globales.keys()),
            "Frecuencia": list(frecuencias_globales.values())
        }).sort("Frecuencia", descending=True)
    else:
        df_final = pl.DataFrame({"Palabras_Rotas": [], "Frecuencia": []})
    
    ruta_salida = f"candidatos_mojibake_{Path(nombre_archivo).stem}.csv"
    df_final.write_csv(ruta_salida)
    
    print(f"\n[INFO] Escaneo finalizado. Lotes auditados: {lotes_procesados}.")
    print(f"[RESULTADO] Se aislaron {len(df_final):,} candidatos sospechosos.")
    print(f"[INFO] Artefacto exportado: {ruta_salida}\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("[WARN] Uso de CLI incorrecto.")
        print("[INFO] Sintaxis esperada: python mojibake_hunter.py <nombre_del_archivo.csv>")
        print("[INFO] Ejemplo: python mojibake_hunter.py GERG_AECF_1891_Anexo2B.CSV")
    else:
        archivo_objetivo = sys.argv[1]
        cazar_mojibake(archivo_objetivo)