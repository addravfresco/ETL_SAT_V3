"""
Módulo vectorizado de saneamiento, normalización y Data Quality.
"""

from typing import List, Tuple

import polars as pl
import polars.selectors as cs

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Ejecuta el ciclo completo de Data Quality y limpieza sobre un lote de datos.
    
    Implementa el principio de "Espejo Milimétrico": el DataFrame original no 
    sufre pérdida de registros. Los datos anómalos (Hard/Soft Rejects) se auditan
    en un flujo paralelo (cuarentena).

    Args:
        df (pl.DataFrame): Lote de datos crudos extraídos de la fuente.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: 
            - df_sano: DataFrame con la limpieza vectorizada aplicada (100% volumetría).
            - df_cuarentena: DataFrame exclusivo con registros anómalos y su "Motivo_Rechazo".
    """
    if df.is_empty():
        return df, df.with_columns(pl.lit("").alias("Motivo_Rechazo")).clear()

    df_sano = df
    # Acumulador en lista para evitar fragmentación de memoria por pl.concat iterativos
    cuarentena_dfs: List[pl.DataFrame] = []

    # =========================================================================
    # FASE 1: Normalización Base (Vectorizada con Selectores)
    # =========================================================================
    df_sano = df_sano.with_columns(
        cs.string().str.strip_chars().str.to_uppercase()
    )

    # =========================================================================
    # FASE 2 y 3: Identificación Sensible y Limpieza Quirúrgica (Mojibake)
    # =========================================================================
    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "CONDICIONES"]
    
    cols_a_limpiar = [
        c for c in df_sano.columns 
        if any(key in c.upper() for key in palabras_clave) and df_sano.schema[c] == pl.String
    ]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&\?#]"

        df_sano = df_sano.with_columns(
            [
                pl.when(pl.col(c).str.contains(regex_basura))
                .then(pl.col(c).str.replace_many(patrones_sucios, valores_limpios))
                .otherwise(pl.col(c))
                .alias(c)
                for c in cols_a_limpiar
            ]
        )

    # =========================================================================
    # FASE 4: HARD REJECTS (Violaciones Estructurales)
    # =========================================================================
    if "UUID" in df_sano.columns:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        df_uuid_malos = df_sano.filter(
            ~pl.col("UUID").str.contains(regex_uuid)
        ).with_columns(
            pl.lit("[HARD] Violación de Integridad: UUID Inválido o Roto").alias("Motivo_Rechazo")
        )
        if not df_uuid_malos.is_empty():
            cuarentena_dfs.append(df_uuid_malos)

    if "EmisorRFC" in df_sano.columns:
        df_rfc_vacio = df_sano.filter(
            pl.col("EmisorRFC").is_null() | (pl.col("EmisorRFC") == "")
        ).with_columns(
            pl.lit("[HARD] Dato Huérfano: EmisorRFC Vacío").alias("Motivo_Rechazo")
        )
        if not df_rfc_vacio.is_empty():
            cuarentena_dfs.append(df_rfc_vacio)

    # =========================================================================
    # FASE 5: SOFT REJECTS (Advertencias de Negocio)
    # =========================================================================
    if "EmisorRFC" in df_sano.columns:
        # Se conserva la compatibilidad con la 'Ñ' por diseño de negocio
        regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
        df_rfc_invalido = df_sano.filter(
            ~pl.col("EmisorRFC").str.contains(regex_rfc)
        ).with_columns(
            pl.lit("[SOFT] Formato de EmisorRFC Anómalo").alias("Motivo_Rechazo")
        )
        if not df_rfc_invalido.is_empty():
            cuarentena_dfs.append(df_rfc_invalido)

    if "TipoDeComprobante" in df_sano.columns:
        df_tc_invalido = df_sano.filter(
            ~pl.col("TipoDeComprobante").is_in(["I", "E", "T", "P", "N"])
        ).with_columns(
            pl.lit("[SOFT] Tipo de Comprobante Desconocido").alias("Motivo_Rechazo")
        )
        if not df_tc_invalido.is_empty():
            cuarentena_dfs.append(df_tc_invalido)

    if "Moneda" in df_sano.columns:
        df_moneda_rara = df_sano.filter(
            pl.col("Moneda").is_not_null() & (pl.col("Moneda").str.len_chars() > 3)
        ).with_columns(
            pl.lit("[SOFT] Moneda Anómala (No ISO 4217)").alias("Motivo_Rechazo")
        )
        if not df_moneda_rara.is_empty():
            cuarentena_dfs.append(df_moneda_rara)

    if "FechaEmision" in df_sano.columns and "FechaCancelacion" in df_sano.columns:
        df_fechas_malas = df_sano.filter(
            pl.col("FechaCancelacion").is_not_null() & 
            (pl.col("FechaCancelacion") != "") & 
            (pl.col("FechaCancelacion") < pl.col("FechaEmision"))
        ).with_columns(
            pl.lit("[SOFT] Inconsistencia: Fecha Cancelación anterior a Emisión").alias("Motivo_Rechazo")
        )
        if not df_fechas_malas.is_empty():
            cuarentena_dfs.append(df_fechas_malas)

    # Evaluación Numérica
    cols_monetarias = [
        "Descuento", "SubTotal", "Total", "TrasladosIVA", "TrasladosIEPS", 
        "TotalImpuestosTrasladados", "RetenidosIVA", "RetenidosISR", 
        "TotalImpuestosRetenidos", "TipoCambio"
    ]
    
    LIMITE_SEGURO = 999999999999.0 
    columnas_presentes = [c for c in cols_monetarias if c in df_sano.columns]

    if columnas_presentes:
        condicion_desbordamiento = pl.any_horizontal(
            [pl.col(c).cast(pl.Float64, strict=False).abs() > LIMITE_SEGURO for c in columnas_presentes]
        )
        filas_culpables = df_sano.filter(condicion_desbordamiento).with_columns(
            pl.lit("[SOFT] Alerta de Desbordamiento Aritmético (Trillones)").alias("Motivo_Rechazo")
        )
        
        if not filas_culpables.is_empty():
            cuarentena_dfs.append(filas_culpables)

    # =========================================================================
    # FASE 6: Ensamblaje Final de Cuarentena (Alto Rendimiento)
    # =========================================================================
    if cuarentena_dfs:
        df_cuarentena = pl.concat(cuarentena_dfs, how="diagonal_relaxed")
    else:
        df_cuarentena = df_sano.clear().with_columns(pl.lit("").alias("Motivo_Rechazo"))

    return df_sano, df_cuarentena