"""Módulo Core de Transformación y Data Quality (SAT_V2) - VERTICAL FACTURAS.

Ejecuta el ciclo de validación y limpieza estructural (Espejo Milimétrico).
Fusiona la resiliencia contra Schema Drift (búsqueda flexible de columnas) 
con una gestión ultra-eficiente de memoria RAM (Proyección Temprana) para aislar 
anomalías fiscales en la bitácora analítica (DLQ).
"""

from typing import List, Tuple

import polars as pl
import polars.selectors as cs

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def _get_col_name(df: pl.DataFrame, target_name: str) -> str | None:
    """Busca el identificador físico de una columna mediante coincidencia case-insensitive."""
    for col in df.columns:
        if col.upper() == target_name.upper():
            return col
    return None


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Ejecuta el ciclo de Data Quality sobre el lote en tránsito (Out-of-Core)."""
    if df.is_empty():
        schema_dq = {"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8}
        return df, pl.DataFrame(schema=schema_dq)

    df_sano = df
    alertas: List[pl.DataFrame] = []

    # =========================================================================
    # FASE 1: Normalización Base Vectorizada (Resistente a Schema Drift)
    # =========================================================================
    acentos = ["Á", "É", "Í", "Ó", "Ú", "Ä", "Ë", "Ï", "Ö", "Ü", "Â", "Ê", "Î", "Ô", "Û"]
    limpias = ["A", "E", "I", "O", "U", "A", "E", "I", "O", "U", "A", "E", "I", "O", "U"]

    df_sano = df_sano.with_columns(
        cs.string()
        .str.replace_all('"', '')  
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"ÃA", "IA")
        .str.replace_all(r"ÃO", "IO")
        .str.replace_many(acentos, limpias)
    ).with_columns(
        pl.when(cs.string() == "").then(None).otherwise(cs.string()).name.keep()
    )

    palabras_clave = ["NOMBRE", "CONCEPTO", "DESCRIPCION", "CONDICIONES"]
    cols_a_limpiar = [c for c in df_sano.columns if any(key in c.upper() for key in palabras_clave) and df_sano.schema[c] == pl.Utf8]

    if cols_a_limpiar:
        patrones_sucios = list(REEMPLAZOS_MOJIBAKE.keys())
        valores_limpios = list(REEMPLAZOS_MOJIBAKE.values())
        regex_basura = r"[\ufffdÃÐðƑâÂ˜¨´™&\?#]"

        df_sano = df_sano.with_columns([
            pl.when(pl.col(c).str.contains(regex_basura))
            .then(pl.col(c).str.replace_many(patrones_sucios, valores_limpios))
            .otherwise(pl.col(c)).alias(c)
            for c in cols_a_limpiar
        ])

    # =========================================================================
    # FASE 2: HARD REJECTS (Violaciones Estructurales Críticas)
    # =========================================================================
    c_uuid = _get_col_name(df_sano, "UUID")
    if c_uuid:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        malos = df_sano.filter(~pl.col(c_uuid).str.contains(regex_uuid) & pl.col(c_uuid).is_not_null())
        if not malos.is_empty():
            alertas.append(malos.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("UUID Inválido o Roto").alias("Regla_Rota")]))

    c_rfc = _get_col_name(df_sano, "EMISORRFC")
    if c_rfc:
        vacios = df_sano.filter(pl.col(c_rfc).is_null())
        if not vacios.is_empty():
            alertas.append(vacios.select([pl.col("FilaOrigen"), pl.lit("HARD").alias("Severidad"), pl.lit("Dato Huérfano: EmisorRFC Vacío").alias("Regla_Rota")]))

    # =========================================================================
    # FASE 3: SOFT REJECTS (Advertencias de Negocio y Reglas Contables)
    # =========================================================================
    regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
    
    if c_rfc:
        inv = df_sano.filter(pl.col(c_rfc).is_not_null() & ~pl.col(c_rfc).str.contains(regex_rfc))
        if not inv.is_empty():
            alertas.append(inv.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Formato de EmisorRFC Anómalo").alias("Regla_Rota")]))

    c_rec = _get_col_name(df_sano, "RECEPTORRFC")
    if c_rec:
        inv_rec = df_sano.filter(pl.col(c_rec).is_not_null() & ~pl.col(c_rec).str.contains(regex_rfc))
        if not inv_rec.is_empty():
            alertas.append(inv_rec.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Formato de ReceptorRFC Anómalo").alias("Regla_Rota")]))

    c_tc = _get_col_name(df_sano, "TIPODECOMPROBANTE")
    if c_tc:
        tc_inv = df_sano.filter(pl.col(c_tc).is_not_null() & ~pl.col(c_tc).is_in(["I", "E", "T", "P", "N"]))
        if not tc_inv.is_empty():
            alertas.append(tc_inv.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Tipo de Comprobante Desconocido").alias("Regla_Rota")]))

    c_moneda = _get_col_name(df_sano, "MONEDA")
    c_tipocambio = _get_col_name(df_sano, "TIPOCAMBIO")
    if c_moneda:
        mon_rara = df_sano.filter(pl.col(c_moneda).is_not_null() & (pl.col(c_moneda).str.len_chars() > 3))
        if not mon_rara.is_empty():
            alertas.append(mon_rara.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Moneda Anómala (No ISO 4217)").alias("Regla_Rota")]))
            
    if c_moneda and c_tipocambio:
        div_err = df_sano.filter((~pl.col(c_moneda).is_in(["MXN", "XXX", "MN"])) & (pl.col(c_tipocambio).cast(pl.Float64, strict=False).fill_null(1.0) <= 1.0))
        if not div_err.is_empty():
            alertas.append(div_err.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Moneda Extranjera sin Tipo de Cambio").alias("Regla_Rota")]))

    col_emision = _get_col_name(df_sano, "FECHAEMISION")
    col_cancelacion = _get_col_name(df_sano, "FECHACANCELACION")
    if col_emision and col_cancelacion:
        fechas_malas = df_sano.filter(
            pl.col(col_cancelacion).is_not_null() & 
            (pl.col(col_cancelacion).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date) < 
             pl.col(col_emision).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date))
        )
        if not fechas_malas.is_empty():
            alertas.append(fechas_malas.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Inconsistencia: Cancelación anterior a Emisión").alias("Regla_Rota")]))

    c_sub = _get_col_name(df_sano, "SUBTOTAL")
    c_des = _get_col_name(df_sano, "DESCUENTO")
    c_tras = _get_col_name(df_sano, "TOTALIMPUESTOSTRASLADADOS")
    c_ret = _get_col_name(df_sano, "TOTALIMPUESTOSRETENIDOS")
    c_tot = _get_col_name(df_sano, "TOTAL")

    if all([c_sub, c_des, c_tras, c_ret, c_tot]):
        calc_total = (
            pl.col(c_sub).cast(pl.Float64, strict=False).fill_null(0.0) - 
            pl.col(c_des).cast(pl.Float64, strict=False).fill_null(0.0) + 
            pl.col(c_tras).cast(pl.Float64, strict=False).fill_null(0.0) - 
            pl.col(c_ret).cast(pl.Float64, strict=False).fill_null(0.0)
        )
        desc = df_sano.filter(
            pl.col(c_tot).is_not_null() & ((calc_total - pl.col(c_tot).cast(pl.Float64, strict=False).fill_null(0.0)).abs() > 1.0)
        )
        if not desc.is_empty():
            alertas.append(desc.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Descuadre Aritmético > $1").alias("Regla_Rota")]))

    if c_tc and c_tot:
        tipo_monto = df_sano.filter(pl.col(c_tc).is_in(["P", "T"]) & (pl.col(c_tot).cast(pl.Float64, strict=False).fill_null(0.0) > 0))
        if not tipo_monto.is_empty():
            alertas.append(tipo_monto.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Pago/Traslado con Monto > 0").alias("Regla_Rota")]))

    cols_monetarias = ["Descuento", "SubTotal", "Total", "TrasladosIVA", "TrasladosIEPS", "TotalImpuestosTrasladados", "RetenidosIVA", "RetenidosISR", "TotalImpuestosRetenidos", "TipoCambio", "ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"]
    cols_pres = [c for c in df_sano.columns if _get_col_name(df_sano, c) in [m.upper() for m in cols_monetarias] or c in cols_monetarias]

    if cols_pres:
        cond_desb = pl.any_horizontal([
            pl.col(c).cast(pl.Utf8).str.replace_all(r"[^\d.-]", "").cast(pl.Float64, strict=False).abs() > 999999999999.0 
            for c in cols_pres
        ])
        filas_culpables = df_sano.filter(cond_desb)
        if not filas_culpables.is_empty():
            alertas.append(filas_culpables.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Alerta de Desbordamiento Aritmético (Trillones)").alias("Regla_Rota")]))

    c_cant = _get_col_name(df_sano, "CONCEPTOCANTIDAD")
    c_vunit = _get_col_name(df_sano, "CONCEPTOVALORUNITARIO")
    c_imp_con = _get_col_name(df_sano, "CONCEPTOIMPORTE")

    if all([c_cant, c_vunit, c_imp_con]):
        calc_imp = pl.col(c_cant).cast(pl.Float64, strict=False).fill_null(0.0) * pl.col(c_vunit).cast(pl.Float64, strict=False).fill_null(0.0)
        desc_con = df_sano.filter(
            pl.col(c_imp_con).is_not_null() & ((calc_imp - pl.col(c_imp_con).cast(pl.Float64, strict=False).fill_null(0.0)).abs() > 1.0)
        )
        if not desc_con.is_empty():
            alertas.append(desc_con.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Descuadre Concepto: Cantidad x ValorUnitario != Importe").alias("Regla_Rota")]))

    c_tipo_cont = _get_col_name(df_sano, "TIPOCONTRIBUYENTE")
    if c_rfc and c_tipo_cont:
        cong = df_sano.filter(
            pl.col(c_rfc).is_not_null() & pl.col(c_tipo_cont).is_not_null() &
            (((pl.col(c_tipo_cont) == "PM") & (pl.col(c_rfc).str.len_chars() != 12)) |
             ((pl.col(c_tipo_cont) == "PF") & (pl.col(c_rfc).str.len_chars() != 13)))
        )
        if not cong.is_empty():
            alertas.append(cong.select([pl.col("FilaOrigen"), pl.lit("SOFT").alias("Severidad"), pl.lit("Incongruencia: TipoContribuyente vs Longitud RFC").alias("Regla_Rota")]))

    # =========================================================================
    # FASE 4: Consolidación de Bitácora Analítica (DQ Log)
    # =========================================================================
    if alertas:
        df_dq_log = pl.concat(alertas)
    else:
        df_dq_log = pl.DataFrame(schema={"FilaOrigen": pl.Int64, "Severidad": pl.Utf8, "Regla_Rota": pl.Utf8})

    return df_sano, df_dq_log


def inyectar_hash_facturas(df: pl.DataFrame, dedupe_enabled: bool, dedupe_keys: list[str] = []) -> pl.DataFrame:
    """Calcula un identificador transaccional único (HashID)."""
    if not dedupe_enabled or not dedupe_keys:
        return df.with_columns(pl.col("FilaOrigen").hash().cast(pl.Utf8).alias("HashID"))
    
    llaves_validas = [c for c in dedupe_keys if c in df.columns]
    if not llaves_validas:
        return df.with_columns(pl.col("FilaOrigen").hash().cast(pl.Utf8).alias("HashID"))
        
    return df.with_columns(
        pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in llaves_validas])
        .hash().cast(pl.Utf8).alias("HashID")
    )