"""Módulo Core de Transformación, Data Quality y Saneamiento Vectorizado (SAT_V2)
Vertical: Facturación (Anexos 1A y 2B - Tablas A y B)

Este módulo actúa como el motor central de validación y limpieza del pipeline 
ETL para los comprobantes y anexos masivos del SAT. Está construido sobre la 
API de Polars para ejecutar transformaciones vectorizadas de ultra-alto rendimiento.
Implementa el patrón de diseño "Espejo Milimétrico", garantizando el paso 
de la volumetría total hacia destino mientras aísla metadatos de anomalías (Soft Rejects).
"""

from typing import List, Tuple

import polars as pl
import polars.selectors as cs

from pkg.cleaning_rules import REEMPLAZOS_MOJIBAKE


def inyectar_hash_facturas(df: pl.DataFrame, dedupe_enabled: bool, dedupe_keys: list[str] = []) -> pl.DataFrame:
    """Inyecta un identificador único (HashID) basado en atributos para control transaccional.

    Genera una firma criptográfica ligera utilizando la función nativa de Polars.
    Si la deduplicación está inactiva o faltan llaves, aplica el hash sobre el 
    índice físico (FilaOrigen) para cumplir con restricciones estructurales (NOT NULL) en SQL Server.

    Args:
        df (pl.DataFrame): Lote de datos sobre el cual se calculará la firma.
        dedupe_enabled (bool): Bandera de configuración de estrategia lógica de duplicados.
        dedupe_keys (list[str], optional): Lista de atributos que componen la llave natural.

    Returns:
        pl.DataFrame: Lote de datos con la nueva columna 'HashID' instanciada.
    """
    if not dedupe_enabled or not dedupe_keys:
        return df.with_columns(
            pl.col("FilaOrigen").hash().cast(pl.Utf8).alias("HashID")
        )
    else:
        llaves_validas = [c for c in dedupe_keys if c in df.columns]
        if not llaves_validas:
            return df.with_columns(pl.col("FilaOrigen").hash().cast(pl.Utf8).alias("HashID"))
            
        return df.with_columns(
            pl.concat_str([pl.col(c).cast(pl.Utf8).fill_null("") for c in llaves_validas])
            .hash()
            .cast(pl.Utf8)
            .alias("HashID")
        )


def _get_col_name(df: pl.DataFrame, target_name: str) -> str | None:
    """Busca el identificador físico de una columna mediante coincidencia case-insensitive.

    Args:
        df (pl.DataFrame): Estructura topológica a inspeccionar.
        target_name (str): Identificador lógico buscado.

    Returns:
        str | None: El nombre exacto de la columna preservando su capitalización original, 
        o None si el atributo no existe en el esquema.
    """
    for col in df.columns:
        if col.upper() == target_name.upper():
            return col
    return None


def transform_sat_batch(df: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """Ejecuta el ciclo de Data Quality y limpieza estructural sobre el lote en tránsito.

    Aplica un pipeline de 6 fases secuenciales operando bajo procesamiento Out-of-Core:
    1. Normalización tipográfica base.
    2. Identificación de Mojibake.
    3. Recuperación de caracteres (Limpieza Quirúrgica).
    4. Evaluación de integridad referencial dura (Hard Rejects).
    5. Evaluación de congruencia fiscal y aritmética (Soft Rejects).
    6. Consolidación de bitácora analítica.

    Args:
        df (pl.DataFrame): Bloque de datos extraído directamente del lector binario.

    Returns:
        Tuple[pl.DataFrame, pl.DataFrame]: Tupla que contiene:
            - El DataFrame principal con limpieza aplicada (100% de la volumetría original).
            - Un DataFrame colateral conteniendo exclusivamente la metadata de las reglas
              rotas y el identificador físico de las filas afectadas (Tabla de Cuarentena).
    """
    if df.is_empty():
        return df, df.with_columns([
            pl.lit("").alias("Severidad"), 
            pl.lit("").alias("Regla_Rota")
        ]).clear()

    df_sano = df
    cuarentena_dfs: List[pl.DataFrame] = []

    # =========================================================================
    # FASE 1: Normalización Base (Vectorizada mediante CS y sustituciones dirigidas)
    # =========================================================================
    acentos = ["Á", "É", "Í", "Ó", "Ú", "Ä", "Ë", "Ï", "Ö", "Ü", "Â", "Ê", "Î", "Ô", "Û"]
    limpias = ["A", "E", "I", "O", "U", "A", "E", "I", "O", "U", "A", "E", "I", "O", "U"]

    df_sano = df_sano.with_columns(
        cs.string()
        .str.replace_all('"', '')  
        .str.strip_chars()
        .str.to_uppercase()
        .str.replace_all(r"ÃA", "IA") # Sustitución dirigida para sufijos -ÍA (ej. SECRETARÍA)
        .str.replace_all(r"ÃO", "IO") # Sustitución dirigida para sufijos -ÍO (ej. ENVÍO)
        .str.replace_many(acentos, limpias) # Supresión global de tildes y diéresis
    )
    
    df_sano = df_sano.with_columns(
        pl.when(cs.string() == "")
        .then(None)
        .otherwise(cs.string())
        .name.keep()
    )

    # =========================================================================
    # FASE 2 y 3: Identificación Sensible y Recuperación de Cadenas (Mojibake)
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
    # FASE 4: HARD REJECTS (Violaciones Estructurales Críticas)
    # =========================================================================
    col_uuid = _get_col_name(df_sano, "UUID")
    if col_uuid:
        regex_uuid = r"^[A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12}$"
        df_uuid_malos = df_sano.filter(
            ~pl.col(col_uuid).str.contains(regex_uuid)
        ).with_columns([
            pl.lit("HARD").alias("Severidad"),
            pl.lit("UUID Inválido o Roto").alias("Regla_Rota")
        ])
        if not df_uuid_malos.is_empty():
            cuarentena_dfs.append(df_uuid_malos)

    col_rfc = _get_col_name(df_sano, "EMISORRFC")
    if col_rfc:
        df_rfc_vacio = df_sano.filter(
            pl.col(col_rfc).is_null() | (pl.col(col_rfc) == "")
        ).with_columns([
            pl.lit("HARD").alias("Severidad"),
            pl.lit("Dato Huérfano: EmisorRFC Vacío").alias("Regla_Rota")
        ])
        if not df_rfc_vacio.is_empty():
            cuarentena_dfs.append(df_rfc_vacio)

    # =========================================================================
    # FASE 5: SOFT REJECTS (Advertencias de Negocio y Reglas Contables)
    # =========================================================================
    # 5.1 Formato RFC (Evaluación de Estructura Alfanumérica)
    regex_rfc = r"^[A-Z&Ñ]{3,4}\d{6}[A-Z0-9]{3}$"
    
    if col_rfc:
        df_rfc_invalido = df_sano.filter(
            pl.col(col_rfc).is_not_null() & ~pl.col(col_rfc).str.contains(regex_rfc)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Formato de EmisorRFC Anómalo").alias("Regla_Rota")
        ])
        if not df_rfc_invalido.is_empty():
            cuarentena_dfs.append(df_rfc_invalido)

    col_rec = _get_col_name(df_sano, "RECEPTORRFC")
    if col_rec:
        df_rec_invalido = df_sano.filter(
            pl.col(col_rec).is_not_null() & ~pl.col(col_rec).str.contains(regex_rfc)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Formato de ReceptorRFC Anómalo").alias("Regla_Rota")
        ])
        if not df_rec_invalido.is_empty():
            cuarentena_dfs.append(df_rec_invalido)

    # 5.2 Dominio de Comprobante Fiscal
    col_tc = _get_col_name(df_sano, "TIPODECOMPROBANTE")
    if col_tc:
        df_tc_invalido = df_sano.filter(
            pl.col(col_tc).is_not_null() & ~pl.col(col_tc).is_in(["I", "E", "T", "P", "N"])
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Tipo de Comprobante Desconocido").alias("Regla_Rota")
        ])
        if not df_tc_invalido.is_empty():
            cuarentena_dfs.append(df_tc_invalido)

    # 5.3 Validación Conjunta de Divisas (ISO 4217) y Tipo de Cambio
    col_moneda = _get_col_name(df_sano, "MONEDA")
    c_tc = _get_col_name(df_sano, "TIPOCAMBIO")
    
    if col_moneda:
        df_moneda_rara = df_sano.filter(
            pl.col(col_moneda).is_not_null() & (pl.col(col_moneda).str.len_chars() > 3)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Moneda Anómala (No ISO 4217)").alias("Regla_Rota")
        ])
        if not df_moneda_rara.is_empty():
            cuarentena_dfs.append(df_moneda_rara)
            
    if col_moneda and c_tc:
        df_divisa_error = df_sano.filter(
            (~pl.col(col_moneda).is_in(["MXN", "XXX", "MN"])) & 
            (pl.col(c_tc).cast(pl.Float64, strict=False).fill_null(1.0) <= 1.0)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Moneda Extranjera sin Tipo de Cambio").alias("Regla_Rota")
        ])
        if not df_divisa_error.is_empty():
            cuarentena_dfs.append(df_divisa_error)

    # 5.4 Consistencia Cronológica 
    col_emision = _get_col_name(df_sano, "FECHAEMISION")
    col_cancelacion = _get_col_name(df_sano, "FECHACANCELACION")
    
    if col_emision and col_cancelacion:
        df_fechas_malas = df_sano.filter(
            pl.col(col_cancelacion).is_not_null() & 
            (pl.col(col_cancelacion) != "") & 
            # Evaluación truncada a Date para evadir discrepancias por zonas horarias del PAC
            (
                pl.col(col_cancelacion).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date) < 
                pl.col(col_emision).str.strptime(pl.Datetime, format="%Y-%m-%d %H:%M:%S", strict=False).cast(pl.Date)
            )
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Inconsistencia: Cancelación en DÍA anterior a Emisión").alias("Regla_Rota")
        ])
        if not df_fechas_malas.is_empty():
            cuarentena_dfs.append(df_fechas_malas)

    # 5.5 Cuadratura Aritmética de Cabeceras (Integridad de Factura)
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
        df_descuadre = df_sano.filter(
            pl.col(c_tot).is_not_null() & 
            ((calc_total - pl.col(c_tot).cast(pl.Float64, strict=False).fill_null(0.0)).abs() > 1.0)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Descuadre Aritmético > $1").alias("Regla_Rota")
        ])
        if not df_descuadre.is_empty():
            cuarentena_dfs.append(df_descuadre)

    # 5.6 Integridad Naturaleza Financiera (Complementos de Pago y Traslados)
    if col_tc and c_tot:
        df_tipo_monto_error = df_sano.filter(
            pl.col(col_tc).is_in(["P", "T"]) & 
            (pl.col(c_tot).cast(pl.Float64, strict=False).fill_null(0.0) > 0)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Pago/Traslado con Monto > 0").alias("Regla_Rota")
        ])
        if not df_tipo_monto_error.is_empty():
            cuarentena_dfs.append(df_tipo_monto_error)

    # 5.7 Prevención Transaccional de Desbordamiento Aritmético (Trillones)
    cols_monetarias = [
        "Descuento", "SubTotal", "Total", "TrasladosIVA", "TrasladosIEPS", 
        "TotalImpuestosTrasladados", "RetenidosIVA", "RetenidosISR", 
        "TotalImpuestosRetenidos", "TipoCambio",
        "ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"
    ]
    
    LIMITE_SEGURO = 999999999999.0 
    columnas_presentes = [c for c in df_sano.columns if _get_col_name(df_sano, c) in [m.upper() for m in cols_monetarias] or c in cols_monetarias]

    if columnas_presentes:
        condicion_desbordamiento = pl.any_horizontal(
            [
                pl.col(c)
                .cast(pl.Utf8)
                .str.replace_all(r"[^\d.-]", "")
                .cast(pl.Float64, strict=False)
                .abs() > LIMITE_SEGURO 
                for c in columnas_presentes
            ]
        )
        filas_culpables = df_sano.filter(condicion_desbordamiento).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Alerta de Desbordamiento Aritmético (Trillones)").alias("Regla_Rota")
        ])
        
        if not filas_culpables.is_empty():
            cuarentena_dfs.append(filas_culpables)

    # 5.8 Cuadratura Unitaria de Conceptos (Nivel Detalle)
    c_cant = _get_col_name(df_sano, "CONCEPTOCANTIDAD")
    c_vunit = _get_col_name(df_sano, "CONCEPTOVALORUNITARIO")
    c_imp_con = _get_col_name(df_sano, "CONCEPTOIMPORTE")

    if all([c_cant, c_vunit, c_imp_con]):
        calc_importe_concepto = (
            pl.col(c_cant).cast(pl.Float64, strict=False).fill_null(0.0) * pl.col(c_vunit).cast(pl.Float64, strict=False).fill_null(0.0)
        )
        df_descuadre_concepto = df_sano.filter(
            pl.col(c_imp_con).is_not_null() & 
            ((calc_importe_concepto - pl.col(c_imp_con).cast(pl.Float64, strict=False).fill_null(0.0)).abs() > 1.0)
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Descuadre Concepto: Cantidad x ValorUnitario != Importe").alias("Regla_Rota")
        ])
        if not df_descuadre_concepto.is_empty():
            cuarentena_dfs.append(df_descuadre_concepto)

    # 5.9 Congruencia Fiscal: Tipo de Persona (Física/Moral) vs Longitud RFC
    c_tipo_cont = _get_col_name(df_sano, "TIPOCONTRIBUYENTE")
    
    if col_rfc and c_tipo_cont:
        # Validación de reglas emitidas por el marco normativo del SAT (PM=12, PF=13)
        df_congruencia_rfc = df_sano.filter(
            pl.col(col_rfc).is_not_null() & pl.col(c_tipo_cont).is_not_null() &
            (
                ((pl.col(c_tipo_cont) == "PM") & (pl.col(col_rfc).str.len_chars() != 12)) |
                ((pl.col(c_tipo_cont) == "PF") & (pl.col(col_rfc).str.len_chars() != 13))
            )
        ).with_columns([
            pl.lit("SOFT").alias("Severidad"),
            pl.lit("Incongruencia: TipoContribuyente vs Longitud de RFC").alias("Regla_Rota")
        ])
        if not df_congruencia_rfc.is_empty():
            cuarentena_dfs.append(df_congruencia_rfc)

    # =========================================================================
    # FASE 6: Consolidación de Bitácora y Aislamiento Dinámico
    # =========================================================================
    if cuarentena_dfs:
        df_cuarentena = pl.concat(cuarentena_dfs, how="diagonal_relaxed").select(
            ["FilaOrigen", "Severidad", "Regla_Rota"]
        )
    else:
        df_cuarentena = df_sano.clear().with_columns([
            pl.lit("").alias("Severidad"), 
            pl.lit("").alias("Regla_Rota")
        ]).select(["FilaOrigen", "Severidad", "Regla_Rota"])

    return df_sano, df_cuarentena