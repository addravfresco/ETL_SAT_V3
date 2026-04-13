"""
Módulo de consolidación STG -> Producción.

Automatiza la fase final del patrón ELT delegando en SQL Server la
deduplicación opcional, conversión de tipos y migración masiva desde tablas
staging hacia tablas definitivas.

Incluye Minimal Logging (TABLOCK) para máxima velocidad en bases de datos
con modelo de recuperación SIMPLE o BULK_LOGGED.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from sqlalchemy import text

from pkg.config import get_engine
from pkg.load import build_staging_table_name


@dataclass(frozen=True)
class ColumnMeta:
    name: str
    data_type: str
    char_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    ordinal_position: int


@dataclass(frozen=True)
class ConsolidationResult:
    inserted_rows: int
    duplicate_rows: int
    cast_warning_rows: int
    log_table_name: str | None


def _quote_identifier(identifier: str) -> str:
    return f"[{identifier}]"


def _get_columns(table_name: str, schema_name: str = "dbo") -> List[ColumnMeta]:
    engine = get_engine()

    sql = text(
        """
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :schema_name
          AND TABLE_NAME = :table_name
        ORDER BY ORDINAL_POSITION
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(
            sql,
            {"schema_name": schema_name, "table_name": table_name},
        ).mappings().all()

    if not rows:
        raise ValueError(
            f"No fue posible recuperar el esquema de la tabla "
            f"{schema_name}.{table_name} desde INFORMATION_SCHEMA.COLUMNS."
        )

    return [
        ColumnMeta(
            name=row["COLUMN_NAME"],
            data_type=str(row["DATA_TYPE"]).lower(),
            char_length=row["CHARACTER_MAXIMUM_LENGTH"],
            numeric_precision=row["NUMERIC_PRECISION"],
            numeric_scale=row["NUMERIC_SCALE"],
            ordinal_position=row["ORDINAL_POSITION"],
        )
        for row in rows
    ]


def _get_column_meta_map(columns: List[ColumnMeta]) -> dict[str, ColumnMeta]:
    return {col.name.upper(): col for col in columns}


def _build_cast_expression(col: ColumnMeta) -> str:
    quoted_col = _quote_identifier(col.name)
    src = f"Src.{quoted_col}"
    src_as_text = f"CAST({src} AS NVARCHAR(4000))"
    cleaned_text = f"NULLIF(LTRIM(RTRIM({src_as_text})), '')"

    if col.data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
        return f"{src} AS {quoted_col}"

    if col.data_type == "date":
        return f"TRY_CONVERT(DATE, {src}, 23) AS {quoted_col}"

    if col.data_type in {"datetime", "datetime2", "smalldatetime"}:
        return f"TRY_CONVERT({col.data_type.upper()}, TRY_CONVERT(DATE, {src}, 23)) AS {quoted_col}"

    if col.data_type in {"decimal", "numeric"}:
        precision = col.numeric_precision or 18
        scale = col.numeric_scale or 4
        return f"TRY_CAST({cleaned_text} AS DECIMAL({precision},{scale})) AS {quoted_col}"

    if col.data_type in {"float", "real"}:
        return f"TRY_CAST({cleaned_text} AS FLOAT) AS {quoted_col}"

    if col.data_type == "bigint":
        return f"TRY_CAST({cleaned_text} AS BIGINT) AS {quoted_col}"

    if col.data_type == "int":
        return f"TRY_CAST({cleaned_text} AS INT) AS {quoted_col}"

    if col.data_type == "smallint":
        return f"TRY_CAST({cleaned_text} AS SMALLINT) AS {quoted_col}"

    if col.data_type == "tinyint":
        return f"TRY_CAST({cleaned_text} AS TINYINT) AS {quoted_col}"

    if col.data_type == "bit":
        return f"TRY_CAST({cleaned_text} AS BIT) AS {quoted_col}"

    if col.data_type == "uniqueidentifier":
        return f"TRY_CAST({src} AS UNIQUEIDENTIFIER) AS {quoted_col}"

    return f"{src} AS {quoted_col}"


def _build_cast_failure_condition(col: ColumnMeta) -> str:
    quoted_col = _quote_identifier(col.name)
    src = f"Src.{quoted_col}"
    src_as_text = f"CAST({src} AS NVARCHAR(4000))"
    cleaned_text = f"NULLIF(LTRIM(RTRIM({src_as_text})), '')"

    if col.data_type in {"varchar", "nvarchar", "char", "nchar", "text", "ntext"}:
        return "1 = 0"

    if col.data_type == "date":
        return f"({cleaned_text} IS NOT NULL AND TRY_CONVERT(DATE, {src}, 23) IS NULL)"

    if col.data_type in {"datetime", "datetime2", "smalldatetime"}:
        return f"({cleaned_text} IS NOT NULL AND TRY_CONVERT(DATE, {src}, 23) IS NULL)"

    if col.data_type in {"decimal", "numeric"}:
        precision = col.numeric_precision or 18
        scale = col.numeric_scale or 4
        return (
            f"({cleaned_text} IS NOT NULL AND "
            f"TRY_CAST({cleaned_text} AS DECIMAL({precision},{scale})) IS NULL)"
        )

    if col.data_type in {"float", "real"}:
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS FLOAT) IS NULL)"

    if col.data_type == "bigint":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS BIGINT) IS NULL)"

    if col.data_type == "int":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS INT) IS NULL)"

    if col.data_type == "smallint":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS SMALLINT) IS NULL)"

    if col.data_type == "tinyint":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS TINYINT) IS NULL)"

    if col.data_type == "bit":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({cleaned_text} AS BIT) IS NULL)"

    if col.data_type == "uniqueidentifier":
        return f"({cleaned_text} IS NOT NULL AND TRY_CAST({src} AS UNIQUEIDENTIFIER) IS NULL)"

    return "1 = 0"


def _build_cast_failure_detail_expression(warning_columns: List[ColumnMeta]) -> str:
    parts: list[str] = []

    for col in warning_columns:
        condition = _build_cast_failure_condition(col)
        parts.append(f"CASE WHEN {condition} THEN ',{col.name}' ELSE '' END")

    if not parts:
        return "CAST(NULL AS NVARCHAR(4000))"

    concatenated = " + ".join(parts)
    return f"NULLIF(STUFF({concatenated}, 1, 1, ''), '')"


def _build_log_table_sql(
    log_table_name: str,
    stg_table_name: str,
    schema_name: str = "dbo",
) -> str:
    quoted_schema = _quote_identifier(schema_name)
    quoted_log = f"{quoted_schema}.{_quote_identifier(log_table_name)}"
    quoted_stg = f"{quoted_schema}.{_quote_identifier(stg_table_name)}"

    return f"""
    IF OBJECT_ID(N'{schema_name}.{log_table_name}', 'U') IS NULL
    BEGIN
        SELECT TOP (0)
            CAST(NULL AS DATETIME2(0)) AS [FechaProceso],
            CAST(NULL AS NVARCHAR(200)) AS [Motivo_Log_SQL],
            CAST(NULL AS NVARCHAR(4000)) AS [Detalle_Log_SQL],
            *
        INTO {quoted_log}
        FROM {quoted_stg};
    END;
    """


def consolidate_staging_to_target(
    table_name: str,
    dedupe_enabled: bool,
    dedupe_keys: list[str] | None = None,
    order_by: list[str] | None = None,
    schema_name: str = "dbo",
    cast_warning_columns: list[str] | None = None,
    log_table_name: str | None = None,
) -> ConsolidationResult:
    engine = get_engine()
    stg_table = build_staging_table_name(table_name)
    columns = _get_columns(table_name=table_name, schema_name=schema_name)
    column_map = _get_column_meta_map(columns)

    insert_columns = ", ".join(_quote_identifier(col.name) for col in columns)
    select_columns = ",\n            ".join(_build_cast_expression(col) for col in columns)
    source_column_list = ", ".join(f"Src.{_quote_identifier(col.name)}" for col in columns)

    quoted_schema = _quote_identifier(schema_name)
    quoted_target = f"{quoted_schema}.{_quote_identifier(table_name)}"
    quoted_stg = f"{quoted_schema}.{_quote_identifier(stg_table)}"

    cast_warning_columns = cast_warning_columns or []
    warning_meta: list[ColumnMeta] = []

    for col_name in cast_warning_columns:
        key = col_name.upper()
        if key not in column_map:
            raise ValueError(
                f"La columna monitoreada '{col_name}' no existe en la tabla destino {schema_name}.{table_name}."
            )
        warning_meta.append(column_map[key])

    cast_warning_predicates = [_build_cast_failure_condition(col) for col in warning_meta]
    cast_warning_condition = (
        " OR ".join(f"({predicate})" for predicate in cast_warning_predicates)
        if cast_warning_predicates
        else "1 = 0"
    )
    cast_warning_detail = _build_cast_failure_detail_expression(warning_meta)

    if dedupe_enabled and not dedupe_keys:
        raise ValueError(
            f"La deduplicación está habilitada para {table_name}, pero no se definieron dedupe_keys."
        )

    partition_clause = ", ".join(f"Base.{_quote_identifier(col)}" for col in (dedupe_keys or []))
    order_clause = ", ".join(
        f"Base.{_quote_identifier(col)} ASC" for col in (order_by or ["FilaOrigen"])
    )

    ranked_cte = (
        f"""
        WITH Ranked AS (
            SELECT
                Base.*,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_clause}
                    ORDER BY {order_clause}
                ) AS rn
            FROM {quoted_stg} AS Base
        )
        """
        if dedupe_enabled
        else f"""
        WITH Ranked AS (
            SELECT
                Base.*,
                CAST(1 AS BIGINT) AS rn
            FROM {quoted_stg} AS Base
        )
        """
    )

    duplicate_condition = "Src.rn > 1"
    warning_condition = (
        f"Src.rn = 1 AND ({cast_warning_condition})"
        if dedupe_enabled
        else cast_warning_condition
    )
    insert_condition = "Src.rn = 1" if dedupe_enabled else "1 = 1"

    duplicate_count = 0
    cast_warning_count = 0

    with engine.begin() as conn:
        if log_table_name:
            conn.execute(
                text(
                    _build_log_table_sql(
                        log_table_name=log_table_name,
                        stg_table_name=stg_table,
                        schema_name=schema_name,
                    )
                )
            )

        if log_table_name and dedupe_enabled:
            quoted_log = f"{quoted_schema}.{_quote_identifier(log_table_name)}"

            sql_insert_duplicates = f"""
            {ranked_cte}
            INSERT INTO {quoted_log} (
                [FechaProceso],
                [Motivo_Log_SQL],
                [Detalle_Log_SQL],
                {insert_columns}
            )
            SELECT
                SYSUTCDATETIME() AS [FechaProceso],
                '[SQL][DUPLICADO] Registro descartado por deduplicación' AS [Motivo_Log_SQL],
                'Se conservó la fila con menor prioridad según order_by' AS [Detalle_Log_SQL],
                {source_column_list}
            FROM Ranked AS Src
            WHERE {duplicate_condition};
            """

            sql_count_duplicates = f"""
            {ranked_cte}
            SELECT COUNT(1) AS duplicate_rows
            FROM Ranked AS Src
            WHERE {duplicate_condition};
            """

            conn.execute(text(sql_insert_duplicates))
            duplicate_count_result = conn.execute(text(sql_count_duplicates)).mappings().first()
            duplicate_count = int(duplicate_count_result["duplicate_rows"]) if duplicate_count_result else 0

        if log_table_name and warning_meta:
            quoted_log = f"{quoted_schema}.{_quote_identifier(log_table_name)}"

            sql_insert_warnings = f"""
            {ranked_cte}
            INSERT INTO {quoted_log} (
                [FechaProceso],
                [Motivo_Log_SQL],
                [Detalle_Log_SQL],
                {insert_columns}
            )
            SELECT
                SYSUTCDATETIME() AS [FechaProceso],
                '[SQL][CAST][WARNING] Conversión inválida; la fila se insertó con NULL en columnas tipadas' AS [Motivo_Log_SQL],
                {cast_warning_detail} AS [Detalle_Log_SQL],
                {source_column_list}
            FROM Ranked AS Src
            WHERE {warning_condition};
            """

            sql_count_warnings = f"""
            {ranked_cte}
            SELECT COUNT(1) AS cast_warning_rows
            FROM Ranked AS Src
            WHERE {warning_condition};
            """

            conn.execute(text(sql_insert_warnings))
            cast_warning_result = conn.execute(text(sql_count_warnings)).mappings().first()
            cast_warning_count = int(cast_warning_result["cast_warning_rows"]) if cast_warning_result else 0

        # =====================================================================
        # ARQUITECTURA ELT CLÁSICA: CARGA MASIVA (SINGLE BATCH)
        # =====================================================================
        
        print(f"[INFO] Iniciando inserción masiva en {table_name}. El motor de SQL Server está procesando...")

        sql_insert_main = f"""
        {ranked_cte}
        INSERT INTO {quoted_target} WITH (TABLOCK) ({insert_columns})
        SELECT {select_columns}
        FROM Ranked AS Src
        WHERE {insert_condition};
        """
        
        res = conn.execute(text(sql_insert_main))
        total_inserted = res.rowcount if res.rowcount != -1 else 0

        # Limpieza de Staging
        conn.execute(text(f"DROP TABLE IF EXISTS {quoted_stg};"))

    return ConsolidationResult(
        inserted_rows=total_inserted,
        duplicate_rows=duplicate_count,
        cast_warning_rows=cast_warning_count,
        log_table_name=log_table_name,
    )