"""
Módulo de persistencia masiva - Staging y Cuarentena (Arquitectura ELT).
"""

from __future__ import annotations

import polars as pl

from pkg.config import get_engine


def build_staging_table_name(table_name: str) -> str:
    """Genera el nombre de la tabla staging."""
    return f"STG_{table_name}"


def upload_to_sql_blindado(df: pl.DataFrame, table_name: str, anexo_id: str) -> None:
    """Inserta un lote de datos procesados directamente en la tabla staging."""
    if df.is_empty():
        return

    stg_table = build_staging_table_name(table_name)

    df.write_database(
        table_name=stg_table,
        connection=get_engine(),
        if_table_exists="append",
        engine="sqlalchemy",
    )


def upload_cuarentena_sql(df_quarantine: pl.DataFrame, table_name: str) -> None:
    """
    Filtra los Hard Rejects de la bandeja de cuarentena y los inyecta 
    en una tabla física en SQL Server para auditoría del negocio.
    """
    if df_quarantine.is_empty():
        return
        
    # Extraemos solo los que son HARD Rejects
    df_hard = df_quarantine.filter(pl.col("Motivo_Rechazo").str.contains(r"\[HARD\]"))
    
    if df_hard.is_empty():
        return

    # Creamos el nombre de la tabla de cuarentena (Ej. CUARENTENA_ANEXO_1A_2024)
    cuarentena_table = f"CUARENTENA_{table_name}"

    df_hard.write_database(
        table_name=cuarentena_table,
        connection=get_engine(),
        if_table_exists="append",
        engine="sqlalchemy",
    )