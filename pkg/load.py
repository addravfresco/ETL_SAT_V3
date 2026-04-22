"""Módulo de persistencia masiva - Staging y Data Quality.

Realiza inserciones masivas hacia tablas de Staging utilizando el motor 
optimizado de SQLAlchemy con el parámetro 'fast_executemany' activo 
para inyecciones vectorizadas en SQL Server.
"""

from __future__ import annotations

import polars as pl
from sqlalchemy import exc

from pkg.config import get_engine


def build_staging_table_name(table_name: str) -> str:
    """Genera el identificador estandarizado para la tabla de staging."""
    return f"STG_{table_name}"


def upload_to_sql_blindado(df: pl.DataFrame, table_name: str, anexo_id: str) -> None:
    """Inserta un lote de datos en la tabla Staging con tolerancia a fallos.

    Utiliza el motor SQLAlchemy pre-configurado con fast_executemany para 
    maximizar el rendimiento (Bulk Insert) hacia SQL Server, implementando
    purga de pool ante micro-cortes de red.
    """
    if df.is_empty():
        return

    stg_table = build_staging_table_name(table_name)
    engine = get_engine()

    try:
        df.write_database(
            table_name=stg_table,
            connection=engine,
            if_table_exists="append",
            engine="sqlalchemy"
        )
    except exc.PendingRollbackError:
        engine.dispose()
        print(f"\n[CRÍTICO] Inconsistencia transaccional detectada. Pool reiniciado para la tabla {stg_table}.")
        raise
    except Exception as e_sql:
        engine.dispose()
        print("\n" + "="*80)
        print(f"[DIAGNÓSTICO FORENSE] Fallo crítico durante inyección masiva en {stg_table}.")
        print(str(e_sql))
        print("="*80 + "\n")
        raise


def upload_dq_log_sql(df_dq_log: pl.DataFrame, table_name: str) -> None:
    """Inyecta la bitácora analítica de Data Quality en SQL Server."""
    if df_dq_log.is_empty():
        return
        
    dq_table = f"DQ_LOG_{table_name}"
    engine = get_engine()

    try:
        df_dq_log.write_database(
            table_name=dq_table,
            connection=engine,
            if_table_exists="append",
            engine="sqlalchemy"
        )
    except exc.PendingRollbackError:
        engine.dispose()
        print(f"\n[CRÍTICO] Inconsistencia transaccional. Pool reiniciado para el log {dq_table}.")
        raise
    except Exception as e_sql:
        engine.dispose()
        print("\n" + "="*80)
        print(f"[DIAGNÓSTICO FORENSE] Fallo crítico durante inyección analítica en {dq_table}.")
        print(str(e_sql))
        print("="*80 + "\n")
        raise