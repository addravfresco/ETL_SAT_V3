"""Módulo de persistencia masiva - Staging y Data Quality.

Implementa mecanismos de inyección masiva (Bulk Insert) hacia tablas de Staging
utilizando conectores de alto rendimiento (ADBC) con estrategias de tolerancia
a fallos (Fallback a SQLAlchemy). Garantiza la persistencia de la volumetría
total y de la bitácora analítica generada por el motor de calidad de datos.
"""

from __future__ import annotations

import polars as pl
from sqlalchemy import exc

from pkg.config import get_connection_string, get_engine


def build_staging_table_name(table_name: str) -> str:
    """Genera el identificador estandarizado para la tabla de staging.

    Args:
        table_name (str): Nombre base de la tabla lógica destino.

    Returns:
        str: Nombre de la tabla temporal prefijada con 'STG_'.
    """
    return f"STG_{table_name}"


def upload_to_sql_blindado(df: pl.DataFrame, table_name: str, anexo_id: str) -> None:
    """Inserta un lote de datos en la tabla Staging con tolerancia a fallos.

    Utiliza el conector ADBC (Arrow Database Connectivity) como motor principal
    para maximizar la velocidad de transferencia. En caso de rechazo por anomalías
    estrictas del lado del servidor, implementa un fallback estructural hacia 
    SQLAlchemy para recuperar el rastro del error original (Stack Trace) y 
    purgar el pool de conexiones envenenadas.

    Args:
        df (pl.DataFrame): Lote de datos vectorizado a inyectar en la base de datos.
        table_name (str): Nombre base de la tabla lógica destino.
        anexo_id (str): Identificador del flujo de datos actual.

    Raises:
        Exception: Propaga la excepción original detectada por ADBC o SQLAlchemy
            tras haber documentado el diagnóstico forense en la salida estándar.
    """
    if df.is_empty():
        return

    stg_table = build_staging_table_name(table_name)

    try:
        # Ejecución primaria de alta velocidad vía ADBC
        uri = get_connection_string().replace("mssql+pyodbc", "mssql")
        df.write_database(
            table_name=stg_table,
            connection=uri,
            if_table_exists="append",
            engine="adbc"
        )
    except Exception as e_adbc:
        # Fallback diagnóstico: SQLAlchemy evalúa la integridad del lote y del pool
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
            print("\n" + "="*80)
            print("[DIAGNÓSTICO FORENSE] El motor de SQL Server rechazó el lote. Causa raíz:")
            print(str(e_adbc))
            print("="*80 + "\n")
            raise
        except Exception as e_sql:
            engine.dispose()
            print("\n" + "="*80)
            print(f"[DIAGNÓSTICO FORENSE] Fallo crítico durante inyección masiva en {stg_table}.")
            print(str(e_sql))
            print("="*80 + "\n")
            raise


def upload_dq_log_sql(df_dq_log: pl.DataFrame, table_name: str) -> None:
    """Inyecta la bitácora analítica de Data Quality en SQL Server.

    Aplica el mismo patrón de resiliencia (ADBC -> SQLAlchemy Fallback) utilizado 
    en la carga de Staging para garantizar que los metadatos de anomalías no se 
    pierdan ante inestabilidades de red o bloqueos de transacciones.

    Args:
        df_dq_log (pl.DataFrame): Colección de alertas y registros anómalos.
        table_name (str): Nombre base de la tabla lógica asociada a los logs.

    Raises:
        Exception: Propaga excepciones críticas de I/O tras reiniciar el motor.
    """
    if df_dq_log.is_empty():
        return
        
    dq_table = f"DQ_LOG_{table_name}"

    try:
        # Ejecución primaria de alta velocidad vía ADBC
        uri = get_connection_string().replace("mssql+pyodbc", "mssql")
        df_dq_log.write_database(
            table_name=dq_table,
            connection=uri,
            if_table_exists="append",
            engine="adbc"
        )
    except Exception as e_adbc:
        # Fallback diagnóstico para la bitácora
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
            print(f"\n[CRÍTICO] Inconsistencia transaccional detectada. Pool reiniciado para el log {dq_table}.")
            print("\n" + "="*80)
            print("[DIAGNÓSTICO FORENSE] El motor de SQL Server rechazó el lote de bitácora. Causa raíz:")
            print(str(e_adbc))
            print("="*80 + "\n")
            raise
        except Exception as e_sql:
            engine.dispose()
            print("\n" + "="*80)
            print(f"[DIAGNÓSTICO FORENSE] Fallo crítico durante inyección analítica en {dq_table}.")
            print(str(e_sql))
            print("="*80 + "\n")
            raise