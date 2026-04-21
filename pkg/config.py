"""Módulo de gestión centralizada de credenciales y parámetros de conexión.

Provee la lógica necesaria para construir cadenas de conexión seguras 
hacia SQL Server mediante SQLAlchemy y PyODBC. Actúa como un Singleton 
para el motor de base de datos, garantizando la reutilización de conexiones
y la optimización de inserciones masivas.
"""

import os
import urllib.parse

from dotenv import load_dotenv
from sqlalchemy import Engine, create_engine

load_dotenv()

SERVER = os.getenv("DB_SERVER")
DATABASE = os.getenv("DB_NAME")
USER = os.getenv("DB_USER")
PASSWORD = os.getenv("DB_PASSWORD")
DRIVER = os.getenv("DB_DRIVER", "{ODBC Driver 17 for SQL Server}")
TRUSTED = os.getenv("DB_TRUSTED", "NO")

_engine = None


def get_connection_string() -> str:
    """Construye la cadena de conexión ODBC codificada para SQLAlchemy.

    Evalúa las variables de entorno para determinar el tipo de autenticación
    (Trusted Connection o SQL Authentication) y genera una cadena segura con
    los parámetros de encriptación requeridos.

    Returns:
        str: Cadena de conexión parametrizada y codificada en formato URL.

    Raises:
        ValueError: Si las variables críticas de servidor (DB_SERVER, DB_NAME)
            o las credenciales de autenticación (DB_USER, DB_PASSWORD) no están 
            definidas en el entorno.
    """
    if not all([SERVER, DATABASE]):
        raise ValueError("Ausencia de variables críticas en el entorno: DB_SERVER o DB_NAME.")

    if TRUSTED.upper() in ["YES", "TRUE", "1"]:
        auth_str = "Trusted_Connection=yes;"
    else:
        if not all([USER, PASSWORD]):
            raise ValueError("Ausencia de DB_USER o DB_PASSWORD en el entorno para autenticación SQL.")
        auth_str = f"UID={USER};PWD={PASSWORD};"

    cadena_odbc = (
        f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};"
        f"{auth_str}Encrypt=no;TrustServerCertificate=yes;"
    )
    
    params = urllib.parse.quote_plus(cadena_odbc)
    
    return f"mssql+pyodbc:///?odbc_connect={params}"


def get_engine() -> Engine:
    """Retorna la instancia global del motor SQLAlchemy (Singleton).

    Garantiza que solo exista una instancia del motor de base de datos 
    durante el ciclo de vida de la aplicación. Habilita nativamente el 
    parámetro 'fast_executemany' para optimizar operaciones bulk.

    Returns:
        Engine: Instancia reutilizable y optimizada del motor SQLAlchemy.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(get_connection_string(), fast_executemany=True)
    return _engine