"""Módulo de configuración global e infraestructura de ejecución (Arquitectura ELT SAT - FACTURAS).

Centraliza la gestión de rutas maestras, reglas de orquestación, contratos de datos
estrictos (TypedDict) y el aprovisionamiento de memoria Out-of-Core para el motor 
de Polars, delegando operaciones de swap a unidades virtuales (V:) para proteger 
el disco principal del sistema operativo.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Optional, TypedDict

import polars as pl
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# 1. RUTAS E INFRAESTRUCTURA DE ALMACENAMIENTO (REDIRECCIÓN A V:)
# =============================================================================
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# Ruta destino para la cuarentena masiva y logs operativos
ruta_logs_env = os.getenv("ETL_LOG_DIR", r"V:\Logs_ETL_Facturas")
LOG_DIR: Path = Path(ruta_logs_env)

# Directorio de procesamiento para archivos temporales del sistema
TEMP_DIR: Path = BASE_DIR / "temp_processing"

# Ruta de montaje de red (UNC) para los archivos crudos del SAT
ruta_sat_env = os.getenv("SAT_RAW_DIR", str(BASE_DIR / "data_fallback"))
SAT_RAW_DIR: Path = Path(ruta_sat_env)

# Aprovisionamiento de directorios de infraestructura
for folder in (LOG_DIR, TEMP_DIR):
    folder.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 2. GESTIÓN DE MEMORIA Y RENDIMIENTO (POLARS - SWAP EN UNIDAD V:)
# =============================================================================
# Redirección de procesamiento temporal (Spill-to-Disk) para evadir saturación del disco local
ruta_temp_env = os.getenv("ETL_TEMP_DIR", r"V:\SAT\ETL_TEMP")
DISCO_TRABAJO: Path = Path(ruta_temp_env)
DISCO_TRABAJO.mkdir(parents=True, exist_ok=True)

# Inyección de variables de entorno para el motor de ejecución en Rust
os.environ["POLARS_TEMP_DIR"] = str(DISCO_TRABAJO)
os.environ["TMPDIR"] = str(DISCO_TRABAJO)
os.environ["TEMP"] = str(DISCO_TRABAJO)
os.environ["TMP"] = str(DISCO_TRABAJO)
os.environ["POLARS_MAX_THREADS"] = "4"

# Parámetros de renderizado en terminal para monitoreo
pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

# Tamaño transaccional por iteración de lectura binaria (Chunking)
BATCH_SIZE: int = 100_000

# =============================================================================
# 3. CONTRATO DE ORQUESTACIÓN (TYPED DICT)
# =============================================================================
class TableConfig(TypedDict):
    """Esquema estricto para la configuración modular de cada Anexo.

    Attributes:
        file_name (str): Nombre del archivo físico a procesar.
        table_name (str): Nombre de la tabla lógica destino en SQL Server.
        separator (str): Carácter delimitador de campos en el archivo crudo.
        dedupe_enabled (bool): Activa la estrategia de deduplicación en Consolidación.
        dedupe_keys (list[str]): Llaves lógicas para el agrupamiento de duplicados.
        order_by (list[str]): Criterio de ordenamiento para resolución de empates.
        log_table_name (Optional[str]): Tabla destino para auditoría forense (Logs).
        cast_warning_columns (list[str]): Columnas monitoreadas por conversión inválida.
    """
    file_name: str
    table_name: str
    separator: str
    dedupe_enabled: bool
    dedupe_keys: list[str]
    order_by: list[str]
    log_table_name: Optional[str]
    cast_warning_columns: list[str]


# DICCIONARIO MAESTRO DE ORQUESTACIÓN
TABLES_CONFIG: dict[str, TableConfig] = {
    # ---------------- CABECERAS (Anexo 1A) ----------------
    "1A": {
        "file_name": "GERG_AECF_1891_Anexo1A-QA.txt",
        "table_name": "ANEXO_1A_2025_1S",
        "separator": "|",
        "dedupe_enabled": False,  
        "dedupe_keys": ["UUID"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_1A_2025_1S",
        "cast_warning_columns": ["FechaEmision", "Total", "SubTotal", "TotalImpuestosTrasladados", "TotalImpuestosRetenidos"]
    },
    "1A_2024": {
        "file_name": "AECF_0101_Anexo1.csv",
        "table_name": "ANEXO_1A_2024",
        "separator": "|",
        "dedupe_enabled": False,  
        "dedupe_keys": ["UUID"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_1A_2024",
        "cast_warning_columns": ["FechaEmision", "Total", "SubTotal", "TotalImpuestosTrasladados", "TotalImpuestosRetenidos"]
    },
    "1A_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaA.csv.csv",
        "table_name": "ANEXO_1A_2025_2S",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_1A_2025_2S",
        "cast_warning_columns": ["FechaEmision", "Total", "SubTotal", "TotalImpuestosTrasladados", "TotalImpuestosRetenidos"]
    # Referencia Operativa (Contador.py): 241,736,238 Registros    
    },
    "1A_2023": {
        "file_name": "AECF_0488_Anexo1.csv",
        "table_name": "ANEXO_1A_2023", 
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_1A_2023",
        "cast_warning_columns": ["FechaEmision", "Total", "SubTotal", "TotalImpuestosTrasladados", "TotalImpuestosRetenidos"]
    },
    
    # ---------------- DETALLES (Anexo 2B) ----------------
    "2B": {
        "file_name": "GERG_AECF_1891_Anexo2B.csv",
        "table_name": "ANEXO_2B_2025_1S",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2025_1S",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"]
    },
    "2B_2024": {
        "file_name": "AECF_0101_Anexo2.csv",
        "table_name": "ANEXO_2B_2024",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2024",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"]
    },
    "2B_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaB.csv.csv",
        "table_name": "ANEXO_2B_2025_2S",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2025_2S",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"]
    },
    "2B_2023": {
        "file_name": "AECF_0488_Anexo2.csv",
        "table_name": "ANEXO_2B_2023", 
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2023",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte"]
    },
    "1A_2022": {
        "file_name": "AECF_0080_Anexo1A.csv",
        "table_name": "ANEXO_1A_2022",
        "separator": "|",
        "dedupe_enabled": False,  
        "dedupe_keys": ["UUID"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_1A_2022",
        "cast_warning_columns": ["FechaEmision", "Total", "SubTotal", "TotalImpuestosTrasladados", "TotalImpuestosRetenidos"]
    },
    
    # ---------------- ENTORNO DE PRUEBAS (TEST FACTURAS) ----------------
    "TEST_1A": {
        "file_name": "AECF_0101_Anexo1.csv",
        "table_name": "ANEXO_1A_TEST", 
        "separator": "|", 
        "dedupe_enabled": False, 
        "dedupe_keys": [], 
        "order_by": ["FilaOrigen"], 
        "log_table_name": "LOG_CONSOLIDACION_TEST_1A", 
        "cast_warning_columns": []
    }
}

# =============================================================================
# 4. CONTRATO DE TIPADO DINÁMICO
# =============================================================================
# Definición estricta de tipos esperados antes del volcado a la base de datos
REGLAS_DINAMICAS: dict[str, Any] = {
    "UUID": pl.Utf8,
    "EMISORRFC": pl.Utf8,
    "RECEPTORRFC": pl.Utf8,
    "FECHAEMISION": pl.Utf8,       # Diferido a Utf8 (Validación en motor DQ y SQL)
    "FECHACERTIFICACION": pl.Utf8, # Diferido a Utf8
    "FECHACANCELACION": pl.Utf8,   # Diferido a Utf8
    "DESCUENTO": pl.Float64,
    "SUBTOTAL": pl.Float64,
    "TOTAL": pl.Float64,
    "TRASLADOSIVA": pl.Float64,
    "TRASLADOSIEPS": pl.Float64,
    "TOTALIMPUESTOSTRASLADADOS": pl.Float64,
    "RETENIDOSIVA": pl.Float64,
    "RETENIDOSISR": pl.Float64,
    "TOTALIMPUESTOSRETENIDOS": pl.Float64,
    "TIPOCAMBIO": pl.Float64,
    "CONCEPTOCANTIDAD": pl.Float64,
    "CONCEPTOVALORUNITARIO": pl.Float64,
    "CONCEPTOIMPORTE": pl.Float64,
    "FilaOrigen": pl.Int64,        # Llave transaccional maestra
}