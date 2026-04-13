"""
Módulo de configuración global e infraestructura de ejecución (Arquitectura ELT SAT).

Centraliza la gestión de rutas maestras, reglas de orquestación y 
el aprovisionamiento de memoria Out-of-Core para Polars.
"""

import os
from pathlib import Path
from typing import Any, Dict

import polars as pl
from dotenv import load_dotenv

load_dotenv()

# =============================================================================
# 1. RUTAS E INFRAESTRUCTURA DE ALMACENAMIENTO
# =============================================================================
BASE_DIR: Path = Path(__file__).resolve().parent.parent
LOG_DIR: Path = BASE_DIR / "logs"
TEMP_DIR: Path = BASE_DIR / "temp_processing"

ruta_sat_env = os.getenv("SAT_RAW_DIR", str(BASE_DIR / "data_fallback"))
SAT_RAW_DIR: Path = Path(ruta_sat_env)

for folder in (LOG_DIR, TEMP_DIR):
    folder.mkdir(parents=True, exist_ok=True)

# =============================================================================
# 2. GESTIÓN DE MEMORIA Y RENDIMIENTO (POLARS)
# =============================================================================
ruta_temp_env = os.getenv("ETL_TEMP_DIR", r"C:\Temp_Polars")
DISCO_TRABAJO: Path = Path(ruta_temp_env)
DISCO_TRABAJO.mkdir(parents=True, exist_ok=True)

os.environ["POLARS_TEMP_DIR"] = str(DISCO_TRABAJO)
os.environ["TMPDIR"] = str(DISCO_TRABAJO)
os.environ["TEMP"] = str(DISCO_TRABAJO)
os.environ["TMP"] = str(DISCO_TRABAJO)
os.environ["POLARS_MAX_THREADS"] = "4"

pl.Config.set_tbl_rows(20)
pl.Config.set_fmt_str_lengths(50)

BATCH_SIZE: int = 100_000

# =============================================================================
# 3. DICCIONARIO MAESTRO DE ORQUESTACIÓN (ESPEJO MILIMÉTRICO)
# =============================================================================
TABLES_CONFIG: Dict[str, Dict[str, Any]] = {
    # ---------------- CABECERAS (Anexo 1A) ----------------
    "1A": {
        "file_name": "GERG_AECF_1891_Anexo1A-QA.txt",
        "table_name": "ANEXO_1A_2025_1S",
        "separator": "|",
        "dedupe_enabled": False,  # APAGADO PARA MANTENER VOLUMETRÍA EXACTA
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
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte", "Descuento"]
    },
    "2B_2024": {
        "file_name": "AECF_0101_Anexo2.csv",
        "table_name": "ANEXO_2B_2024",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2024",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte", "Descuento"]
    },
    "2B_2025_2S": {
        "file_name": "AECF_0129_Anexo1_TablaB.csv.csv",
        "table_name": "ANEXO_2B_2025_2S",
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2025_2S",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte", "Descuento"]
    },
    "2B_2023": {
        "file_name": "AECF_0488_Anexo2.csv",
        "table_name": "ANEXO_2B_2023", 
        "separator": "|",
        "dedupe_enabled": False,
        "dedupe_keys": ["UUID", "CONCEPTOCLAVEPRODSERV", "CONCEPTOIMPORTE"],
        "order_by": ["FilaOrigen"],
        "log_table_name": "LOG_CONSOLIDACION_ANEXO_2B_2023",
        "cast_warning_columns": ["ConceptoCantidad", "ConceptoValorUnitario", "ConceptoImporte", "Descuento"]
    }
}

# =============================================================================
# 4. REGLAS DE TIPADO DINÁMICO
# =============================================================================
REGLAS_DINAMICAS: Dict[str, Any] = {
    "UUID": pl.Utf8,
    "EMISORRFC": pl.Utf8,
    "RECEPTORRFC": pl.Utf8,
    "FECHAEMISION": pl.Datetime,
    "FECHACERTIFICACION": pl.Datetime,
    "FECHACANCELACION": pl.Datetime,
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
}