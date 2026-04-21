# SAT ETL Data Pipeline V3 - Out-of-Core Processing

Este repositorio contiene la arquitectura de Ingeniería de Datos para la extracción, saneamiento masivo y consolidación transaccional de los anexos del SAT (e.g., Anexo 1A, Anexo 2B). 

La solución está diseñada bajo el estándar *Out-of-Core* utilizando el motor vectorial Polars (Rust) y conectores ADBC. Esto permite procesar y cargar archivos planos de alta volumetría (superiores a 150 GB) hacia SQL Server sin exceder la capacidad de la memoria RAM del servidor, garantizando tiempos de ejecución óptimos y la estabilidad general del sistema.

---

## Características Principales

* **Evaluación Vectorizada (Lazy & Batched):** Escaneo de archivos de gran volumen mediante fragmentación dinámica y lectura de buffer binario para mitigar cuellos de botella de I/O en infraestructuras de red (SMB).
* **Calidad de Datos (Data Quality):** Detección y limpieza de anomalías de codificación (Mojibake), validación estructural de RFCs/UUIDs y auditoría de cuadratura aritmética.
* **Arquitectura de Espejo Milimétrico:** Garantiza el procesamiento del 100% de la volumetría origen. Los registros anómalos (Soft Rejects y Hard Rejects) se aíslan en bitácoras de auditoría paralelas en la base de datos relacional sin interrumpir el flujo principal.
* **Tolerancia a Fallos (Micro-Checkpoints):** Gestión de estado transaccional O(1) con registro de Byte Offset, permitiendo la reanudación instantánea (Cold Resume) ante contingencias operativas o de red.
* **Consolidación Set-Based:** Operaciones masivas delegadas al motor de SQL Server, implementando Minimal Logging (TABLOCK) y rutinas lógicas de deduplicación.

---

## Prerrequisitos de Infraestructura

* **Lenguaje:** Python 3.9 o superior.
* **Controlador de Base de Datos:** ODBC Driver 17 for SQL Server.
* **Motor Relacional:** SQL Server (On-Premise / Enterprise).
* **Almacenamiento:** Unidad de red (SMB) para archivos de origen y partición local de alto rendimiento (V:) aprovisionada para el Swap transaccional de Polars.

---

## Instalación y Configuración

**1. Clonación del repositorio y aprovisionamiento del entorno virtual:**
```powershell
git clone <url_del_repositorio>
cd ETL_SAT_V3
python -m venv venv
.\venv\Scripts\activate

2. Instalación de dependencias (Sincronización Estricta):
Se requiere el uso de pip-tools para certificar que el entorno excluya paquetes residuales de desarrollo y garantice la instalación del conector ADBC.

python -m pip install pip-tools
python -m piptools sync requirements.txt

3. Configuración de Variables de Entorno (.env):
Cree un archivo .env en la raíz del directorio del proyecto respetando el siguiente contrato:

DB_SERVER=IP_SERVIDOR\INSTANCIA,PUERTO
DB_NAME=SAT_V3
DB_TRUSTED=NO
DB_USER=usuario_etl
DB_PASSWORD=secreto_industrial
DB_DRIVER={ODBC Driver 17 for SQL Server}

# Aprovisionamiento de Rutas de Infraestructura
SAT_RAW_DIR=\\ruta\de\red\Bases de Datos\SAT
ETL_TEMP_DIR=V:\Temp_Polars
ETL_LOG_DIR=V:\Logs_ETL_Facturas

Topología del Repositorio
La estructura del código obedece al principio de separación de responsabilidades (SoC), aislando la parametrización global, los motores de transformación y las utilidades operativas.

ETL_SAT_V3/
├── Data/                   # Documentación técnica, diccionarios y reportes
│   ├── Diccionario de Datos SAT.xlsx
│   ├── Manual Técnico ETL SAT_V3.docx
│   └── ...
├── logs/                   # Artefactos físicos generados por telemetría
│   ├── AUDIT_ANEXO_1A_2023.txt
│   └── CUARENTENA_ANEXO_1A_2023.csv
├── pkg/                    # Core Engine de Arquitectura
│   ├── checkpoint.py       # Gestor de reanudación y persistencia O(1)
│   ├── cleaning_rules.py   # Repositorio de reglas de saneamiento léxico
│   ├── config.py           # Instancia Singleton de base de datos
│   ├── consolidation.py    # Motor de migración masiva (Staging -> Target)
│   ├── enforcer.py         # Coerción de tipos y validación de esquemas
│   ├── extract.py          # Lector binario de particiones continuas
│   ├── globals.py          # Contratos de DDL y enrutamiento central
│   ├── load.py             # Inyección de datos (Integración ADBC / SQL)
│   ├── reports.py          # Framework de observabilidad pasiva
│   └── transform.py        # Algoritmos de calidad y reglas de negocio
├── scripts_operativos/     # Herramientas SOP (Standard Operating Procedures)
│   ├── contador.py         # Escáner binario de volumetría física
│   ├── mojibake_hunter.py  # Analizador estadístico de texto anómalo
│   ├── profile_sat.py      # Generador de topologías (Batched)
│   ├── profile_sat_ligero.py # Auditor de Schema Drift (Streaming)
│   ├── project_mapper.py   # Renderizador de estructura de directorio
│   └── sql_index_master.sql# Procedimientos DDL de optimización
├── .env                    # Entorno de variables y credenciales locales
├── estado_etl.json         # Checkpoints del motor en formato JSON
├── main.py                 # Orquestador unitario
├── requirements.txt        # Manifiesto estricto de dependencias
└── run_all.py              # Orquestador de cadena secuencial (Fail-Fast)

Operación del Pipeline
1. Ejecución Unitaria (Carga Específica)
Para desencadenar el procesamiento de un único anexo listado en el diccionario TABLES_CONFIG (pkg/globals.py):

python main.py 1A_2025_2S

2. Ejecución de Carga Histórica (Pipeline Secuencial)
Para orquestar el despliegue de múltiples anexos asegurando integridad referencial cruzada. El motor detendrá la cadena automáticamente ante cualquier fallo sistémico (Patrón Fail-Fast).

# Ejecutar la cola predeterminada en código
python run_all.py

# Sobrescribir la cola de ejecución a demanda mediante CLI
python run_all.py 1A_2024 2B_2024

3. Auditoría Forense Pre-Vuelo (Herramientas SOP)
De manera previa al procesamiento de nuevos archivos crudos, se requiere el uso de las utilidades forenses para validar integridad sin comprometer el rendimiento del servidor.

# Verificar volumen exacto de registros a nivel físico
python scripts_operativos/contador.py AECF_0129_Anexo1.csv

# Generar catálogo base y evaluar la densidad de atributos nulos
python scripts_operativos/profile_sat_ligero.py AECF_0129_Anexo1.csv

# Aislar y cuantificar patrones de anomalías de codificación
python scripts_operativos/mojibake_hunter.py AECF_0129_Anexo1.csv