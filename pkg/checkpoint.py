"""
Módulo de persistencia de estado transaccional (Bookmarking).

Gestiona la lectura, escritura y depuración de punteros de ejecución en formato JSON 
para permitir la reanudación segura (Cold Resume) en arquitecturas de 
procesamiento por lotes, mitigando la pérdida de avance ante interrupciones.
"""

import json
from pathlib import Path
from typing import Any, Dict

STATE_FILE = "estado_etl.json"


def leer_estado(anexo_id: str) -> int:
    """
    Recupera el índice de la última fila procesada para un anexo específico.
    """
    ruta_archivo = Path(STATE_FILE)
    
    if not ruta_archivo.exists():
        return 0

    try:
        with open(ruta_archivo, "r", encoding="utf-8") as file:
            datos: Dict[str, Any] = json.load(file)
            return datos.get(anexo_id, 0)
    except json.JSONDecodeError:
        return 0


def guardar_estado(anexo_id: str, filas_procesadas: int) -> None:
    """
    Actualiza el puntero de ejecución del anexo en el archivo de estado global.
    """
    ruta_archivo = Path(STATE_FILE)
    datos: Dict[str, int] = {}
    
    if ruta_archivo.exists():
        try:
            with open(ruta_archivo, "r", encoding="utf-8") as file:
                datos = json.load(file)
        except json.JSONDecodeError:
            pass 
            
    datos[anexo_id] = filas_procesadas
    
    with open(ruta_archivo, "w", encoding="utf-8") as file:
        json.dump(datos, file, indent=4)


def eliminar_estado(anexo_id: str) -> None:
    """
    Purga el estado transaccional de un anexo tras su consolidación exitosa.
    Garantiza que la siguiente ejecución del mismo anexo sea un Cold Start limpio.
    """
    ruta_archivo = Path(STATE_FILE)
    
    if not ruta_archivo.exists():
        return
        
    try:
        with open(ruta_archivo, "r", encoding="utf-8") as file:
            datos: Dict[str, Any] = json.load(file)
            
        if anexo_id in datos:
            del datos[anexo_id]
            
            with open(ruta_archivo, "w", encoding="utf-8") as file:
                json.dump(datos, file, indent=4)
                
    except json.JSONDecodeError:
        pass