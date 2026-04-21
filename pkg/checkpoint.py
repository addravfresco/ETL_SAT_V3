"""Módulo de persistencia de estado transaccional (Bookmarking Avanzado).

Gestiona la lectura y escritura de punteros de ejecución. Implementa memoria 
espacial (Byte Offset) para lograr una reanudación (Cold Resume) en tiempo 
constante O(1), erradicando el costo de búsqueda secuencial en archivos masivos.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypedDict

STATE_FILE = "estado_etl.json"


class CheckpointData(TypedDict):
    """Estructura estricta del estado guardado en disco.

    Attributes:
        filas_procesadas (int): Cantidad de filas lógicas ya procesadas exitosamente.
        byte_offset (int): Coordenada física en bytes dentro del archivo fuente.
    """
    filas_procesadas: int
    byte_offset: int


def leer_estado(anexo_id: str) -> tuple[int, int]:
    """Recupera el índice de fila y la coordenada en bytes para un anexo específico.

    Args:
        anexo_id (str): Identificador único del flujo de datos.

    Returns:
        tuple[int, int]: Una tupla que contiene (filas_procesadas, byte_offset). 
        Retorna (0, 0) si no existe un estado previo persistido en disco.
    """
    ruta_archivo = Path(STATE_FILE)

    if not ruta_archivo.exists():
        return 0, 0

    try:
        with ruta_archivo.open("r", encoding="utf-8") as file:
            datos: dict[str, dict[str, Any]] = json.load(file)
            estado_anexo = datos.get(anexo_id, {})
            
            # Soporte de retrocompatibilidad para versiones anteriores del esquema de estado.
            if isinstance(estado_anexo, int):
                return estado_anexo, 0
                
            return int(estado_anexo.get("filas_procesadas", 0)), int(estado_anexo.get("byte_offset", 0))
            
    except (json.JSONDecodeError, ValueError, TypeError):
        return 0, 0


def guardar_estado(anexo_id: str, filas_procesadas: int, byte_offset: int) -> None:
    """Actualiza el puntero de ejecución y la coordenada física en el estado global.

    Args:
        anexo_id (str): Identificador único del flujo de datos.
        filas_procesadas (int): Filas lógicas procesadas acumuladas.
        byte_offset (int): Posición física exacta (seek) en el archivo crudo.
    """
    ruta_archivo = Path(STATE_FILE)
    datos: dict[str, Any] = {}

    if ruta_archivo.exists():
        try:
            with ruta_archivo.open("r", encoding="utf-8") as file:
                datos = json.load(file)
        except json.JSONDecodeError:
            datos = {}

    datos[anexo_id] = {
        "filas_procesadas": filas_procesadas,
        "byte_offset": byte_offset
    }

    with ruta_archivo.open("w", encoding="utf-8") as file:
        json.dump(datos, file, indent=4, ensure_ascii=False)


def eliminar_estado(anexo_id: str) -> None:
    """Elimina el estado persistido de un anexo tras una ejecución exitosa.

    Args:
        anexo_id (str): Identificador único del flujo cuyo estado debe ser purgado.
    """
    ruta_archivo = Path(STATE_FILE)

    if not ruta_archivo.exists():
        return

    try:
        with ruta_archivo.open("r", encoding="utf-8") as file:
            datos: dict[str, Any] = json.load(file)
    except json.JSONDecodeError:
        return

    if anexo_id in datos:
        del datos[anexo_id]

    if datos:
        with ruta_archivo.open("w", encoding="utf-8") as file:
            json.dump(datos, file, indent=4, ensure_ascii=False)
    else:
        ruta_archivo.unlink(missing_ok=True)