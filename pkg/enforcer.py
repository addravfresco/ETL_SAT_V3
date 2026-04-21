"""Módulo de Integridad Estructural y Tipado (Schema Enforcement) - UNIVERSAL.

Garantiza que el lote de datos cumpla con el contrato de tipos definido
en la configuración de la arquitectura. Prepara los datos para un volcado 
masivo a Staging, difiriendo los truncamientos y coerciones duras a la capa 
de base de datos relacional para evitar fallos de ejecución en memoria.
"""

from __future__ import annotations

from typing import Any

import polars as pl


def estandarizar_nombres_columnas(df: pl.DataFrame) -> pl.DataFrame:
    """Sanitiza los identificadores de columna y elimina artefactos de codificación.

    Aplica una limpieza sobre los metadatos del esquema (nombres de columnas) para 
    remover caracteres invisibles, como el BOM (Byte Order Mark), saltos de línea 
    aislados y delimitadores de texto mal estructurados, asegurando compatibilidad 
    directa con los identificadores de SQL Server.

    Args:
        df (pl.DataFrame): El DataFrame de Polars con los encabezados crudos de extracción.

    Returns:
        pl.DataFrame: Instancia del DataFrame con identificadores topológicos normalizados.
    """
    nuevas_columnas: list[str] = []

    for idx, col in enumerate(df.columns):
        nombre_limpio = (
            col.strip()
            .replace("\ufeff", "")
            .replace("\n", "")
            .replace('"', "")
            .replace("'", "")
        )

        # Fallback de resiliencia: Asignación de identificador primario en caso de 
        # que el sistema de origen genere un encabezado inicial vacío u omitido.
        if not nombre_limpio and idx == 0:
            nombre_limpio = "UUID"

        nuevas_columnas.append(nombre_limpio)

    return df.rename(dict(zip(df.columns, nuevas_columnas)))


def aplicar_tipos_seguros(df: pl.DataFrame, reglas_tipos: dict[str, Any]) -> pl.DataFrame:
    """Aplica coerciones de tipo tolerantes a fallos según el contrato dinámico.

    Ejecuta un casting seguro en memoria. Implementa rutinas de limpieza previa 
    vectorizadas para tipos numéricos e intencionalmente omite la validación de 
    longitud máxima para cadenas de texto, delegando la responsabilidad del 
    truncamiento (VARCHAR limits) a la operación transaccional en el motor de base de datos.

    Args:
        df (pl.DataFrame): Lote de datos (Chunk) a someter a validación de tipos.
        reglas_tipos (dict[str, Any]): Diccionario que define el contrato lógico 
            de datos, mapeando el nombre de la columna a su tipo nativo en Polars.

    Returns:
        pl.DataFrame: Lote de datos con casting estructural aplicado, listo para inyección.
    """
    if df.is_empty():
        return df

    df = estandarizar_nombres_columnas(df)
    reglas_normalizadas = {k.upper(): v for k, v in reglas_tipos.items()}

    expresiones: list[pl.Expr] = []

    for col in df.columns:
        col_up = col.upper()

        # Validación contra el contrato de datos maestro.
        if col_up in reglas_normalizadas:
            dtype_destino = reglas_normalizadas[col_up]

            if dtype_destino == pl.Float64:
                expr = (
                    pl.col(col)
                    .cast(pl.Utf8, strict=False)
                    .str.replace_all(",", "")  # Supresión de separadores de millares para parseo aritmético puro
                    .str.strip_chars()
                    .cast(pl.Float64, strict=False)
                    .alias(col)
                )
            elif dtype_destino == pl.Int64:
                expr = (
                    pl.col(col)
                    .cast(pl.Utf8, strict=False)
                    .str.replace_all(",", "")
                    .str.strip_chars()
                    .cast(pl.Int64, strict=False)
                    .alias(col)
                )
            elif dtype_destino == pl.Utf8:
                # Conversión estructural explícita. El límite de longitud física (Truncation) 
                # se maneja exclusivamente en el DDL y procedimientos T-SQL destino.
                expr = pl.col(col).cast(pl.Utf8, strict=False).alias(col)
            else:
                expr = pl.col(col).cast(dtype_destino, strict=False).alias(col)

            expresiones.append(expr)
            
        # Manejo de atributos fuera del contrato principal: preservación como cadena de texto genérica.
        elif df.schema[col] == pl.Utf8:
             expr = pl.col(col).cast(pl.Utf8, strict=False).alias(col)
             expresiones.append(expr)

    return df.with_columns(expresiones) if expresiones else df