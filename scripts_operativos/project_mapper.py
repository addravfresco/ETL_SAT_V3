"""Módulo de mapeo topológico y documentación de repositorio.

Genera una representación visual en formato de árbol de la estructura 
de directorios y archivos del proyecto. Excluye automáticamente artefactos 
del sistema, entornos virtuales y volúmenes de Big Data para mantener 
la legibilidad de la documentación técnica.
"""

import os
from pathlib import Path
from typing import List, Set

# =============================================================================
# CONFIGURACIÓN DE FILTROS DE EXCLUSIÓN
# =============================================================================
# Directorios excluidos del escaneo topológico (Entornos, Git, Caché y Datos Masivos)
IGNORE_DIRS: Set[str] = {
    ".git", 
    ".venv", 
    "venv", 
    "env",
    "__pycache__", 
    ".idea", 
    ".vscode",
    "sat_bigdata",
    "data_fallback",
    "data_lake",
    "temp_processing",
    "tmp"
}

# Archivos de sistema excluidos de la representación visual
IGNORE_FILES: Set[str] = {
    ".DS_Store", 
    "Thumbs.db"
}


def generate_tree(dir_path: Path, prefix: str = "") -> List[str]:
    """Genera recursivamente la representación topológica de un directorio.

    Itera sobre el sistema de archivos clasificando primero los directorios y 
    luego los archivos, aplicando las reglas de exclusión configuradas en el 
    ámbito global para omitir artefactos no relevantes para la arquitectura.

    Args:
        dir_path (Path): Ruta absoluta o relativa del directorio a escanear.
        prefix (str, optional): Cadena de formateo visual para la indentación jerárquica.
            Por defecto es una cadena vacía.

    Returns:
        List[str]: Colección de cadenas formateadas que representan el árbol visual.
    """
    try:
        contents = list(dir_path.iterdir())
    except PermissionError:
        return [f"{prefix}└── [ACCESO DENEGADO]"]
        
    # Ordenamiento lógico: Directorios consolidados en la parte superior
    contents.sort(key=lambda x: (not x.is_dir(), x.name.lower()))

    tree_lines: List[str] = []
    
    # Aplicación de reglas de exclusión (Filtro de Ruido)
    filtered_contents = [
        c for c in contents 
        if c.name not in IGNORE_DIRS and c.name not in IGNORE_FILES
    ]

    # Asignación de conectores visuales para la ramificación del árbol
    pointers = [("├── " if i < len(filtered_contents) - 1 else "└── ") for i in range(len(filtered_contents))]

    for pointer, path in zip(pointers, filtered_contents):
        # Determinación de la sangría transversal para los niveles subsecuentes
        connector = "│   " if pointer == "├── " else "    "
        line = f"{prefix}{pointer}{path.name}"
        
        if path.is_dir():
            line += "/"
            tree_lines.append(line)
            # Ejecución recursiva de profundidad (Depth-First Search)
            tree_lines.extend(generate_tree(path, prefix + connector))
        else:
            tree_lines.append(line)

    return tree_lines


def main() -> None:
    """Ejecuta el escaneo topológico y persiste la estructura del repositorio.

    Inicia el recorrido desde el directorio de trabajo actual (CWD), renderiza 
    la salida en la interfaz de línea de comandos y consolida el artefacto 
    final en un archivo de texto plano para propósitos de documentación.
    """
    root_dir = Path.cwd()
    print(f"\n[INFO] Iniciando escaneo topológico del repositorio: {root_dir.name}...")
    
    tree_lines = generate_tree(root_dir)
    
    # 1. Renderizado en interfaz de consola (CLI)
    print(f"\n{root_dir.name}/")
    for line in tree_lines:
        print(line)
        
    # 2. Persistencia en artefacto de texto plano
    output_file = "ESTRUCTURA_PROYECTO.txt"
    try:
        with open(output_file, "w", encoding="utf-8") as file:
            file.write(f"TOPOLOGÍA DEL REPOSITORIO: {root_dir.name}\n")
            file.write("=" * 60 + "\n")
            file.write(f"{root_dir.name}/\n")
            for line in tree_lines:
                file.write(line + "\n")
                
        print(f"\n[SUCCESS] Topología exportada exitosamente en: {output_file}")
        
        # Apertura automática delegada al sistema operativo huésped
        if os.name == "nt":  # Windows
            os.startfile(output_file)
    except Exception as e:
        print(f"[ERROR] Fallo al persistir el artefacto de estructura: {e}")


if __name__ == "__main__":
    main()