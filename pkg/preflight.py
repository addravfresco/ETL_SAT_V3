"""
Módulo de pre-procesamiento y perfilado de archivos masivos (Preflight).

Ejecuta análisis sobre los artefactos de origen aplicando reglas de negocio
estrictas para codificaciones anómalas (UTF-16, Latin-1) y transcodificándolas 
a UTF-8 estricto mediante streaming de bajo nivel.
"""

from pathlib import Path


def detect_and_convert_to_utf8(file_path: Path) -> Path:
    """
    Asigna y transcodifica el encoding correcto basado en reglas duras para los 
    archivos del SAT, o usa escaneo profundo para archivos nuevos.
    """
    # =========================================================================
    # BYPASS ABSOLUTO PARA 2025 (Fast-Pass)
    # =========================================================================
    # Sabemos que a partir de 2025 (Tablas C-G) el SAT entrega datos limpios.
    # Evitamos cualquier I/O innecesario y pasamos el archivo directo a Polars.
    if "Tabla" in file_path.name or "2025" in file_path.name:
        print("[PREFLIGHT] Archivo 2025 detectado. Calidad de origen confiable. Bypass activado.")
        return file_path

    # Para los archivos de 2024 y anteriores, mantenemos la ruta segura:
    out_path = file_path.with_suffix('.utf8_clean.csv')

    # 1. Caché de procesamiento transaccional
    if out_path.exists():
        print(f"[PREFLIGHT] Caché detectado. Utilizando artefacto limpio pre-procesado: {out_path.name}")
        return out_path

    # =========================================================================
    # 2. REGLAS DURAS + DETECCIÓN DINÁMICA DE ALTA PROFUNDIDAD (Archivos Viejos)
    # =========================================================================
    if "Anexo3" in file_path.name:
        encoding_origen = 'utf-16'
    elif "Anexo4" in file_path.name:
        encoding_origen = 'latin-1'
    else:
        print(f"[PREFLIGHT] Archivo desconocido detectado ({file_path.name}). Iniciando escaneo profundo...")
        # Leemos 50 MB para evitar la "Trampa ASCII".
        with open(file_path, 'rb') as f:
            muestra_bytes = f.read(1024 * 1024 * 50) 
            
        if b'\x00' in muestra_bytes:
            encoding_origen = 'utf-16'
        else:
            try:
                muestra_bytes.decode('utf-8')
                encoding_origen = 'utf-8'
            except UnicodeDecodeError:
                encoding_origen = 'latin-1'

    # 3. Bypass de eficiencia
    if encoding_origen == 'utf-8':
        print("[PREFLIGHT] Análisis completado: Archivo nativo en UTF-8. No requiere transcodificación.")
        return file_path

    # 4. Transcodificación Obligatoria por bloques (Streaming Chunking)
    print(f"[PREFLIGHT] Regla de negocio aplicada. Origen forzado a: {encoding_origen.upper()}.")
    print("[PREFLIGHT] Iniciando transcodificación I/O a UTF-8 estricto. Operación de larga duración...")

    CHUNK_SIZE = 1024 * 1024 * 50  # Bloques de 50MB
    
    try:
        with open(file_path, 'r', encoding=encoding_origen) as f_in:
            with open(out_path, 'w', encoding='utf-8') as f_out:
                while True:
                    chunk = f_in.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    f_out.write(chunk)
    except Exception as e:
        if out_path.exists():
            out_path.unlink() # Rollback si falla la conversión
        raise RuntimeError(f"Fallo durante la transcodificación de {file_path.name}: {e}")

    print(f"[PREFLIGHT] Transcodificación completada exitosamente. Artefacto seguro: {out_path.name}")
    return out_path