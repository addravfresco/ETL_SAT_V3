"""Módulo de utilidad de pre-vuelo: Contador de volumetría física de alta velocidad.

Calcula la cantidad exacta de registros físicos (saltos de línea) en un
archivo crudo masivo (TXT/CSV). Utiliza lectura de buffer en modo binario 
para evadir la decodificación de caracteres corruptos (Mojibake) y evitar 
el desbordamiento de memoria RAM durante la fase de auditoría inicial.
"""

import sys
import time

from pkg.globals import SAT_RAW_DIR


def contar_lineas_rapido(nombre_archivo: str) -> None:
    """Ejecuta el escaneo de volumetría física sobre el archivo crudo.

    Realiza una lectura secuencial del archivo utilizando un buffer binario 
    para identificar saltos de línea físicos sin sobrecargar la memoria 
    principal ni activar el motor de decodificación de Python.

    Args:
        nombre_archivo (str): Nombre físico del archivo objetivo con extensión.

    Raises:
        SystemExit: Si el archivo no existe en la ruta de montaje especificada
            o si ocurre una excepción de I/O durante la lectura binaria.
    """
    archivo_sat = SAT_RAW_DIR / nombre_archivo

    print("\n[INFO] Iniciando conteo forense de volumetría física...")
    print(f"[INFO] Archivo objetivo: {archivo_sat.name}")
    
    if not archivo_sat.exists():
        print(f"[ERROR] Archivo no localizado en el directorio fuente: {archivo_sat.parent}")
        sys.exit(1)

    print("[INFO] Lectura de buffer binario en curso. Por favor espere...")
    
    start_time = time.time()
    contador = 0
    
    # Lectura en modo binario (rb) para optimizar el I/O secuencial y 
    # prevenir la propagación de excepciones por decodificación anómala.
    try:
        with open(archivo_sat, "rb") as file:
            for _ in file:
                contador += 1
    except Exception as e:
        print(f"[CRITICAL ERROR] Interrupción I/O durante la lectura: {e}")
        sys.exit(1)

    end_time = time.time()
    minutos = (end_time - start_time) / 60
    segundos = (end_time - start_time) % 60
    
    print("[INFO] Escaneo volumétrico finalizado con éxito.")
    print(f"[RESULTADO] Total de filas físicas detectadas: {contador:,.0f}")
    
    if minutos >= 1:
        print(f"[INFO] Tiempo de ejecución: {int(minutos)} min y {segundos:.2f} seg\n")
    else:
        print(f"[INFO] Tiempo de ejecución: {segundos:.2f} segundos\n")


if __name__ == "__main__":
    # Validación de la interfaz de línea de comandos (CLI)
    if len(sys.argv) < 2:
        print("[WARN] Uso de CLI incorrecto.")
        print("[INFO] Sintaxis esperada: python contador.py <nombre_del_archivo>")
        print("[INFO] Ejemplo: python contador.py AECF_0129_Anexo1_TablaB.csv")
    else:
        archivo_objetivo = sys.argv[1]
        contar_lineas_rapido(archivo_objetivo)