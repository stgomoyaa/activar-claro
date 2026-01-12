"""
Script para sincronizar listadonumeros_claro.txt con la base de datos PostgreSQL.
Sube solo los registros únicos que no existen en la base de datos.
"""

import psycopg2
from datetime import datetime
import sys
import threading
from queue import Queue

# Configuración de la base de datos
DB_CONFIG = {
    "host": "crossover.proxy.rlwy.net",
    "database": "railway",
    "user": "postgres",
    "password": "QOHmELJXXFPmWBlyFmgtjLMvZfeoFaJa",
    "port": 43307
}

ARCHIVO_LOCAL = "listadonumeros_claro.txt"


def leer_archivo_local():
    """Lee el archivo local y retorna una lista de tuplas (numero, iccid)"""
    try:
        registros = []
        with open(ARCHIVO_LOCAL, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if linea and "=" in linea:
                    numero, iccid = linea.split("=", 1)
                    registros.append((numero.strip(), iccid.strip()))
        
        print(f"✅ Leídos {len(registros)} registros del archivo local.")
        return registros
    
    except FileNotFoundError:
        print(f"❌ No se encontró el archivo '{ARCHIVO_LOCAL}'")
        return []
    except Exception as e:
        print(f"❌ Error al leer el archivo: {e}")
        return []


def obtener_registros_existentes(conn):
    """Obtiene todos los números e ICCIDs que ya existen en la base de datos"""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT numero_telefono, iccid FROM claro_numbers")
        registros = cursor.fetchall()
        cursor.close()
        
        # Crear sets para búsqueda rápida
        numeros_existentes = {r[0] for r in registros}
        iccids_existentes = {r[1] for r in registros}
        
        print(f"📊 Base de datos actual: {len(registros)} registros")
        return numeros_existentes, iccids_existentes
    
    except Exception as e:
        print(f"❌ Error al consultar la base de datos: {e}")
        return set(), set()


def insertar_registro_worker(numero, iccid, resultado_queue, lock_print):
    """Worker que inserta un registro en la base de datos (ejecutado en un hilo)"""
    try:
        # Cada hilo crea su propia conexión
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute(
            "INSERT INTO claro_numbers (iccid, numero_telefono, fecha_activacion) VALUES (%s, %s, %s)",
            (iccid, numero, fecha_actual)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        with lock_print:
            print(f"✅ Insertado: {numero} = {iccid}")
        
        resultado_queue.put(('exito', numero, iccid))
        
    except psycopg2.IntegrityError:
        with lock_print:
            print(f"⚠️ Duplicado: {numero} = {iccid}")
        resultado_queue.put(('duplicado', numero, iccid))
    except Exception as e:
        with lock_print:
            print(f"❌ Error: {numero} = {iccid} ({e})")
        resultado_queue.put(('error', numero, iccid))


def insertar_registros_paralelo(registros_nuevos, max_hilos=10):
    """Inserta los registros usando múltiples hilos"""
    if not registros_nuevos:
        print("ℹ️ No hay registros nuevos para insertar.")
        return 0
    
    resultado_queue = Queue()
    lock_print = threading.Lock()
    hilos = []
    
    print(f"🚀 Iniciando inserción con {max_hilos} hilos paralelos...\n")
    
    # Crear e iniciar hilos en lotes
    total = len(registros_nuevos)
    procesados = 0
    
    for i in range(0, total, max_hilos):
        lote = registros_nuevos[i:i+max_hilos]
        hilos_lote = []
        
        for numero, iccid in lote:
            hilo = threading.Thread(
                target=insertar_registro_worker,
                args=(numero, iccid, resultado_queue, lock_print)
            )
            hilo.start()
            hilos_lote.append(hilo)
        
        # Esperar a que termine este lote antes de continuar con el siguiente
        for hilo in hilos_lote:
            hilo.join()
        
        procesados += len(lote)
        with lock_print:
            print(f"\n📊 Progreso: {procesados}/{total} ({(procesados/total)*100:.1f}%)\n")
    
    # Contar resultados
    exitosos = 0
    duplicados = 0
    errores = 0
    
    while not resultado_queue.empty():
        resultado, _, _ = resultado_queue.get()
        if resultado == 'exito':
            exitosos += 1
        elif resultado == 'duplicado':
            duplicados += 1
        elif resultado == 'error':
            errores += 1
    
    print(f"\n{'='*60}")
    print(f"📈 Resultados finales:")
    print(f"   ✅ Insertados exitosamente: {exitosos}")
    print(f"   ⚠️  Duplicados encontrados: {duplicados}")
    print(f"   ❌ Errores: {errores}")
    print(f"{'='*60}\n")
    
    return exitosos


def sincronizar():
    """Función principal que sincroniza el archivo local con la base de datos"""
    print("=" * 60)
    print("🔄 SINCRONIZACIÓN DE BASE DE DATOS")
    print("=" * 60)
    print()
    
    # Leer archivo local
    registros_locales = leer_archivo_local()
    if not registros_locales:
        print("❌ No hay registros para procesar.")
        return
    
    print()
    
    try:
        # Conectar a la base de datos
        print("🔌 Conectando a la base de datos...")
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Conexión exitosa.")
        print()
        
        # Obtener registros existentes
        numeros_existentes, iccids_existentes = obtener_registros_existentes(conn)
        print()
        
        # Filtrar registros nuevos (que no existan ni por número ni por ICCID)
        registros_nuevos = []
        duplicados_numero = 0
        duplicados_iccid = 0
        
        for numero, iccid in registros_locales:
            if numero in numeros_existentes:
                duplicados_numero += 1
            elif iccid in iccids_existentes:
                duplicados_iccid += 1
            else:
                registros_nuevos.append((numero, iccid))
        
        print(f"📋 Resumen del análisis:")
        print(f"   • Total registros en archivo: {len(registros_locales)}")
        print(f"   • Duplicados por número: {duplicados_numero}")
        print(f"   • Duplicados por ICCID: {duplicados_iccid}")
        print(f"   • Registros únicos a insertar: {len(registros_nuevos)}")
        print()
        
        # Cerrar conexión principal
        conn.close()
        
        # Insertar registros nuevos usando hilos
        if registros_nuevos:
            print("📤 Insertando registros nuevos...\n")
            insertados = insertar_registros_paralelo(registros_nuevos, max_hilos=20)
            print(f"✅ Proceso completado: {insertados} registros insertados.")
        else:
            print("✅ Todos los registros ya existen en la base de datos.")
        
        print()
        print("=" * 60)
        
    except psycopg2.OperationalError as e:
        print(f"❌ Error de conexión a la base de datos: {e}")
        print("Verifica que:")
        print("  • Tengas acceso a internet")
        print("  • Las credenciales sean correctas")
        print("  • El servidor esté disponible")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")


if __name__ == "__main__":
    try:
        sincronizar()
    except KeyboardInterrupt:
        print("\n\n⚠️ Proceso interrumpido por el usuario.")
        sys.exit(0)

