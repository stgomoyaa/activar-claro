"""
Script para sincronizar listadonumeros_claro.txt con la base de datos PostgreSQL.
Sube solo los registros únicos que no existen en la base de datos.
"""

import psycopg2
from datetime import datetime
import sys

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


def insertar_registros(conn, registros_nuevos):
    """Inserta los registros nuevos en la base de datos"""
    if not registros_nuevos:
        print("ℹ️ No hay registros nuevos para insertar.")
        return 0
    
    try:
        cursor = conn.cursor()
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        insertados = 0
        for numero, iccid in registros_nuevos:
            try:
                cursor.execute(
                    "INSERT INTO claro_numbers (iccid, numero_telefono, fecha_activacion) VALUES (%s, %s, %s)",
                    (iccid, numero, fecha_actual)
                )
                insertados += 1
            except psycopg2.IntegrityError:
                # Si hay un error de integridad (duplicado), continuar con el siguiente
                conn.rollback()
                continue
            except Exception as e:
                print(f"⚠️ Error al insertar {numero}={iccid}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        cursor.close()
        
        print(f"✅ Insertados {insertados} registros nuevos en la base de datos.")
        return insertados
    
    except Exception as e:
        print(f"❌ Error al insertar registros: {e}")
        conn.rollback()
        return 0


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
        
        # Insertar registros nuevos
        if registros_nuevos:
            print("📤 Insertando registros nuevos...")
            insertados = insertar_registros(conn, registros_nuevos)
            print()
            print(f"✅ Proceso completado: {insertados} registros insertados.")
        else:
            print("✅ Todos los registros ya existen en la base de datos.")
        
        # Cerrar conexión
        conn.close()
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

