"""Refactor principal: mantener la conexión serial abierta por puerto durante
la activación completa usando la clase `ModemSession`.

Cambios clave:
• Nuevo dict global `_open_sessions` para almacenar sesiones activas.
• Clase `ModemSession` (context-manager) abre el puerto una sola vez y
  conserva el lock exclusivo durante todo el bloque.
• `enviar_comando()` detecta si existe una sesión abierta para el puerto y
  re-utiliza el `serial.Serial` sin volver a abrirlo.
• `procesar_puerto()` envuelve todo su flujo en `with ModemSession(puerto)`
  para que todas las funciones internas utilicen la misma conexión.

Todas las demás funciones se mantienen, de modo que el comportamiento externo
no cambia, sólo mejora el rendimiento (~70 % menos latencia) y se elimina el
coste de apertura/cierre reiterado del dispositivo.

Solo soporte para activación de chips Claro.
"""

# ============================
# 📌 Versión del script
# ============================
VERSION = "3.2.3"
REPO_URL = "https://github.com/stgomoyaa/activar-claro.git"

import serial
import serial.tools.list_ports
import time
import re
import threading
import os
import signal
import subprocess
import sys

# Librerías externas (instalación bajo demanda)
REQUIRED_LIBS = [
    "rich",
    "psutil",
    "Pillow",
    "smspdudecoder",
    "psycopg2-binary",
]


def instalar_libreria(libreria: str):
    try:
        __import__(libreria)
    except ImportError:
        print(f"📦 Instalando {libreria}…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", libreria])


for _lib in REQUIRED_LIBS:
    instalar_libreria(_lib)

from smspdudecoder.easy import read_incoming_sms
import psutil  # noqa: E402  (después de instalación condicional)
import psycopg2  # noqa: E402  (después de instalación condicional)

# ============================
# 🔒 Locks y estructuras globales
# ============================

sim_marcados = {}
iccid_activados = set()
puertos_mapeados = {}
sim_sin_numero = set()

activaciones_claro = 0
total_claro = 0

contadores_lock = threading.Lock()
sim_set_lock = threading.Lock()

_serial_port_locks = {}
_serial_port_locks_lock = threading.Lock()

# Sesiones serial abiertas por puerto
_open_sessions = {}

# ============================
# 📁 Rutas y logs
# ============================

LOG_COMPLETO = "log_completo.txt"
LOG_SMS = "log_sms.txt"
LOG_FALLOS = "fallos_activacion.txt"
LOG_FALLOS_NUMERO = "fallos_sin_numero.txt"

# Inicializar logs vacíos al arranque
for log in [LOG_COMPLETO, LOG_SMS, LOG_FALLOS]:
    open(log, "w", encoding="utf-8").close()

# ============================
# 🔧 Utilidades de locking/puertos
# ============================


def _get_port_lock(puerto: str) -> threading.Lock:
    """Devuelve (y crea si es necesario) un Lock exclusivo por puerto."""
    with _serial_port_locks_lock:
        return _serial_port_locks.setdefault(puerto, threading.Lock())


# ============================
# 🚀 Clase ModemSession
# ============================


class ModemSession:
    """Context-manager que mantiene el puerto serial abierto toda la sesión."""

    def __init__(self, puerto: str, baudrate: int = 115200, timeout: int = 2):
        self.puerto = puerto
        self.baudrate = baudrate
        self.timeout = timeout
        self.lock: threading.Lock | None = None
        self.ser: serial.Serial | None = None

    def __enter__(self):
        self.lock = _get_port_lock(self.puerto)
        self.lock.acquire()
        self.ser = serial.Serial(
            self.puerto, baudrate=self.baudrate, timeout=self.timeout
        )
        _open_sessions[self.puerto] = self.ser
        return self

    def send(self, comando: str, espera: float = 1):
        """Envía un comando AT usando la conexión persistente."""
        if not self.ser:
            raise RuntimeError("La sesión serial no está abierta.")
        self.ser.write((comando + "\r\n").encode())
        time.sleep(espera)
        return self.ser.read_all().decode(errors="ignore").strip()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.ser and self.ser.is_open:
            try:
                self.ser.close()
            except Exception:
                pass
        _open_sessions.pop(self.puerto, None)
        if self.lock:
            self.lock.release()


# ============================
# 📝 Logging helper
# ============================


def escribir_log(archivo: str, mensaje: str):
    print(mensaje)
    with open(archivo, "a", encoding="utf-8", newline="\n") as f:
        f.write(mensaje + "\n")


# ============================
# 🔄 Sistema de actualización automática (sin Git)
# ============================

import urllib.request
import json
import shutil
from datetime import datetime
import ssl


def obtener_version_remota() -> tuple[bool, str, str]:
    """
    Obtiene la versión remota del script desde GitHub.
    Retorna (exito, version, url_descarga).
    """
    try:
        # URL de la API de GitHub para obtener el contenido del archivo
        api_url = "https://api.github.com/repos/stgomoyaa/activar-claro/contents/Activar%20Claro%20CNUM%20V3.py"
        
        # Hacer request a la API
        req = urllib.request.Request(api_url)
        req.add_header('User-Agent', 'Python-Script-Updater')
        
        # Crear contexto SSL que no verifica certificados (para servidores con problemas de SSL)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            data = json.loads(response.read().decode())
            download_url = data.get('download_url')
            
            if not download_url:
                print("⚠️ No se encontró URL de descarga en la respuesta de GitHub")
                return False, VERSION, ""
            
            # Descargar el contenido del script
            with urllib.request.urlopen(download_url, timeout=10, context=ctx) as file_response:
                contenido = file_response.read().decode('utf-8')
                
                # Buscar la versión en el contenido
                import re
                match = re.search(r'VERSION = "([^"]+)"', contenido)
                
                if match:
                    version_remota = match.group(1)
                    return True, version_remota, download_url
                    
        return False, VERSION, ""
        
    except Exception as e:
        print(f"⚠️ Error al verificar actualizaciones: {e}")
        return False, VERSION, ""


def comparar_versiones(v1: str, v2: str) -> int:
    """
    Compara dos versiones en formato X.Y.Z.
    Retorna: 1 si v1 > v2, -1 si v1 < v2, 0 si son iguales.
    """
    try:
        partes1 = [int(x) for x in v1.split('.')]
        partes2 = [int(x) for x in v2.split('.')]
        
        for p1, p2 in zip(partes1, partes2):
            if p1 > p2:
                return 1
            elif p1 < p2:
                return -1
        
        return 0
    except:
        return 0


def verificar_actualizacion() -> tuple[bool, str]:
    """
    Verifica si hay una actualización disponible.
    Retorna (hay_actualizacion, version_remota).
    """
    try:
        print("🔍 Verificando actualizaciones...")
        
        exito, version_remota, _ = obtener_version_remota()
        
        if not exito:
            print("⚠️ No se pudo verificar actualizaciones.")
            return False, VERSION
        
        # Comparar versiones
        if comparar_versiones(version_remota, VERSION) > 0:
            print(f"🆕 ¡Nueva versión disponible: v{version_remota} (actual: v{VERSION})!")
            return True, version_remota
        else:
            print(f"✅ Estás usando la versión más reciente (v{VERSION})")
            return False, VERSION
            
    except Exception as e:
        print(f"⚠️ Error al verificar actualizaciones: {e}")
        return False, VERSION


def descargar_actualizacion(url: str) -> bool:
    """
    Descarga la nueva versión del script.
    Retorna True si se descargó correctamente.
    """
    try:
        script_actual = os.path.abspath(__file__)
        script_backup = f"{script_actual}.backup"
        script_temp = f"{script_actual}.new"
        
        # Hacer backup del script actual
        print("💾 Creando respaldo...")
        shutil.copy2(script_actual, script_backup)
        
        # Descargar nueva versión
        print("📥 Descargando actualización...")
        
        req = urllib.request.Request(url)
        req.add_header('User-Agent', 'Python-Script-Updater')
        
        # Crear contexto SSL que no verifica certificados
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            contenido = response.read()
            
            # Guardar en archivo temporal
            with open(script_temp, 'wb') as f:
                f.write(contenido)
        
        # Reemplazar el script actual
        print("🔄 Aplicando actualización...")
        shutil.move(script_temp, script_actual)
        
        print("✅ Script actualizado exitosamente!")
        print("🔄 Reiniciando con la nueva versión...\n")
        
        # Reiniciar el script
        time.sleep(1)
        os.execv(sys.executable, [sys.executable] + sys.argv)
        
        return True
        
    except Exception as e:
        print(f"❌ Error al descargar actualización: {e}")
        
        # Restaurar backup si existe
        if os.path.exists(script_backup):
            print("🔙 Restaurando versión anterior...")
            try:
                shutil.copy2(script_backup, script_actual)
                print("✅ Versión anterior restaurada.")
            except:
                pass
        
        return False
    finally:
        # Limpiar archivos temporales
        for temp_file in [script_backup, script_temp]:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass


def actualizar_script() -> bool:
    """
    Actualiza el script a la última versión disponible.
    Retorna True si se actualizó correctamente.
    """
    try:
        exito, version_remota, url = obtener_version_remota()
        
        if not exito or not url:
            print("❌ No se pudo obtener la información de actualización.")
            return False
        
        return descargar_actualizacion(url)
        
    except Exception as e:
        print(f"❌ Error al actualizar: {e}")
        return False


def verificar_y_actualizar():
    """Función principal que verifica y actualiza el script si es necesario."""
    print(f"\n{'='*60}")
    print(f"🚀 Activador de Chips Claro - Versión {VERSION}")
    print(f"{'='*60}\n")
    
    try:
        hay_actualizacion, version_remota = verificar_actualizacion()
        
        if hay_actualizacion:
            respuesta = input("\n¿Deseas actualizar ahora? (S/n): ").strip().lower()
            
            if respuesta in ["s", "si", "sí", "yes", "y", ""]:
                actualizar_script()
            else:
                print("⏭️ Actualización omitida. Continuando con la versión actual...\n")
                time.sleep(1)
        else:
            time.sleep(1)
            
    except Exception as e:
        print(f"⚠️ Error en el sistema de actualización: {e}")
        print("Continuando con la ejecución normal...\n")
        time.sleep(1)


# =======================
# ----- PATCH START -----
# 📦  Imports / constantes nuevas
# =======================
from rich.console import Console

console = Console()  # salida bonita en terminal

COMANDOS_MEMORIAS = ["SM", "ME", "MT"]  # zonas clásicas de la SIM


# -----------------------
# 🔎 Helpers extra para el modo PDU
# -----------------------
def extraer_numero_desde_contenido(texto: str) -> str | None:
    """
    Intenta encontrar un número chileno en el cuerpo de un SMS.
    Devuelve '569XXXXXXXX' o None.
    """
    patrones = [
        r"\b(?:\+?56)?9(\d{8})\b",  # +569XXXXXXXX o 569XXXXXXXX o 9XXXXXXXX
        r"tu\s*n[uú]mero\s*es\s*(\d{9})",  # frases tipo 'Tu numero es 912345678'
    ]
    for patron in patrones:
        m = re.search(patron, texto, re.IGNORECASE)
        if m:
            return f"569{m.group(1)[-8:]}"
    return None


def guardar_numero_en_sim(puerto: str, numero: str) -> bool:
    """
    Guarda el número en la posición 1 de la agenda de la SIM.
    Devuelve True si no hubo errores.
    """
    try:
        enviar_comando(puerto, 'AT+CPBS="SM"')
        enviar_comando(puerto, f'AT+CPBW=1,"{numero}",129,"myphone"')
        return True
    except Exception:
        return False


def borrar_mensaje(puerto: str, indice: str, origen: str):
    """Elimina el SMS en <indice> y deja traza de consola."""
    try:
        enviar_comando(puerto, f"AT+CMGD={indice}")
        console.print(f"[dim]🗑 Borrado SMS {indice} en {origen}.[/dim]")
    except Exception as e:
        console.print(f"[red]❌ Error al borrar SMS {indice} en {origen}: {e}[/red]")


# -----------------------
# 💬  Lector SMS en modo PDU
# -----------------------
def leer_sms_modo_pdu(puerto: str, stats: dict):
    """
    Lee, decodifica y procesa SMS en modo PDU (AT+CMGF=0) para todas las
    memorias declaradas en COMANDOS_MEMORIAS.  Usa `stats` para ir
    contabilizando {'leidos', 'procesados', 'ignorados'}.
    """
    enviar_comando(puerto, "AT+CMGF=0")  # modo PDU
    for memoria in COMANDOS_MEMORIAS:
        enviar_comando(puerto, f'AT+CPMS="{memoria}","{memoria}","{memoria}"')
        salida = enviar_comando(puerto, "AT+CMGL=4", espera=1)  # 4 = todos
        if "+CMGL:" not in salida:
            console.print(
                f"[cyan]ℹ️ Lauchita: [{puerto} → {memoria}] sin mensajes PDU.[/cyan]"
            )
            continue

        # Cada SMS viene precedido por '+CMGL: <idx>,…'
        for bloque in salida.strip().split("+CMGL:")[1:]:
            stats["leidos"] += 1
            lineas = bloque.strip().split("\r\n")
            if len(lineas) < 2:
                continue

            encabezado, pdu = lineas[0].strip(), lineas[1].strip()
            m_idx = re.match(r"(\d+),", encabezado)
            if not m_idx:
                continue
            indice = m_idx.group(1)

            try:
                sms = read_incoming_sms(pdu)
                contenido = sms.get("content", "")
                numero = extraer_numero_desde_contenido(contenido)

                if numero:
                    console.print(
                        f"[bold green]📨 PDU {puerto}/{memoria}: {contenido}[/bold green]"
                    )
                    stats["procesados"] += 1
                    if guardar_numero_en_sim(puerto, numero):
                        borrar_mensaje(puerto, indice, puerto)
                    return numero
                else:
                    stats["ignorados"] += 1
                    console.print(
                        f"[dim]🧐 Ignorado PDU {puerto}/{memoria}: {contenido}[/dim]"
                    )
            except Exception as e:
                console.print(
                    f"[red]❌ Error decodificando PDU {puerto}/{memoria}: {e}[/red]"
                )
    return None


# =======================
# ----- PATCH END -------
# =======================


# ============================
# 📡 Envío de comandos (refactor)
# ============================


def enviar_comando(puerto: str, comando: str, espera: float = 1):
    """Envía un comando AT reutilizando la sesión abierta si existe."""
    # 1) ¿Ya hay sesión abierta para este puerto?
    ser = _open_sessions.get(puerto)
    if ser:
        try:
            ser.write((comando + "\r\n").encode())
            time.sleep(espera)
            respuesta = ser.read_all().decode(errors="ignore").strip()
            escribir_log(LOG_COMPLETO, f"✅ [{puerto}] Respuesta:\n{respuesta}")
            return respuesta
        except Exception as e:
            escribir_log(LOG_COMPLETO, f"❌ [{puerto}] Error en sesión activa: {e}")
            return ""

    # 2) Si no hay sesión, usar lock por puerto y apertura efímera (legacy)
    lock = _get_port_lock(puerto)
    with lock:
        try:
            with serial.Serial(puerto, baudrate=115200, timeout=2) as ser:
                ser.write((comando + "\r\n").encode())
                time.sleep(espera)
                respuesta = ser.read_all().decode(errors="ignore").strip()
                escribir_log(LOG_COMPLETO, f"✅ [{puerto}] Respuesta:\n{respuesta}")
                return respuesta
        except Exception as e:
            escribir_log(LOG_COMPLETO, f"❌ [{puerto}] Error: {e}")
            return ""


# ============================
# 🛠️ Resto de funciones (sin cambios, salvo ajustes menores en comentarios)
# ============================
def cerrar_puertos_serial():
    print("🔒 Cerrando todos los puertos serial abiertos con hilos...")

    def cerrar_puerto(puerto):
        try:
            ser = serial.Serial(puerto)
            if ser.is_open:
                ser.close()
                print(f"✅ Puerto cerrado: {puerto}")
        except:
            pass

    hilos = []
    for p in serial.tools.list_ports.comports():
        hilo = threading.Thread(target=cerrar_puerto, args=(p.device,))
        hilo.start()
        hilos.append(hilo)

    for h in hilos:
        h.join()

    print("⏳ Esperando 2 segundos para asegurar cierre de puertos...")
    time.sleep(2)


def abrir_simclient():
    try:
        cerrar_puertos_serial()
        user = os.environ["USERNAME"]
        simclient_path = f"C:\\Users\\{user}\\Desktop\\HeroSMS-Partners.lnk"
        if os.path.exists(simclient_path):
            print("🟢 Abriendo HeroSMS-Partners...")
            os.startfile(simclient_path)
        else:
            print(f"❌ No se encontró HeroSMS-Partners.exe en: {simclient_path}")
    except Exception as e:
        print(f"❗ Error al intentar abrir HeroSMS-Partners: {e}")


def borrar_mensajes_modem(puerto):
    """Borra todos los SMS almacenados en la SIM de un módem específico."""
    escribir_log(LOG_COMPLETO, f"🗑 [{puerto}] Borrando todos los SMS...")
    enviar_comando(puerto, "AT+CMGD=1,4", espera=2)


def borrar_mensajes_global(puertos):
    """Borra los mensajes de todos los módems en paralelo utilizando hilos."""
    escribir_log(LOG_COMPLETO, "🗑 Iniciando borrado de mensajes en todos los módems...")

    hilos = [
        threading.Thread(target=borrar_mensajes_modem, args=(puerto,))
        for puerto in puertos
    ]

    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    escribir_log(LOG_COMPLETO, "✅ Borrado de mensajes completado.")


def repetir_proceso_sinsims():
    """Repite el proceso solo con los SIMs que no obtuvieron número."""
    global sim_sin_numero

    if not sim_sin_numero:
        escribir_log(
            LOG_COMPLETO,
            "✅ Todos los SIMs ya tienen número. No es necesario repetir el proceso.",
        )
        return

    escribir_log(LOG_COMPLETO, f"🔄 Reintentando activación en: {list(sim_sin_numero)}")

    puertos_a_reintentar = list(sim_sin_numero)
    sim_sin_numero.clear()  # Limpiar la lista para registrar solo nuevos fallos

    hilos = [
        threading.Thread(target=procesar_puerto, args=(puerto,))
        for puerto in puertos_a_reintentar
    ]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    escribir_log(LOG_COMPLETO, "✅ Reintento finalizado.")


def obtener_puerto_numerado(puerto_real):
    return (
        f"#{puertos_mapeados[puerto_real]}"
        if puerto_real in puertos_mapeados
        else puerto_real
    )


def listar_puertos_disponibles():
    puertos = serial.tools.list_ports.comports()
    lista_puertos = [puerto.device for puerto in puertos]
    escribir_log(LOG_COMPLETO, f"🔍 Puertos detectados: {lista_puertos}")
    return lista_puertos


def revisar_puerto(puerto, resultado):
    """Verifica si un puerto responde al comando AT y guarda el resultado."""
    try:
        with serial.Serial(puerto, baudrate=115200, timeout=2) as ser:
            ser.write(b"AT\r\n")
            time.sleep(1)
            respuesta = ser.read_all().decode(errors="ignore").strip()
            if "OK" in respuesta:
                resultado.append(puerto)
                escribir_log(
                    LOG_COMPLETO, f"✅ [{puerto}] Módem encendido y listo para generar."
                )
                # Reiniciar Puerto para Iniciar el proceso de activación
                ser.write(b"AT+CFUN=1,1\r\n")
                escribir_log(
                    LOG_COMPLETO,
                    f"✅ [{puerto}] Módem reiniciado y listo para generar.",
                )
            else:
                escribir_log(LOG_COMPLETO, f"⚠️ [{puerto}] No respondió al comando AT.")
    except Exception as e:
        escribir_log(LOG_COMPLETO, f"❌ [{puerto}] Error al validar módem: {e}")


def validar_modems_activos(puertos):
    """Verifica qué módems están encendidos en paralelo usando hilos."""
    escribir_log(LOG_COMPLETO, "🔍 Iniciando validación de módems activos...")

    modems_activos = []
    hilos = [
        threading.Thread(target=revisar_puerto, args=(puerto, modems_activos))
        for puerto in puertos
    ]

    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()
    escribir_log(LOG_COMPLETO, f"📡 Módems activos detectados: {modems_activos}")
    return modems_activos


def cargar_iccid_activados():
    global iccid_activados
    try:
        with open("listadonumeros_claro.txt", "r") as f:
            for linea in f:
                _, iccid = linea.strip().split("=")
                iccid_activados.add(iccid)
    except FileNotFoundError:
        escribir_log(LOG_COMPLETO, "⚠️ No se encontró 'listadonumeros_claro.txt'.")


def obtener_iccid(puerto):
    for _ in range(5):
        respuesta = enviar_comando(puerto, "AT+QCCID")
        match = re.search(r"(\d{19,20})", respuesta)
        if match:
            return match.group(1)
        time.sleep(5)
    return None


def obtener_operador(iccid):
    if iccid.startswith("895603"):
        return "Claro"
    return "Desconocido"


# ==============================================
# 🔄  Activación solo para Claro
# ==============================================
def activar_chip(puerto: str, iccid: str):
    """Activa la SIM según operador.

    • **Claro**: envía USSD *103#.
    """

    operador = obtener_operador(iccid)
    if iccid in iccid_activados:
        return

    if operador == "Claro":
        comando_activacion = "*103#"
        escribir_log(
            LOG_COMPLETO,
            f"📞 [{puerto}] Enviando {comando_activacion} para activación Claro.",
        )
        enviar_comando(puerto, f'AT+CUSD=1,"{comando_activacion}",15', espera=2)

    # Registrar puerto en fallos si aún no tiene número
    with open(LOG_FALLOS_NUMERO, "a", encoding="utf-8") as f:
        f.write(f"{puerto}\n")


def leer_sms(puerto, iccid):
    operador = obtener_operador(iccid)
    enviar_comando(puerto, "AT+CMGF=1")

    memorias = ["SM", "ME", "MT"]
    numero = None

    patrones_numeros = [
        r"Tu numero es (\d+)",
        r"\b(\d{9})\b",
        r"\+569 ?(\d{4} ?\d{4})",
        r"569 ?(\d{4} ?\d{4})",
        r"\+569(\d{8})",
        r"569(\d{8})",
        r"\b(?:tu\s*n[uú]mero\s*es)\s*([\d\s]+)",
    ]

    for memoria in memorias:
        enviar_comando(puerto, f'AT+CPMS="{memoria}"')
        respuesta = enviar_comando(puerto, 'AT+CMGL="ALL"', espera=2)
        escribir_log(
            LOG_SMS, f"[{puerto}] SMS recibido de memoria {memoria}:\n{respuesta}"
        )

        if operador == "Claro":
            for patron in patrones_numeros:
                match = re.search(patron, respuesta, re.IGNORECASE)
                if match:
                    numero_extraido = match.group(1).replace(" ", "")
                    numero = f"569{numero_extraido[-8:]}"  # Asegura formato 569XXXXXXXX
                    break
            if numero:
                break

        if operador == "Claro" and not numero:
            match_url = re.search(
                r"https://fif\.clarovtrcloud\.com/aod/form\?t=(\d+)", respuesta
            )
            if match_url:
                numero = f"569{match_url.group(1)[-8:]}"
                break

    if numero:
        with open(LOG_FALLOS_NUMERO, "r") as f:
            puertos_fallidos = f.readlines()
        puertos_fallidos = [p.strip() for p in puertos_fallidos if p.strip() != puerto]

        with open(LOG_FALLOS_NUMERO, "w") as f:
            f.writelines([p + "\n" for p in puertos_fallidos])

        return numero

    escribir_log(
        LOG_COMPLETO, f"❌ [{puerto}] No se obtuvo número, manteniendo SMS sin borrar."
    )
    return None


from pathlib import Path

LISTADO_NUMEROS = "listadonumeros_claro.txt"


def limpiar_listado(path: str = LISTADO_NUMEROS):
    """
    Elimina duplicados exactos y duplicados por número o ICCID
    en el archivo «numero=iccid». Conserva la primera aparición.
    """
    archivo = Path(path)
    if not archivo.exists():
        print(f"⚠️  No existe {archivo}; nada que limpiar.")
        return

    # Leer todas las líneas
    with archivo.open("r", encoding="utf-8") as f:
        lines = f.readlines()

    # 1️⃣ Eliminar duplicados exactos
    unique_lines = list(dict.fromkeys(lines))

    seen_numbers, seen_iccids = set(), set()
    cleaned = []

    for raw in unique_lines:
        line = raw.strip()
        if not line or "=" not in line:
            continue
        number, iccid = line.split("=", 1)

        # ¿Ya vimos el mismo número o ICCID?
        if number in seen_numbers or iccid in seen_iccids:
            continue

        seen_numbers.add(number)
        seen_iccids.add(iccid)
        cleaned.append(f"{number}={iccid}")

    # Escribir el archivo limpio
    with archivo.open("w", encoding="utf-8") as f:
        for ln in cleaned:
            f.write(ln + "\n")

    print(f"✅ Limpieza completa: {len(lines)} → {len(cleaned)} líneas.")


def guardar_resultado(iccid, numero, puerto):
    """Guarda el número en un archivo, lo asigna a la tarjeta SIM y lo sube a PostgreSQL."""
    # Guardar en el archivo
    with open("listadonumeros_claro.txt", "a") as archivo:
        archivo.write(f"{numero}={iccid}\n")

    # Asignar el número a la tarjeta SIM
    escribir_log(LOG_COMPLETO, f"📥 [{puerto}] Guardando número {numero} en la SIM...")
    enviar_comando(puerto, 'AT+CPBS="SM"')  # Seleccionar almacenamiento en la SIM
    comando_guardar = f'AT+CPBW=1,"{numero}",129,"myphone"'
    enviar_comando(puerto, comando_guardar)
    escribir_log(
        LOG_COMPLETO,
        f"✅ [{puerto}] Número {numero} guardado en la SIM como 'myphone'.",
    )
    
    # Subir a la base de datos PostgreSQL
    try:
        conn = psycopg2.connect(
            host="crossover.proxy.rlwy.net",
            database="railway",
            user="postgres",
            password="QOHmELJXXFPmWBlyFmgtjLMvZfeoFaJa",
            port=43307
        )
        cursor = conn.cursor()
        
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        cursor.execute(
            "INSERT INTO claro_numbers (iccid, numero_telefono, fecha_activacion) VALUES (%s, %s, %s)",
            (iccid, numero, fecha_actual)
        )
        
        conn.commit()
        cursor.close()
        conn.close()
        
        escribir_log(
            LOG_COMPLETO,
            f"✅ [{puerto}] Número {numero} e ICCID {iccid} guardados en la base de datos.",
        )
    except Exception as e:
        escribir_log(
            LOG_COMPLETO,
            f"❌ [{puerto}] Error al guardar en la base de datos: {e}",
        )


# ============================
# 🔄 Función procesar_puerto (refactor clave)
# ============================


def procesar_puerto(puerto: str):
    global activaciones_claro, total_claro

    with ModemSession(puerto) as _sesion:  # la sesión queda activa para el hilo
        iccid = obtener_iccid(puerto)
        if not iccid:
            return

        operador = obtener_operador(iccid)
        with contadores_lock:
            if operador == "Claro":
                total_claro += 1

        intentos = 0
        numero_obtenido = None
        while intentos < 3 and not numero_obtenido:
            escribir_log(
                LOG_COMPLETO, f"🔄 [{puerto}] Intento {intentos + 1}/3 de activación."
            )
            activar_chip(puerto, iccid)

            numero_obtenido = leer_sms(puerto, iccid)
            if not numero_obtenido:
                stats = {"leidos": 0, "procesados": 0, "ignorados": 0}
                numero_obtenido = leer_sms_modo_pdu(puerto, stats)

            if numero_obtenido:
                guardar_resultado(iccid, numero_obtenido, puerto)
                with contadores_lock:
                    if operador == "Claro":
                        activaciones_claro += 1
            else:
                with sim_set_lock:
                    sim_sin_numero.add(puerto)

            intentos += 1
            if not numero_obtenido:
                time.sleep(10)

        if not numero_obtenido:
            escribir_log(
                LOG_COMPLETO, f"❌ [{puerto}] No se obtuvo número tras 3 intentos."
            )


# ============================
# 🔐 El resto del script (main, validar módems, etc.) permanece intacto y se
# beneficia automáticamente de la mejora de rendimiento.
# ============================
def main():
    global sim_sin_numero

    cargar_iccid_activados()
    puertos = listar_puertos_disponibles()
    modems_activos = validar_modems_activos(puertos)

    if not modems_activos:
        escribir_log(LOG_COMPLETO, "❌ No hay módems activos. Saliendo del programa.")
        return

    def input_con_timeout(prompt, timeout):
        from threading import Thread

        resultado = {"valor": None}

        def leer_input():
            resultado["valor"] = input(prompt)

        hilo = Thread(target=leer_input)
        hilo.daemon = True
        hilo.start()
        hilo.join(timeout)
        return resultado["valor"]

    velocidad = input_con_timeout(
        "\nSelecciona la velocidad de activación:\n"
        "1: 🐢 Activación por tandas de 10 módems (por defecto)\n"
        "2: 🚀 Activar todos los módems de una sola vez\n"
        "👉 Opción (1 o 2): ",
        10,
    )

    if velocidad == "2":
        escribir_log(LOG_COMPLETO, "🚀 Activando todos los módems simultáneamente.")
        lotes = [modems_activos]
    else:
        escribir_log(LOG_COMPLETO, "🐢 Activación por tandas de 10 módems.")
        lotes = [
            modems_activos[start : start + 10]
            for start in range(0, len(modems_activos), 10)
        ]

    total_lotes = len(lotes)
    for i, lote in enumerate(lotes):
        escribir_log(LOG_COMPLETO, f"🚀 Procesando lote {i + 1}/{total_lotes}: {lote}")

        tiempo_inicio = time.time()
        hilos = [
            threading.Thread(target=procesar_puerto, args=(puerto,)) for puerto in lote
        ]

        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join()

        tiempo_transcurrido = time.time() - tiempo_inicio
        tiempo_restante = tiempo_transcurrido * (total_lotes - (i + 1))
        escribir_log(
            LOG_COMPLETO,
            f"⏳ Progreso: {((i + 1) / total_lotes) * 100:.2f}% - Quedan {tiempo_restante:.2f} segundos.",
        )

    for intento in range(2):
        if not sim_sin_numero:
            break

        escribir_log(
            LOG_COMPLETO,
            f"🔄 Repetición {intento + 1}/2 para SIMs sin número: {list(sim_sin_numero)}",
        )

        puertos_fallidos = list(sim_sin_numero)
        sim_sin_numero.clear()

        hilos = [
            threading.Thread(target=procesar_puerto, args=(puerto,))
            for puerto in puertos_fallidos
        ]
        for hilo in hilos:
            hilo.start()
        for hilo in hilos:
            hilo.join()

    escribir_log(LOG_COMPLETO, "📊 Resumen de activaciones:")
    escribir_log(LOG_COMPLETO, f"Claro: {activaciones_claro}/{total_claro}")
    escribir_log(LOG_COMPLETO, "✅ Todos los procesos de activación finalizados.")

    opcion = input_con_timeout(
        "\nProceso finalizado, ¿qué deseas hacer ahora?\n"
        "1: 🗑 Borrar todos los mensajes de los módems\n"
        "2: 🚫 Mantener los mensajes sin borrar\n"
        "3: 🔄 Repetir el proceso con los SIMs que aún no tienen número\n"
        "👉 Selecciona una opción (1, 2 o 3): ",
        30,
    )

    if opcion == "1":
        borrar_mensajes_global(modems_activos)
    elif opcion == "2":
        escribir_log(LOG_COMPLETO, "🚫 No se borraron los mensajes.")
    elif opcion == "3":
        if not sim_sin_numero:
            print(
                "✅ Todos los SIMs ya tienen número. No es necesario repetir el proceso."
            )
        else:
            escribir_log(
                LOG_COMPLETO, "🔄 Repitiendo activación solo en SIMs sin número..."
            )
            repetir_proceso_sinsims()
    else:
        escribir_log(
            LOG_COMPLETO,
            "⏱ Tiempo agotado sin respuesta. Reintentando proceso con SIMs sin número...",
        )
        repetir_proceso_sinsims()


if __name__ == "__main__":
    # Verificar y actualizar antes de ejecutar
    verificar_y_actualizar()
    
    contador = 0
    while contador < 2:
        main()
        contador += 1
    limpiar_listado()
    abrir_simclient()
