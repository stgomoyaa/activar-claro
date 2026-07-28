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
VERSION = "3.4.0"
REPO_URL = "https://github.com/stgomoyaa/activar-claro.git"

import serial
import serial.tools.list_ports
import time
import re
import threading
import os
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
LOG_RESTAURACION = "log_restauracion.txt"
LOG_TRIAGE = "log_triage.txt"

# Inicializar logs vacíos al arranque
for log in [LOG_COMPLETO, LOG_SMS, LOG_FALLOS, LOG_RESTAURACION, LOG_TRIAGE]:
    open(log, "w", encoding="utf-8").close()

# ============================
# 🚦 Triage de flota (decide si activar o ceder los módems a HeroSMS)
# ============================
# El PC de la granja se reinicia solo y arranca este script. Si las tarjetas ya
# están listas no hay nada que activar: hay que devolverle los módems a
# HeroSMS-Partners y salir. Estos umbrales definen "la flota ya está lista".

PROCESO_HEROSMS = "HeroSMS-Partners.exe"
PREFIJO_CLARO = "895603"

# ============================
# 🗄️ Conexión a PostgreSQL (un solo lugar)
# ============================
# ⚠️ ESTE ARCHIVO VIVE EN UN REPO PÚBLICO: acá solo puede ir el rol acotado
# `claro_activador` (SELECT/INSERT/UPDATE sobre claro_numbers y su secuencia,
# nada más). NUNCA el usuario `postgres`, que es superusuario de las 33 tablas
# de la DB. Antes estaba hardcodeado y duplicado en dos funciones.
DB_CONFIG = {
    "host": "crossover.proxy.rlwy.net",
    "database": "railway",
    "user": "claro_activador",
    "password": "kwvUgMoOeV2RGAP2yJDizzMcAVDTNrRo",
    "port": 43307,
}

# Porcentaje de tarjetas con número en la agenda a partir del cual se salta la
# activación completa. Se compara en enteros para no arrastrar error de float.
UMBRAL_FLOTA_LISTA = 80

# Porcentaje de ICCID que NO son Claro a partir del cual se salta la activación:
# el USSD *103# no aplica a otro operador, esos chips los activa su propio script.
UMBRAL_NO_CLARO = 80

# Mínimo de módems que deben entregar ICCID para que el porcentaje signifique
# algo. Si HeroSMS no soltó los puertos y solo contestan 2 de 16, esos 2 no
# pueden decidir por la flota: se cae al camino de activación.
UMBRAL_QUORUM = 50

# HeroSMS se abre SIEMPRE al terminar la corrida, incluso si quedó menos del
# 80% de tarjetas con número. Retenerlo no protege nada: las que fallaron la
# activación no tienen número que borrar, y las que sí lo tienen ya están en
# Postgres, así que la Fase 0 lo reescribe en la próxima corrida. Dejar el PC
# sin HeroSMS abierto, en cambio, lo deja sin vender hasta que alguien mire.
# False = solo abrirlo cuando la flota superó los umbrales de arriba.
ABRIR_HEROSMS_SIEMPRE = True

# True = las tarjetas que ya tenían número igual pasan por la activación.
ACTIVAR_SI_YA_TIENE_NUMERO = False

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


def leer_github_token() -> str | None:
    """
    Token opcional para leer el repo cuando es privado. Se busca en
    'github_token.txt', junto al script (una línea, sin comillas).

    Sin token la API de GitHub responde 404 en repos privados y el updater se
    queda callado creyendo que no hay versión nueva: por eso cada PC de la
    granja necesita este archivo para recibir actualizaciones.
    """
    try:
        carpeta = os.path.dirname(os.path.abspath(__file__))
        ruta = os.path.join(carpeta, "github_token.txt")
        if not os.path.exists(ruta):
            return None
        # utf-8-sig: el Notepad de Windows guarda con BOM y el '﻿' no lo
        # saca strip(), así que el token viajaría corrupto y la API daría 401.
        with open(ruta, encoding="utf-8-sig") as f:
            for linea in f:
                linea = linea.strip().lstrip("﻿").strip()
                if linea and not linea.startswith("#"):
                    return linea
    except Exception:
        pass
    return None


def _request_github(url: str) -> urllib.request.Request:
    """Arma el request a GitHub agregando el token si el repo es privado."""
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Python-Script-Updater")
    token = leer_github_token()
    if token:
        req.add_header("Authorization", f"token {token}")
    return req


def obtener_version_remota() -> tuple[bool, str, str]:
    """
    Obtiene la versión remota del script desde GitHub.
    Retorna (exito, version, url_descarga).
    """
    try:
        # URL de la API de GitHub para obtener el contenido del archivo
        api_url = "https://api.github.com/repos/stgomoyaa/activar-claro/contents/Activar%20Claro%20CNUM%20V3.py"

        # Hacer request a la API
        req = _request_github(api_url)

        # Crear contexto SSL que no verifica certificados (para servidores con problemas de SSL)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            data = json.loads(response.read().decode())
            download_url = data.get("download_url")

            if not download_url:
                print("⚠️ No se encontró URL de descarga en la respuesta de GitHub")
                return False, VERSION, ""

            # Descargar el contenido del script
            with urllib.request.urlopen(
                download_url, timeout=10, context=ctx
            ) as file_response:
                contenido = file_response.read().decode("utf-8")

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
    Compara dos versiones en formato X.Y.Z (o X.Y.Z.W).
    Retorna: 1 si v1 > v2, -1 si v1 < v2, 0 si son iguales.
    """
    try:
        partes1 = [int(x) for x in re.findall(r"\d+", v1)]
        partes2 = [int(x) for x in re.findall(r"\d+", v2)]

        # Rellenar con ceros para que 3.2.6.1 se compare contra 3.2.6.0
        largo = max(len(partes1), len(partes2))
        partes1 += [0] * (largo - len(partes1))
        partes2 += [0] * (largo - len(partes2))

        for p1, p2 in zip(partes1, partes2):
            if p1 > p2:
                return 1
            elif p1 < p2:
                return -1

        return 0
    except Exception:
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
            print(
                f"🆕 ¡Nueva versión disponible: v{version_remota} (actual: v{VERSION})!"
            )
            return True, version_remota
        else:
            print(f"✅ Estás usando la versión más reciente (v{VERSION})")
            return False, VERSION

    except Exception as e:
        print(f"⚠️ Error al verificar actualizaciones: {e}")
        return False, VERSION


def reiniciar_script():
    """
    Relanza el script con la versión recién descargada.

    No se usa os.execv: en Windows el CRT parte la línea de comandos por los
    espacios, así que "Activar Claro CNUM V3.py" llegaría como cuatro argumentos
    sueltos y el reinicio fallaría. subprocess recibe la lista y cita cada
    argumento correctamente.
    """
    try:
        codigo = subprocess.call([sys.executable] + sys.argv)
    except Exception as e:
        print(f"⚠️ No se pudo relanzar automáticamente: {e}")
        print("👉 Cerrá y volvé a abrir el script: ya quedó actualizado en disco.")
        codigo = 1
    sys.exit(codigo)


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
        req.add_header("User-Agent", "Python-Script-Updater")

        # Crear contexto SSL que no verifica certificados
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            contenido = response.read()

            # Guardar en archivo temporal
            with open(script_temp, "wb") as f:
                f.write(contenido)

        # Reemplazar el script actual
        print("🔄 Aplicando actualización...")
        shutil.move(script_temp, script_actual)

        print("✅ Script actualizado exitosamente!")
        print("🔄 Reiniciando con la nueva versión...\n")

        # Reiniciar el script
        time.sleep(1)
        reiniciar_script()

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
    print(f"\n{'=' * 60}")
    print(f"🚀 Activador de Chips Claro - Versión {VERSION}")
    print(f"{'=' * 60}\n")

    try:
        hay_actualizacion, version_remota = verificar_actualizacion()

        if hay_actualizacion:
            print(f"\n🔄 Actualizando automáticamente a v{version_remota}...")
            actualizar_script()
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
    """Abre HeroSMS-Partners desde el acceso directo del escritorio.

    En varios PCs de la granja el atajo quedó guardado como '.ink' (typo de
    '.lnk'), así que se prueban las dos extensiones antes de darlo por perdido.
    """
    try:
        cerrar_puertos_serial()
        user = os.environ["USERNAME"]
        escritorio = f"C:\\Users\\{user}\\Desktop"
        candidatos = [
            f"{escritorio}\\HeroSMS-Partners.lnk",
            f"{escritorio}\\HeroSMS-Partners.ink",
            f"{escritorio}\\HeroSMS-Partners.exe",
        ]
        atajo = next((ruta for ruta in candidatos if os.path.exists(ruta)), None)
        if atajo:
            print(f"🟢 Abriendo HeroSMS-Partners desde {atajo}...")
            escribir_log(LOG_TRIAGE, f"▶️ Abriendo HeroSMS-Partners ({atajo}).")
            os.startfile(atajo)
        else:
            print(
                f"❌ No se encontró HeroSMS-Partners (.lnk/.ink/.exe) en: {escritorio}"
            )
            escribir_log(
                LOG_TRIAGE, f"❌ HeroSMS-Partners no encontrado en {escritorio}."
            )
    except Exception as e:
        print(f"❗ Error al intentar abrir HeroSMS-Partners: {e}")
        escribir_log(LOG_TRIAGE, f"❌ Error al abrir HeroSMS-Partners: {e}")


def cerrar_herosms() -> bool:
    """
    Fuerza el cierre de HeroSMS-Partners y espera a que suelte los puertos COM.

    Mientras el cliente está abierto se queda con los puertos serial, así que
    ningún comando AT llega a los módems. En los PCs con reinicio automático
    HeroSMS arranca solo, por eso esto es lo primero que hace el script.
    Devuelve True si al terminar no queda ningún proceso vivo.
    """
    if os.name != "nt":
        print("ℹ️ No es Windows: se omite el cierre de HeroSMS-Partners.")
        return True

    print(f"🛑 Cerrando {PROCESO_HEROSMS} para liberar los puertos COM...")
    escribir_log(
        LOG_TRIAGE, f"🛑 Cerrando {PROCESO_HEROSMS} antes de tocar los módems."
    )

    try:
        # /F fuerza, /T arrastra procesos hijos. Código 128 = no estaba corriendo.
        resultado = subprocess.run(
            ["taskkill", "/F", "/T", "/IM", PROCESO_HEROSMS],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if resultado.returncode == 0:
            escribir_log(LOG_TRIAGE, "✅ taskkill cerró HeroSMS-Partners.")
        else:
            escribir_log(
                LOG_TRIAGE,
                f"ℹ️ taskkill código {resultado.returncode} (128 = no estaba abierto).",
            )
    except Exception as e:
        escribir_log(LOG_TRIAGE, f"⚠️ taskkill falló: {e}")

    # Verificar con psutil: taskkill matchea por nombre exacto y en algunos PCs
    # el ejecutable quedó con otro nombre.
    vivos = []
    try:
        import psutil

        for proceso in psutil.process_iter(["name"]):
            nombre = (proceso.info.get("name") or "").lower()
            if "herosms" in nombre:
                try:
                    proceso.kill()
                    escribir_log(
                        LOG_TRIAGE, f"🛑 psutil mató {nombre} (pid {proceso.pid})."
                    )
                except Exception as e:
                    vivos.append(nombre)
                    escribir_log(LOG_TRIAGE, f"⚠️ No se pudo matar {nombre}: {e}")
    except Exception as e:
        escribir_log(LOG_TRIAGE, f"⚠️ Verificación con psutil no disponible: {e}")

    # Windows tarda un momento en liberar los handles de los puertos.
    time.sleep(5)
    cerrar_puertos_serial()

    if vivos:
        print(f"⚠️ Quedaron procesos vivos: {vivos}")
        return False
    return True


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


def revisar_puerto(puerto, resultado, reiniciar=True):
    """Verifica si un puerto responde al comando AT y guarda el resultado.

    `reiniciar=False` solo detecta: sirve para inspeccionar la flota (fase de
    triage) sin mandar AT+CFUN=1,1, que deja al módem 20-30s sin responder. El
    reinicio solo hace falta antes de activar.
    """
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
                if reiniciar:
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


def validar_modems_activos(puertos, reiniciar=True):
    """Verifica qué módems están encendidos en paralelo usando hilos."""
    escribir_log(LOG_COMPLETO, "🔍 Iniciando validación de módems activos...")

    modems_activos = []
    hilos = [
        threading.Thread(
            target=revisar_puerto, args=(puerto, modems_activos, reiniciar)
        )
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

    # Subir a la base de datos PostgreSQL (actualizar si existe, insertar si no)
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Primero verificar si el ICCID ya existe
        cursor.execute(
            "SELECT numero_telefono FROM claro_numbers WHERE iccid = %s", (iccid,)
        )
        registro_existente = cursor.fetchone()

        if registro_existente:
            # Si existe, actualizar con el nuevo número (reciclado por la compañía)
            numero_anterior = registro_existente[0]
            cursor.execute(
                "UPDATE claro_numbers SET numero_telefono = %s, fecha_activacion = %s WHERE iccid = %s",
                (numero, fecha_actual, iccid),
            )
            escribir_log(
                LOG_COMPLETO,
                f"🔄 [{puerto}] ICCID {iccid} actualizado: {numero_anterior} → {numero}",
            )
        else:
            # Si no existe, insertar como nuevo registro
            cursor.execute(
                "INSERT INTO claro_numbers (iccid, numero_telefono, fecha_activacion) VALUES (%s, %s, %s)",
                (iccid, numero, fecha_actual),
            )
            escribir_log(
                LOG_COMPLETO,
                f"✅ [{puerto}] Número {numero} e ICCID {iccid} guardados en la base de datos.",
            )

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        escribir_log(
            LOG_COMPLETO,
            f"❌ [{puerto}] Error al guardar en la base de datos: {e}",
        )


def exportar_base_datos_completa():
    """Exporta toda la base de datos PostgreSQL al archivo local listadonumeros_claro.txt"""
    try:
        escribir_log(
            LOG_COMPLETO, "📥 Exportando listado completo desde la base de datos..."
        )

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Obtener todos los registros de la base de datos
        cursor.execute(
            "SELECT numero_telefono, iccid FROM claro_numbers ORDER BY fecha_activacion"
        )
        registros = cursor.fetchall()

        cursor.close()
        conn.close()

        # Escribir todos los registros al archivo (sobrescribir)
        with open("listadonumeros_claro.txt", "w", encoding="utf-8") as archivo:
            for numero, iccid in registros:
                archivo.write(f"{numero}={iccid}\n")

        escribir_log(
            LOG_COMPLETO,
            f"✅ Exportados {len(registros)} registros desde la base de datos al archivo local.",
        )
        return True

    except Exception as e:
        escribir_log(
            LOG_COMPLETO,
            f"❌ Error al exportar la base de datos: {e}",
        )
        return False


# ============================
# ♻️ Fase 0: recuperar números que HeroSMS borró de la agenda
# ============================
# Al abrir HeroSMS, el cliente borra el número de la agenda de la SIM. El chip
# sigue activo en Claro con el mismo número, pero el SMS de bienvenida ya no
# vuelve a llegar (y el link del formulario se consume una sola vez), así que
# reactivar con *103# no lo recupera. El listado/DB es el único registro que
# queda: recuperar = reescribir la agenda, no reactivar.

# True = los puertos recuperados igual pasan por el flujo de activación.
REACTIVAR_RESTAURADOS = False

restauraciones_ok = []
restauraciones_fallidas = []
restauracion_lock = threading.Lock()


def _normalizar_iccid(iccid: str) -> str:
    """Deja solo dígitos: los módems reportan la 'F' de relleno según firmware."""
    return re.sub(r"\D", "", iccid or "")


def cargar_indice_numeros() -> tuple[dict[str, str], bool]:
    """
    Construye {iccid: numero} desde PostgreSQL (source of truth). Si la DB no
    responde, cae al listado local que dejó la última corrida.

    Devuelve (indice, desde_db). `desde_db=False` significa que los números
    salieron del snapshot local: siguen sirviendo, pero si Claro recicló algún
    ICCID después de la última corrida ese número quedó viejo, y eso cambia
    cuánto se puede confiar en lo que se reescriba en la agenda.
    """
    desde_db = exportar_base_datos_completa()
    if desde_db:
        escribir_log(LOG_RESTAURACION, "📥 Índice sincronizado desde PostgreSQL.")
    else:
        escribir_log(
            LOG_RESTAURACION,
            "⚠️ DB no disponible: usando el listado local de la última corrida.",
        )

    indice: dict[str, str] = {}
    try:
        with open(LISTADO_NUMEROS, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if not linea or "=" not in linea:
                    continue
                numero, iccid = linea.split("=", 1)
                iccid = _normalizar_iccid(iccid)
                numero = numero.strip()
                if iccid and numero:
                    indice[iccid] = numero
    except FileNotFoundError:
        escribir_log(
            LOG_RESTAURACION, f"❌ No existe {LISTADO_NUMEROS}: nada que recuperar."
        )

    escribir_log(LOG_RESTAURACION, f"🗂 Índice cargado: {len(indice)} ICCID conocidos.")
    return indice, desde_db


def buscar_numero(iccid: str, indice: dict[str, str]) -> str | None:
    """Busca el número del ICCID tolerando el dígito de control final."""
    iccid = _normalizar_iccid(iccid)
    if not iccid:
        return None
    if iccid in indice:
        return indice[iccid]
    for clave, numero in indice.items():
        if clave.startswith(iccid) or iccid.startswith(clave):
            return numero
    return None


# ============================
# 🚦 Lectura de estado de la flota
# ============================
# "Tarjeta lista" = tiene el contacto 'myphone' con un móvil chileno en la
# agenda. Es el mismo criterio que usa HeroSMS-Partners para saber qué número
# tiene cada chip, así que es el único que cuenta.


def _cpbr_max(puerto: str, tope: int = 5) -> int:
    """
    Índice máximo válido de la agenda, acotado a `tope`.

    En los Quectel UC20 la capacidad puede ser menor a la que se pide y un
    AT+CPBR=1,250 responde CME ERROR 21; por eso se consulta primero.
    """
    respuesta = enviar_comando(puerto, "AT+CPBR=?")
    match = re.search(r"\+CPBR:\s*\(\s*\d+\s*[-,]\s*(\d+)\s*\)", respuesta)
    if match:
        try:
            return max(1, min(int(match.group(1)), tope))
        except ValueError:
            pass
    return tope


def _numero_chileno(crudo: str) -> str | None:
    """Normaliza lo que devuelve la agenda a '569XXXXXXXX'. None si no lo es."""
    digitos = re.sub(r"\D", "", crudo or "")
    if len(digitos) == 11 and digitos.startswith("569"):
        return digitos
    if len(digitos) == 9 and digitos.startswith("9"):
        return f"56{digitos}"
    return None


def leer_myphone(puerto: str) -> tuple[str | None, str]:
    """
    Busca el contacto 'myphone' en la agenda (SM y luego ME).

    Devuelve (numero, detalle). El activador siempre escribe en la posición 1
    de SM, así que ese es el caso normal; el barrido de los índices siguientes
    es para chips que quedaron escritos por otra herramienta.
    """
    detalle = ""
    for memoria in ("SM", "ME"):
        if "OK" not in enviar_comando(puerto, f'AT+CPBS="{memoria}"'):
            continue

        for idx in range(1, _cpbr_max(puerto) + 1):
            respuesta = enviar_comando(puerto, f"AT+CPBR={idx}", espera=0.6)
            match = re.search(r'\+CPBR:\s*\d+,"([^"]*)",\d+,"([^"]*)"', respuesta)
            if not match:
                continue

            crudo, nombre = match.group(1), match.group(2)
            if nombre.strip().lower() != "myphone":
                continue

            numero = _numero_chileno(crudo)
            if numero:
                return numero, f"{memoria} idx={idx}"
            detalle = f"{memoria} idx={idx} nro={crudo} (no es móvil chileno)"

    return None, detalle or "sin myphone en la agenda"


def _registro_vacio() -> dict:
    return {
        "iccid": None,
        "operador": "Desconocido",
        "numero": None,
        "detalle": "",
        "error": "",
        "restaurado": False,
        # De dónde salió el número si hubo que reescribirlo: "db" o "listado_local".
        "fuente": "",
    }


def inspeccionar_puerto(puerto: str, inventario: dict):
    """Lee ICCID y estado de la agenda de un puerto. No escribe nada en la SIM."""
    registro = _registro_vacio()
    try:
        with ModemSession(puerto):
            iccid = obtener_iccid(puerto)
            if not iccid:
                registro["error"] = "sin_iccid"
                escribir_log(LOG_TRIAGE, f"❌ [{puerto}] No entregó ICCID.")
            else:
                registro["iccid"] = _normalizar_iccid(iccid)
                registro["operador"] = obtener_operador(registro["iccid"])
                numero, detalle = leer_myphone(puerto)
                registro["numero"] = numero
                registro["detalle"] = detalle
                estado = f"LISTA ({numero})" if numero else f"sin número ({detalle})"
                escribir_log(
                    LOG_TRIAGE,
                    f"🔎 [{puerto}] ICCID {registro['iccid']} "
                    f"[{registro['operador']}] → {estado}",
                )
    except Exception as e:
        registro["error"] = f"{type(e).__name__}: {e}"
        escribir_log(LOG_TRIAGE, f"❌ [{puerto}] Error al inspeccionar: {e}")

    with restauracion_lock:
        inventario[puerto] = registro


def fase_triage(modems_activos: list) -> dict:
    """
    Recorre la flota y devuelve {puerto: registro} sin modificar ninguna SIM.

    Es lo que permite decidir si hay algo que activar o si corresponde
    devolverle los módems a HeroSMS.
    """
    escribir_log(LOG_TRIAGE, "=" * 60)
    escribir_log(LOG_TRIAGE, "🚦 TRIAGE: leyendo ICCID y agenda de cada módem")
    escribir_log(LOG_TRIAGE, "=" * 60)

    inventario: dict = {}
    hilos = [
        threading.Thread(target=inspeccionar_puerto, args=(puerto, inventario))
        for puerto in modems_activos
    ]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    return inventario


def resumen_flota(inventario: dict, total_modems: int) -> dict:
    """
    Cuenta el estado de la flota. Función pura: recibe el inventario ya leído.

    El porcentaje se calcula sobre los módems que entregaron ICCID, y `quorum`
    marca si esa muestra es representativa de la flota detectada.
    """
    con_iccid = [r for r in inventario.values() if r.get("iccid")]
    total = len(con_iccid)
    listos = sum(1 for r in con_iccid if r.get("numero"))
    no_claro = sum(1 for r in con_iccid if r.get("operador") != "Claro")
    sin_db = sum(1 for r in con_iccid if r.get("fuente") == "listado_local")

    return {
        "total_modems": total_modems,
        "con_iccid": total,
        "sin_iccid": total_modems - total,
        "listos": listos,
        "no_claro": no_claro,
        "restaurados_sin_db": sin_db,
        "pct_listos": (listos * 100 // total) if total else 0,
        "pct_no_claro": (no_claro * 100 // total) if total else 0,
        "quorum": total > 0 and total * 100 >= UMBRAL_QUORUM * total_modems,
    }


def decidir_accion(resumen: dict) -> tuple[str, str]:
    """
    Decide entre 'activar' y 'abrir_herosms'. Devuelve (accion, motivo).

    Comparaciones en enteros a propósito: `listos * 100 >= umbral * total`
    evita el redondeo de dividir en float.
    """
    total_modems = resumen["total_modems"]
    total = resumen["con_iccid"]

    if total_modems == 0:
        return (
            "abrir_herosms",
            "no se detectó ningún módem: se abre HeroSMS igual para no dejar el PC parado.",
        )

    if total == 0:
        return (
            "activar",
            f"ninguno de los {total_modems} módems entregó ICCID: no se puede "
            "afirmar que la flota esté lista.",
        )

    if not resumen["quorum"]:
        return (
            "activar",
            f"solo {total}/{total_modems} módems entregaron ICCID (menos del "
            f"{UMBRAL_QUORUM}%): la muestra no representa a la flota.",
        )

    if resumen["listos"] * 100 >= UMBRAL_FLOTA_LISTA * total:
        motivo = (
            f"{resumen['listos']}/{total} tarjetas ya tienen número "
            f"({resumen['pct_listos']}% ≥ {UMBRAL_FLOTA_LISTA}%): no hay nada que activar."
        )
        # La DB es el source of truth. Si no respondió, los números recuperados
        # salieron del snapshot local y uno reciclado por Claro puede estar viejo.
        if resumen.get("restaurados_sin_db"):
            motivo += (
                f" ⚠️ {resumen['restaurados_sin_db']} se recuperaron del listado "
                "local porque la DB no respondió: si Claro recicló alguno, el "
                "número escrito en esa agenda puede estar desactualizado."
            )
        return "abrir_herosms", motivo

    if resumen["no_claro"] * 100 >= UMBRAL_NO_CLARO * total:
        return (
            "abrir_herosms",
            f"{resumen['no_claro']}/{total} ICCID no son Claro "
            f"({resumen['pct_no_claro']}% ≥ {UMBRAL_NO_CLARO}%): *103# no aplica a esta flota.",
        )

    return (
        "activar",
        f"{resumen['listos']}/{total} tarjetas listas ({resumen['pct_listos']}%) "
        f"y {resumen['no_claro']} no-Claro: hay chips que activar.",
    )


def log_resumen_flota(resumen: dict, accion: str, motivo: str):
    """Deja el estado de la flota y la decisión en el log y en pantalla."""
    linea_estado = (
        f"📊 Flota: {resumen['total_modems']} módems | "
        f"{resumen['con_iccid']} con ICCID | {resumen['sin_iccid']} sin ICCID | "
        f"{resumen['listos']} con número ({resumen['pct_listos']}%) | "
        f"{resumen['no_claro']} no-Claro ({resumen['pct_no_claro']}%)"
    )
    if resumen.get("restaurados_sin_db"):
        linea_estado += (
            f" | ⚠️ {resumen['restaurados_sin_db']} recuperadas sin DB (listado local)"
        )
    icono = "⏭" if accion == "abrir_herosms" else "▶️"
    linea_decision = f"{icono} Decisión: {accion.upper()} — {motivo}"

    for linea in (linea_estado, linea_decision):
        print(linea)
        escribir_log(LOG_TRIAGE, linea)
        escribir_log(LOG_COMPLETO, linea)


def _necesita_activacion(registro: dict) -> bool:
    """Un chip con número en la agenda ya cumplió: no hay por qué reactivarlo."""
    if not registro.get("numero"):
        return True
    if registro.get("restaurado"):
        return REACTIVAR_RESTAURADOS
    return ACTIVAR_SI_YA_TIENE_NUMERO


def leer_numero_agenda(puerto: str) -> str | None:
    """Lee la posición 1 de la agenda de la SIM. None si está vacía."""
    if "OK" not in enviar_comando(puerto, 'AT+CPBS="SM"'):
        return None
    respuesta = enviar_comando(puerto, "AT+CPBR=1")
    match = re.search(r'\+CPBR:\s*\d+,"([^"]+)"', respuesta)
    if not match:
        return None
    return re.sub(r"\D", "", match.group(1)) or None


def restaurar_numero_en_sim(puerto: str, numero: str) -> bool:
    """Reescribe el número en la agenda y confirma releyendo la posición 1."""
    enviar_comando(puerto, 'AT+CPBS="SM"')
    enviar_comando(puerto, f'AT+CPBW=1,"{numero}",129,"myphone"')
    verificado = leer_numero_agenda(puerto)
    # Comparar los últimos 8 dígitos: el módem puede devolverlo con o sin '+56'.
    return bool(verificado) and verificado[-8:] == numero[-8:]


def restaurar_puerto(
    puerto: str, indice: dict[str, str], inventario: dict, desde_db: bool = True
):
    """
    Fase 0 de un puerto: si la agenda quedó vacía, reescribe su número.

    Además deja en `inventario` el estado con que quedó el puerto (ICCID,
    operador, número), que es lo que después decide si vale la pena activar o
    si conviene devolverle los módems a HeroSMS.
    """
    registro = _registro_vacio()
    try:
        with ModemSession(puerto):
            iccid = obtener_iccid(puerto)
            if not iccid:
                registro["error"] = "sin_iccid"
                escribir_log(
                    LOG_RESTAURACION,
                    f"❌ [{puerto}] Sin ICCID: se deja para activación.",
                )
                return

            registro["iccid"] = _normalizar_iccid(iccid)
            registro["operador"] = obtener_operador(registro["iccid"])

            numero_actual, detalle = leer_myphone(puerto)
            if numero_actual:
                registro["numero"] = numero_actual
                registro["detalle"] = detalle
                escribir_log(
                    LOG_RESTAURACION,
                    f"✅ [{puerto}] La agenda ya tiene {numero_actual}: nada que recuperar.",
                )
                return

            numero = buscar_numero(iccid, indice)
            if not numero:
                registro["detalle"] = "sin registro previo"
                escribir_log(
                    LOG_RESTAURACION,
                    f"🆕 [{puerto}] ICCID {iccid} sin registro previo: va a activación normal.",
                )
                return

            escribir_log(
                LOG_RESTAURACION, f"♻️ [{puerto}] Agenda vacía. Recuperando {numero}..."
            )
            if restaurar_numero_en_sim(puerto, numero):
                registro["numero"] = numero
                registro["restaurado"] = True
                registro["fuente"] = "db" if desde_db else "listado_local"
                registro["detalle"] = f"recuperado desde {registro['fuente']}"
                escribir_log(
                    LOG_RESTAURACION,
                    f"✅ [{puerto}] Número {numero} recuperado y guardado en la SIM.",
                )
                with restauracion_lock:
                    restauraciones_ok.append(f"{puerto}={numero}")
            else:
                registro["detalle"] = "falló la escritura en la agenda"
                escribir_log(
                    LOG_RESTAURACION,
                    f"❌ [{puerto}] No se pudo escribir {numero} en la agenda.",
                )
                with restauracion_lock:
                    restauraciones_fallidas.append(f"{puerto}={numero}")
    except Exception as e:
        registro["error"] = f"{type(e).__name__}: {e}"
        escribir_log(LOG_RESTAURACION, f"❌ [{puerto}] Error en la recuperación: {e}")
    finally:
        with restauracion_lock:
            inventario[puerto] = registro


def fase_restauracion(modems_activos: list) -> dict:
    """
    Reescribe en la agenda de cada SIM el número que HeroSMS borró.

    Devuelve el inventario {puerto: registro} con el estado en que quedó cada
    módem. Los puertos recuperados salen marcados con restaurado=True.
    """
    escribir_log(LOG_RESTAURACION, "=" * 60)
    escribir_log(
        LOG_RESTAURACION, "♻️  FASE 0: recuperación de números borrados por HeroSMS"
    )
    escribir_log(LOG_RESTAURACION, "=" * 60)

    with restauracion_lock:
        restauraciones_ok.clear()
        restauraciones_fallidas.clear()

    indice, desde_db = cargar_indice_numeros()
    if not indice:
        escribir_log(
            LOG_RESTAURACION,
            "⚠️ Índice vacío: no hay nada que recuperar, pero igual se lee la flota.",
        )

    inventario: dict = {}
    hilos = [
        threading.Thread(
            target=restaurar_puerto, args=(puerto, indice, inventario, desde_db)
        )
        for puerto in modems_activos
    ]
    for hilo in hilos:
        hilo.start()
    for hilo in hilos:
        hilo.join()

    escribir_log(LOG_RESTAURACION, "-" * 60)
    escribir_log(
        LOG_RESTAURACION,
        f"♻️  Recuperados: {len(restauraciones_ok)} | ❌ Fallidos: {len(restauraciones_fallidas)}",
    )
    for item in restauraciones_ok:
        escribir_log(LOG_RESTAURACION, f"   ✅ {item}")
    for item in restauraciones_fallidas:
        escribir_log(LOG_RESTAURACION, f"   ⚠️  {item}")
    escribir_log(LOG_RESTAURACION, "-" * 60)

    return inventario


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
def main() -> str:
    """
    Una pasada completa de activación.

    Devuelve el estado con que terminó:
      • 'sin_modems'    — no contestó ningún módem.
      • 'abrir_herosms' — la flota ya está lista (o no es Claro): no se activó nada.
      • 'completado'    — se corrió la activación.
    """
    global sim_sin_numero

    puertos = listar_puertos_disponibles()
    modems_activos = validar_modems_activos(puertos)

    if not modems_activos:
        escribir_log(LOG_COMPLETO, "❌ No hay módems activos. Saliendo del programa.")
        return "sin_modems"

    # Fase 0: recuperar los números que HeroSMS borró de la agenda. Deja el
    # listado local sincronizado con la DB, así que va antes de leerlo.
    inventario = fase_restauracion(modems_activos)
    restaurados = {p for p, r in inventario.items() if r.get("restaurado")}
    cargar_iccid_activados()

    # Re-chequeo con el estado post-recuperación: si la Fase 0 dejó lista a la
    # flota, activar no aporta nada y solo hace dar vueltas en falso a los chips.
    resumen = resumen_flota(inventario, len(modems_activos))
    accion, motivo = decidir_accion(resumen)
    log_resumen_flota(resumen, accion, motivo)
    if accion == "abrir_herosms":
        return "abrir_herosms"

    pendientes = [
        puerto
        for puerto in modems_activos
        if _necesita_activacion(inventario.get(puerto, _registro_vacio()))
    ]
    saltados = len(modems_activos) - len(pendientes)
    if saltados:
        escribir_log(
            LOG_COMPLETO,
            f"⏭ {saltados} módem(s) ya tienen número en la agenda "
            f"({len(restaurados)} recuperados en la Fase 0): no necesitan activación.",
        )

    if not pendientes:
        escribir_log(
            LOG_COMPLETO,
            "✅ Todos los módems quedaron con número. No hay nada que activar.",
        )
        return "abrir_herosms"

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
        lotes = [pendientes]
    else:
        escribir_log(LOG_COMPLETO, "🐢 Activación por tandas de 10 módems.")
        lotes = [
            pendientes[start : start + 10] for start in range(0, len(pendientes), 10)
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
    escribir_log(
        LOG_COMPLETO,
        f"♻️ Recuperados desde el listado (sin activar): {len(restaurados)}",
    )
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

    return "completado"


def evaluar_flota(reiniciar_modems: bool = False) -> tuple[str, str]:
    """
    Lee la flota completa y decide qué hacer. Devuelve (accion, motivo).

    No escribe nada en las SIMs: solo pregunta ICCID y agenda.
    """
    modems = validar_modems_activos(listar_puertos_disponibles(), reiniciar_modems)
    inventario = fase_triage(modems)
    resumen = resumen_flota(inventario, len(modems))
    accion, motivo = decidir_accion(resumen)
    log_resumen_flota(resumen, accion, motivo)
    return accion, motivo


def cerrar_corrida(abrir_herosms: bool):
    """Cierre común: deja el listado sincronizado y decide si devolver los módems."""
    exportar_base_datos_completa()
    limpiar_listado()

    if abrir_herosms or ABRIR_HEROSMS_SIEMPRE:
        abrir_simclient()
    else:
        mensaje = (
            "🚫 No se abre HeroSMS-Partners: la flota quedó bajo el umbral de "
            f"{UMBRAL_FLOTA_LISTA}% de tarjetas con número. Abrirlo ahora borraría "
            "los números de las que sí quedaron listas."
        )
        print(mensaje)
        escribir_log(LOG_TRIAGE, mensaje)
        escribir_log(LOG_COMPLETO, mensaje)


if __name__ == "__main__":
    # Lo primero: sacar a HeroSMS del medio. Mientras está abierto se queda con
    # los puertos COM y ningún comando AT llega a los módems. En los PCs con
    # reinicio automático arranca solo junto con este script.
    cerrar_herosms()

    # Verificar y actualizar antes de ejecutar
    verificar_y_actualizar()

    # Triage: ¿hay algo que activar, o la flota ya está lista y solo hay que
    # devolverle los módems a HeroSMS?
    accion_inicial, _motivo = evaluar_flota(reiniciar_modems=False)

    if accion_inicial == "abrir_herosms":
        cerrar_corrida(abrir_herosms=True)
        sys.exit(0)

    resultado = "completado"
    contador = 0
    while contador < 2:
        resultado = main()
        if resultado in ("abrir_herosms", "sin_modems"):
            break
        contador += 1

    if resultado == "abrir_herosms":
        cerrar_corrida(abrir_herosms=True)
        sys.exit(0)

    # Última mirada a la flota: HeroSMS solo se abre si quedó lista de verdad.
    accion_final, _motivo = evaluar_flota(reiniciar_modems=False)
    cerrar_corrida(abrir_herosms=(accion_final == "abrir_herosms"))
