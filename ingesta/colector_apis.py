"""
Colector de APIs públicas -> Firebase (MonitorAqua, F2).

Consulta ThingSpeak, Environment Agency UK, USGS y Open-Meteo según ingesta/estaciones.yml,
convierte cada lectura al contrato (contratos/evento_aqua.schema.json), la valida y la escribe
en FIREBASE_PATH (la misma entrada que usa el ESP32) con una clave determinística
origen_estacion_timestamp y PUT (set): consultar dos veces la misma lectura no la duplica.

Cada fuente corre en su propio hilo e intervalo: si una API falla, se registra y las demás siguen.

Uso:
  python colector_apis.py              corre todas las fuentes habilitadas
  python colector_apis.py --una-vez    una sola pasada por fuente (prueba)
  python colector_apis.py --seco       no escribe en Firebase, solo muestra los eventos
  python colector_apis.py --solo-recarga [--seco]
                                       solo la recarga de arranque (abajo) y termina; para probarla

Recarga al arranque (modo en vivo, el contenedor local): antes del bucle, revisa las últimas RECARGA_REVISAR_H
horas de Firebase (24 por defecto) y, por origen, recarga desde su primer hueco interno de más de RECARGA_HUECO_MIN
minutos (45) o, si no hay, desde su última lectura, hasta ahora. Si un origen no tiene nada en esa revisión, busca
su última lectura dentro del tope de su API (recarga_max_dias en estaciones.yml). Escribe SOLO claves que no
existen en Firebase: las que existen pueden estar ya en Kafka y el puente las repetiría. Se desactiva con
RECARGA_ARRANQUE=0. Así una noche con la PC apagada no depende del cron de GitHub (que queda de respaldo).
  EDAD_MAX_H=7 python colector_apis.py --una-vez --ventana-min 360
                                       modo nube (GitHub Actions, .github/workflows/colector_apis.yml): pide
                                       TODAS las lecturas de los últimos N minutos, no solo la última, y las
                                       escribe con un PATCH por fuente. Las ventanas se solapan entre pasadas y la
                                       clave determinística evita duplicados. GitHub no garantiza el cron: con
                                       6 h de ventana, una pasada tardía rellena lo que dejaron las que faltaron.
                                       Sale con código 1 si alguna fuente falló (la ejecución queda en rojo).
"""
import argparse
import math
import os
import sys
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

from comun import (ORIGENES, URL_HISTORIAL, SesionFirebase, ahora_ms, cargar_validador, clave_firebase,
                   describir_error, errores_contrato, faltan_variables_firebase, log)

SERVICIO = "aqua-colector"
CONFIG_PATH = Path(os.getenv("ESTACIONES_PATH", Path(__file__).with_name("estaciones.yml")))
# Lecturas más viejas que esto no se escriben. En vivo coincide con la ventana del puente (VENTANA_H, 3 h); el
# workflow de la nube la sube con EDAD_MAX_H para su ventana de 6 h (docs/ingesta.md explica cuándo el puente no
# alcanza a ver una lectura de más de 3 h y queda solo en Firebase)
EDAD_MAX_MS = int(float(os.getenv("EDAD_MAX_H", os.getenv("VENTANA_H", "3"))) * 3600 * 1000)
USGS_API_KEY = os.getenv("USGS_API_KEY", "")
RECARGA_ARRANQUE = os.getenv("RECARGA_ARRANQUE", "1") != "0"
RECARGA_REVISAR_H = float(os.getenv("RECARGA_REVISAR_H", "24"))
RECARGA_HUECO_MIN = float(os.getenv("RECARGA_HUECO_MIN", "45"))
RECARGA_MARGEN_MIN = 30  # se recarga desde un poco antes del hueco: EA y USGS publican con ~1 h de atraso
MIN_MS, H_MS, DIA_MS = 60_000, 3_600_000, 86_400_000
HTTP = requests.Session()
HTTP.headers["User-Agent"] = "MonitorAqua/1.0 (UPeU Big Data, equipo Horeb)"


def iso_a_ms(texto):
    return int(datetime.fromisoformat(texto.replace("Z", "+00:00")).timestamp() * 1000)


def a_float(valor):
    try:
        return None if valor in (None, "") else float(valor)
    except (TypeError, ValueError):
        return None


# Conversión de unidades al sistema del contrato (°C, m, m³/s)
CONVERSIONES = {
    "degf": lambda v: (v - 32) * 5 / 9,
    "ft3/s": lambda v: v * 0.028316846592,
    "ft^3/s": lambda v: v * 0.028316846592,
    "cfs": lambda v: v * 0.028316846592,
    "ft": lambda v: v * 0.3048,
}


def convertir(valor, unidad):
    if valor is None or not unidad:
        return valor
    f = CONVERSIONES.get(unidad.strip().lower())
    return round(f(valor), 4) if f else valor


def evento_base(origen, estacion, timestamp, lat=None, lon=None):
    fuente_tipo, tipo_evento = ORIGENES[origen]
    return {
        "tipoEvento": tipo_evento,
        "eventId": clave_firebase(f"{origen}_{estacion}_{timestamp}"),
        "sensorId": f"{origen}-{estacion}",
        "origen": origen,
        "fuenteTipo": fuente_tipo,
        "timestamp": timestamp,
        "ingestaTs": ahora_ms(),
        "lat": lat,
        "lon": lon,
    }


# --- Fuentes ------------------------------------------------------------------
# Cada lector recibe la sección de estaciones.yml y devuelve una lista de eventos.

def desde_iso(ventana_min):
    return (datetime.now(timezone.utc) - timedelta(minutes=ventana_min)).strftime("%Y-%m-%dT%H:%M:%SZ")


def leer_thingspeak(cfg, ventana_min=None):
    eventos = []
    for canal in cfg["canales"]:
        try:
            params = {"minutes": ventana_min} if ventana_min else {"results": 1}
            if canal.get("read_key_env") and os.getenv(canal["read_key_env"]):
                params["api_key"] = os.getenv(canal["read_key_env"])
            d = HTTP.get(f"https://api.thingspeak.com/channels/{canal['id']}/feeds.json",
                         params=params, timeout=20).json()
            feeds = d.get("feeds") or []
            if not feeds:
                continue
            for fila in (feeds if ventana_min else feeds[-1:]):
                ev = evento_base("thingspeak", str(canal["id"]), iso_a_ms(fila["created_at"]),
                                 a_float(d["channel"].get("latitude")) or canal.get("lat"),
                                 a_float(d["channel"].get("longitude")) or canal.get("lon"))
                for medida, campo in canal["campos"].items():
                    valor = a_float(fila.get(campo))
                    escala = canal.get("escalas", {}).get(medida)
                    ev[medida] = round(valor * escala, 4) if valor is not None and escala else valor
                eventos.append(ev)
        except Exception as ex:  # noqa: BLE001 - un canal caído no detiene a los demás
            log(SERVICIO, status="fuente_error", fuente="thingspeak", estacion=canal["id"], error=describir_error(ex))
    return eventos


def leer_ea_uk(cfg, ventana_min=None):
    base = "https://environment.data.gov.uk/flood-monitoring"

    def estacion(est):
        try:
            # Sin _limit la API corta en 500 lecturas (~2.5 días de una estación con nivel y caudal)
            params = ({"since": desde_iso(ventana_min), "_sorted": "", "_limit": 10000} if ventana_min
                      else {"latest": ""})
            items = HTTP.get(f"{base}/id/stations/{est['ref']}/readings", params=params,
                             timeout=30).json().get("items", [])
            # Una lectura por instante: nivel (stage aguas arriba) y caudal en el mismo evento
            por_ts = {}
            for it in items:
                medida = it["measure"].rsplit("/", 1)[-1]
                ts = iso_a_ms(it["dateTime"])
                ev = por_ts.setdefault(ts, evento_base("ea-uk", est["ref"], ts, est.get("lat"), est.get("lon")))
                if medida.startswith(f"{est['ref']}-level-stage"):
                    ev["nivelM"] = a_float(it["value"])
                elif medida.startswith(f"{est['ref']}-flow"):
                    ev["caudalM3s"] = a_float(it["value"])
            return [ev for ev in por_ts.values() if "nivelM" in ev or "caudalM3s" in ev]
        except Exception as ex:  # noqa: BLE001 - una estación caída no detiene a las demás
            log(SERVICIO, status="fuente_error", fuente="ea-uk", estacion=est["ref"], error=describir_error(ex))
            return []

    # La API de EA tarda ~4 s por estación: en paralelo, una pasada de 23 estaciones baja de ~2 min a segundos
    with ThreadPoolExecutor(max_workers=8) as pool:
        return [ev for lista in pool.map(estacion, cfg["estaciones"]) for ev in lista]


USGS_PARAMETROS = {"00010": "tempAguaC", "00400": "ph", "00300": "oxigenoMgL"}


def leer_usgs(cfg, ventana_min=None):
    sitios = {e["id"]: e for e in cfg["estaciones"]}
    params = {"f": "json", "limit": 1000, "monitoring_location_id": ",".join(sitios),
              "parameter_code": ",".join(USGS_PARAMETROS), "statistic_id": "00011"}
    headers = {"X-Api-Key": USGS_API_KEY} if USGS_API_KEY else {}
    if ventana_min:
        # Serie completa de la ventana (colección continuous), no solo el último valor
        params.update(limit=10000, time=f"{desde_iso(ventana_min)}/{desde_iso(0)}")
        features, url = [], "https://api.waterdata.usgs.gov/ogcapi/v1/collections/continuous/items"
        while url:  # varios días de 10 estaciones superan el límite de 10,000 por página: se sigue el enlace "next"
            r = HTTP.get(url, params=params, headers=headers, timeout=60)
            r.raise_for_status()
            d = r.json()
            features += d.get("features", [])
            url = next((l["href"] for l in d.get("links", []) if l.get("rel") == "next"), None)
            params = None  # el enlace "next" ya trae los parámetros
        por_evento = {}
        for f in features:
            p = f["properties"]
            valor = a_float(p.get("value"))
            if valor is None or p["parameter_code"] not in USGS_PARAMETROS:
                continue
            sitio, ts = p["monitoring_location_id"], iso_a_ms(p["time"])
            est = sitios[sitio]
            ev = por_evento.setdefault((sitio, ts), evento_base(
                "usgs", sitio.replace("USGS-", ""), ts, est.get("lat"), est.get("lon")))
            ev[USGS_PARAMETROS[p["parameter_code"]]] = convertir(valor, p.get("unit_of_measure"))
        return list(por_evento.values())
    r = HTTP.get("https://api.waterdata.usgs.gov/ogcapi/v1/collections/latest-continuous/items",
                 params=params, headers=headers, timeout=60)
    r.raise_for_status()
    # Una estación puede tener series viejas (sensores retirados): por (estación, parámetro)
    # se queda la lectura más reciente con valor
    mejor = {}
    for f in r.json().get("features", []):
        p = f["properties"]
        valor = a_float(p.get("value"))
        if valor is None:
            continue
        clave = (p["monitoring_location_id"], p["parameter_code"])
        ts = iso_a_ms(p["time"])
        if clave not in mejor or ts > mejor[clave][0]:
            mejor[clave] = (ts, convertir(valor, p.get("unit_of_measure")))
    por_evento = {}
    for (sitio, parametro), (ts, valor) in mejor.items():
        est = sitios[sitio]
        ev = por_evento.setdefault((sitio, ts), evento_base(
            "usgs", sitio.replace("USGS-", ""), ts, est.get("lat"), est.get("lon")))
        ev[USGS_PARAMETROS[parametro]] = valor
    return list(por_evento.values())


def leer_open_meteo(cfg, ventana_min=None):
    eventos = []
    for ub in cfg["ubicaciones"]:
        if ventana_min:
            # Pasos de 15 min de la ventana: mismos instantes (y claves) que "current"
            d = HTTP.get("https://api.open-meteo.com/v1/forecast", timeout=30, params={
                "latitude": ub["lat"], "longitude": ub["lon"], "timeformat": "unixtime",
                "minutely_15": "temperature_2m,relative_humidity_2m", "forecast_minutely_15": 1,
                "past_minutely_15": max(1, ventana_min // 15)}).json()
            m, unidad = d["minutely_15"], d["minutely_15_units"]["temperature_2m"].replace("°", "deg")
            ahora = ahora_ms()
            for t, temp, hum in zip(m["time"], m["temperature_2m"], m["relative_humidity_2m"]):
                if temp is None or int(t) * 1000 > ahora:
                    continue
                ev = evento_base("open-meteo", ub["id"], int(t) * 1000, ub["lat"], ub["lon"])
                ev["temperaturaC"] = convertir(a_float(temp), unidad)
                ev["humedadPct"] = a_float(hum)
                eventos.append(ev)
            continue
        d = HTTP.get("https://api.open-meteo.com/v1/forecast", timeout=30, params={
            "latitude": ub["lat"], "longitude": ub["lon"], "timeformat": "unixtime",
            "current": "temperature_2m,relative_humidity_2m"}).json()
        actual = d["current"]
        ev = evento_base("open-meteo", ub["id"], int(actual["time"]) * 1000, ub["lat"], ub["lon"])
        ev["temperaturaC"] = convertir(a_float(actual["temperature_2m"]), d["current_units"]["temperature_2m"].replace("°", "deg"))
        ev["humedadPct"] = a_float(actual["relative_humidity_2m"])
        eventos.append(ev)
    return eventos


LECTORES = {"thingspeak": leer_thingspeak, "ea_uk": leer_ea_uk, "usgs": leer_usgs, "open_meteo": leer_open_meteo}


# --- Escritura -----------------------------------------------------------------

class Colector:
    def __init__(self, seco, ventana_min=None):
        self.seco = seco
        self.ventana_min = ventana_min
        self.validador = cargar_validador()
        self.sesion = None if seco else SesionFirebase(SERVICIO)
        self.ultimo_ts = {}  # sensorId -> último timestamp escrito (evita reescrituras inútiles)
        self.lock = threading.Lock()
        self.contadores = {"escritas": 0, "rechazadas": 0, "sin_cambio": 0, "antiguas": 0, "errores": 0}

    def contar(self, campo):
        with self.lock:
            self.contadores[campo] += 1

    def escribir(self, ev, fuente):
        if ev["timestamp"] <= self.ultimo_ts.get(ev["sensorId"], 0):
            self.contar("sin_cambio")
            return
        if ahora_ms() - ev["timestamp"] > EDAD_MAX_MS:
            self.contar("antiguas")
            self.ultimo_ts[ev["sensorId"]] = ev["timestamp"]
            log(SERVICIO, status="antigua", fuente=fuente, sensorId=ev["sensorId"], edadH=round(
                (ahora_ms() - ev["timestamp"]) / 3_600_000, 1))
            return
        errores = errores_contrato(self.validador, ev)
        if errores:
            self.contar("rechazadas")
            log(SERVICIO, status="rejected", fuente=fuente, eventId=ev["eventId"], errores=errores)
            return
        if self.seco:
            log(SERVICIO, status="dry_run", evento=ev)
        else:
            r = HTTP.put(f"{URL_HISTORIAL}/{ev['eventId']}.json", params={"auth": self.sesion.token(), "print": "silent"},
                         json=ev, timeout=30)
            r.raise_for_status()
        self.ultimo_ts[ev["sensorId"]] = ev["timestamp"]
        self.contar("escritas")
        log(SERVICIO, status="written", fuente=fuente, eventId=ev["eventId"], sensorId=ev["sensorId"])

    def escribir_lote(self, eventos, fuente, edad_max_ms=None):
        """Modo ventana: valida todo y escribe con UN PATCH multi-ruta (mismo efecto que un PUT por clave)."""
        lote = {}
        for ev in eventos:
            if ahora_ms() - ev["timestamp"] > (edad_max_ms or EDAD_MAX_MS):
                self.contar("antiguas")
                continue
            errores = errores_contrato(self.validador, ev)
            if errores:
                self.contar("rechazadas")
                log(SERVICIO, status="rejected", fuente=fuente, eventId=ev["eventId"], errores=errores)
                continue
            lote[ev["eventId"]] = ev
        if lote and not self.seco:
            r = HTTP.patch(f"{URL_HISTORIAL}.json", params={"auth": self.sesion.token(), "print": "silent"},
                           json=lote, timeout=60)
            r.raise_for_status()
        with self.lock:
            self.contadores["escritas"] += len(lote)
        log(SERVICIO, status="dry_run_batch" if self.seco else "written_batch", fuente=fuente, eventos=len(lote),
            desde=min((e["timestamp"] for e in lote.values()), default=None),
            hasta=max((e["timestamp"] for e in lote.values()), default=None))

    def pasada(self, fuente, cfg):
        inicio = time.time()
        eventos = sorted(LECTORES[fuente](cfg, self.ventana_min), key=lambda e: e["timestamp"])
        if self.ventana_min:
            try:
                self.escribir_lote(eventos, fuente)
            except Exception as ex:  # noqa: BLE001
                self.contar("errores")
                log(SERVICIO, status="write_error", fuente=fuente, error=describir_error(ex))
            log(SERVICIO, status="pasada", fuente=fuente, eventos=len(eventos),
                duracionS=round(time.time() - inicio, 1), **self.contadores)
            return
        for ev in eventos:
            try:
                self.escribir(ev, fuente)
            except Exception as ex:  # noqa: BLE001
                log(SERVICIO, status="write_error", fuente=fuente, eventId=ev["eventId"], error=describir_error(ex))
        log(SERVICIO, status="pasada", fuente=fuente, eventos=len(eventos),
            duracionS=round(time.time() - inicio, 1), **self.contadores)

    def bucle(self, fuente, cfg, una_vez):
        intervalo = cfg["intervalo_s"]
        while True:
            try:
                self.pasada(fuente, cfg)
            except Exception as ex:  # noqa: BLE001 - la API cayó: se registra y se reintenta en el próximo ciclo
                self.contar("errores")
                log(SERVICIO, status="fuente_error", fuente=fuente, error=describir_error(ex))
            if una_vez:
                return
            time.sleep(intervalo)


# --- Recarga al arranque ---------------------------------------------------------

ORIGEN_DE_FUENTE = {"thingspeak": "thingspeak", "ea_uk": "ea-uk", "usgs": "usgs", "open_meteo": "open-meteo"}


def lecturas_api_firebase(sesion, desde_ms):
    """{clave: lectura} de las APIs (las que traen "origen") con timestamp >= desde_ms."""
    r = HTTP.get(URL_HISTORIAL + ".json", timeout=300, params={
        "auth": sesion.token(), "orderBy": '"timestamp"', "startAt": desde_ms})
    r.raise_for_status()
    return {k: v for k, v in (r.json() or {}).items() if isinstance(v, dict) and v.get("origen")}


def inicio_recarga(ts, hueco_ms):
    """El comienzo del primer hueco interno mayor que hueco_ms o, si no hay, la última lectura."""
    ts = sorted(ts)
    return next((a for a, b in zip(ts, ts[1:]) if b - a > hueco_ms), ts[-1])


def recarga_al_arranque(colector, activas):
    ahora = ahora_ms()
    sesion = colector.sesion or SesionFirebase(SERVICIO)  # en --seco también se lee Firebase
    revisar_ms = int(RECARGA_REVISAR_H * H_MS)
    vistas = lecturas_api_firebase(sesion, ahora - revisar_ms)
    ampliada = False
    resumen = {}
    for fuente, cfg in activas.items():
        tope_ms = int(cfg.get("recarga_max_dias", 0) * DIA_MS)
        if not tope_ms:
            continue
        origen = ORIGEN_DE_FUENTE[fuente]
        ts = [v["timestamp"] for v in vistas.values() if v["origen"] == origen]
        if not ts and tope_ms > revisar_ms:
            if not ampliada:  # una sola lectura ampliada, hasta el tope más largo
                tope_max = int(max(c.get("recarga_max_dias", 0) for c in activas.values()) * DIA_MS)
                vistas.update(lecturas_api_firebase(sesion, ahora - tope_max))
                ampliada = True
            ts = [v["timestamp"] for v in vistas.values() if v["origen"] == origen]
        desde = inicio_recarga(ts, RECARGA_HUECO_MIN * MIN_MS) if ts else ahora - tope_ms
        desde = max(desde - RECARGA_MARGEN_MIN * MIN_MS, ahora - tope_ms)
        ventana_min = math.ceil((ahora - desde) / MIN_MS)
        try:
            leidas = [e for e in LECTORES[fuente](cfg, ventana_min) if e["timestamp"] >= desde]
            nuevas = sorted((e for e in leidas if e["eventId"] not in vistas), key=lambda e: e["timestamp"])
            colector.escribir_lote(nuevas, fuente, edad_max_ms=tope_ms + H_MS)
            resumen[origen] = {"desde": desde, "ventanaMin": ventana_min, "leidas": len(leidas), "nuevas": len(nuevas)}
        except Exception as ex:  # noqa: BLE001 - una API caída no impide arrancar: la recarga es un extra
            colector.contar("errores")
            resumen[origen] = {"error": describir_error(ex)}
    log(SERVICIO, status="recarga_arranque", revisadasH=RECARGA_REVISAR_H, ampliada=ampliada,
        lecturasRevisadas=len(vistas), origenes=resumen)
    return resumen


def main():
    parser = argparse.ArgumentParser(description="Colector de APIs públicas -> Firebase (MonitorAqua)")
    parser.add_argument("--una-vez", action="store_true", help="una pasada por fuente y termina")
    parser.add_argument("--seco", action="store_true", help="no escribe en Firebase")
    parser.add_argument("--fuente", choices=sorted(LECTORES), help="solo esta fuente")
    parser.add_argument("--ventana-min", type=int, help="pide todas las lecturas de los últimos N min (modo nube)")
    parser.add_argument("--solo-recarga", action="store_true", help="solo la recarga de arranque y termina")
    args = parser.parse_args()
    if args.ventana_min and args.ventana_min * 60_000 >= EDAD_MAX_MS:
        parser.error(f"--ventana-min debe ser menor que la edad máxima ({EDAD_MAX_MS // 60_000} min, EDAD_MAX_H o "
                     "VENTANA_H): las lecturas más viejas se descartarían")

    if not args.seco and faltan_variables_firebase():
        log(SERVICIO, status="config_error", faltan=faltan_variables_firebase(), ayuda="completa .env")
        sys.exit(1)
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))["fuentes"]
    activas = {f: c for f, c in config.items() if c.get("habilitada", True) and (not args.fuente or f == args.fuente)}
    log(SERVICIO, status="starting", fuentes={f: c["intervalo_s"] for f, c in activas.items()},
        seco=args.seco, ventanaH=EDAD_MAX_MS / 3_600_000, ventanaMin=args.ventana_min)

    colector = Colector(args.seco, args.ventana_min)
    if args.solo_recarga or (RECARGA_ARRANQUE and not args.una_vez and not args.ventana_min):
        try:
            recarga_al_arranque(colector, activas)
        except Exception as ex:  # noqa: BLE001 - Firebase caído al arrancar: se sigue con el bucle normal
            log(SERVICIO, status="recarga_error", error=describir_error(ex))
        if args.solo_recarga:
            return
    hilos = [threading.Thread(target=colector.bucle, args=(f, c, args.una_vez), name=f, daemon=True)
             for f, c in activas.items()]
    for h in hilos:
        h.start()
    try:
        for h in hilos:
            h.join()
    except KeyboardInterrupt:
        pass
    if args.una_vez and colector.contadores["errores"]:
        sys.exit(1)  # en GitHub Actions la ejecución queda en rojo y avisa por correo


if __name__ == "__main__":
    main()
