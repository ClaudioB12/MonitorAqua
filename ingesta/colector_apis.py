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
"""
import argparse
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

import requests
import yaml

from comun import (ORIGENES, URL_HISTORIAL, SesionFirebase, ahora_ms, cargar_validador, clave_firebase,
                   describir_error, errores_contrato, faltan_variables_firebase, log)

SERVICIO = "aqua-colector"
CONFIG_PATH = Path(os.getenv("ESTACIONES_PATH", Path(__file__).with_name("estaciones.yml")))
# Lecturas más viejas que esto no se escriben: el puente solo mira VENTANA_H hacia atrás
EDAD_MAX_MS = int(float(os.getenv("VENTANA_H", "3")) * 3600 * 1000)
USGS_API_KEY = os.getenv("USGS_API_KEY", "")
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

def leer_thingspeak(cfg):
    eventos = []
    for canal in cfg["canales"]:
        try:
            params = {"results": 1}
            if canal.get("read_key_env") and os.getenv(canal["read_key_env"]):
                params["api_key"] = os.getenv(canal["read_key_env"])
            d = HTTP.get(f"https://api.thingspeak.com/channels/{canal['id']}/feeds.json",
                         params=params, timeout=20).json()
            feeds = d.get("feeds") or []
            if not feeds:
                continue
            ultima = feeds[-1]
            ev = evento_base("thingspeak", str(canal["id"]), iso_a_ms(ultima["created_at"]),
                             a_float(d["channel"].get("latitude")) or canal.get("lat"),
                             a_float(d["channel"].get("longitude")) or canal.get("lon"))
            for medida, campo in canal["campos"].items():
                valor = a_float(ultima.get(campo))
                escala = canal.get("escalas", {}).get(medida)
                ev[medida] = round(valor * escala, 4) if valor is not None and escala else valor
            eventos.append(ev)
        except Exception as ex:  # noqa: BLE001 - un canal caído no detiene a los demás
            log(SERVICIO, status="fuente_error", fuente="thingspeak", estacion=canal["id"], error=describir_error(ex))
    return eventos


def leer_ea_uk(cfg):
    base = "https://environment.data.gov.uk/flood-monitoring"
    eventos = []
    for est in cfg["estaciones"]:
        try:
            items = HTTP.get(f"{base}/id/stations/{est['ref']}/readings", params={"latest": ""},
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
            eventos += [ev for ev in por_ts.values() if "nivelM" in ev or "caudalM3s" in ev]
        except Exception as ex:  # noqa: BLE001
            log(SERVICIO, status="fuente_error", fuente="ea-uk", estacion=est["ref"], error=describir_error(ex))
    return eventos


USGS_PARAMETROS = {"00010": "tempAguaC", "00400": "ph", "00300": "oxigenoMgL"}


def leer_usgs(cfg):
    sitios = {e["id"]: e for e in cfg["estaciones"]}
    params = {"f": "json", "limit": 1000, "monitoring_location_id": ",".join(sitios),
              "parameter_code": ",".join(USGS_PARAMETROS), "statistic_id": "00011"}
    headers = {"X-Api-Key": USGS_API_KEY} if USGS_API_KEY else {}
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


def leer_open_meteo(cfg):
    eventos = []
    for ub in cfg["ubicaciones"]:
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
    def __init__(self, seco):
        self.seco = seco
        self.validador = cargar_validador()
        self.sesion = None if seco else SesionFirebase(SERVICIO)
        self.ultimo_ts = {}  # sensorId -> último timestamp escrito (evita reescrituras inútiles)
        self.lock = threading.Lock()
        self.contadores = {"escritas": 0, "rechazadas": 0, "sin_cambio": 0, "antiguas": 0}

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

    def pasada(self, fuente, cfg):
        inicio = time.time()
        eventos = LECTORES[fuente](cfg)
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
                log(SERVICIO, status="fuente_error", fuente=fuente, error=describir_error(ex))
            if una_vez:
                return
            time.sleep(intervalo)


def main():
    parser = argparse.ArgumentParser(description="Colector de APIs públicas -> Firebase (MonitorAqua)")
    parser.add_argument("--una-vez", action="store_true", help="una pasada por fuente y termina")
    parser.add_argument("--seco", action="store_true", help="no escribe en Firebase")
    parser.add_argument("--fuente", choices=sorted(LECTORES), help="solo esta fuente")
    args = parser.parse_args()

    if not args.seco and faltan_variables_firebase():
        log(SERVICIO, status="config_error", faltan=faltan_variables_firebase(), ayuda="completa .env")
        sys.exit(1)
    config = yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8"))["fuentes"]
    activas = {f: c for f, c in config.items() if c.get("habilitada", True) and (not args.fuente or f == args.fuente)}
    log(SERVICIO, status="starting", fuentes={f: c["intervalo_s"] for f, c in activas.items()},
        seco=args.seco, ventanaH=EDAD_MAX_MS / 3_600_000)

    colector = Colector(args.seco)
    hilos = [threading.Thread(target=colector.bucle, args=(f, c, args.una_vez), name=f, daemon=True)
             for f, c in activas.items()]
    for h in hilos:
        h.start()
    try:
        for h in hilos:
            h.join()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
