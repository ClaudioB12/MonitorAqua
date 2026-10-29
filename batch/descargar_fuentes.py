"""
Descarga de fuentes para la capa batch (F6). Solo lectura sobre las fuentes; escribe en batch/datos/fuentes/f6_*
(no versionado) y un manifiesto con lo descargado.

  ESP32        Firebase /board1/lecturas/historial completo (entradas del firmware, sin "origen"), desde el 2-oct,
               incluido lo anterior a Kafka
  ThingSpeak   los 7 canales de ingesta/estaciones.yml: las últimas 8,000 lecturas de cada uno; el canal 3418755
               completo (70 días de agua y aire en el tanque), paginando hacia atrás
  Open-Meteo   Juliaca horario: archivo (ERA5, archive-api) desde el inicio de ThingSpeak 3418755 y, para los últimos
               días que el archivo todavía no tiene, el pronóstico con past_days

Corre en la imagen de ingesta (credenciales de Firebase en .env):
  docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/batch monitoraqua-ingesta:latest python descargar_fuentes.py
"""
import json
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI.parent / "ingesta"))
FUENTES = AQUI / "datos" / "fuentes"
CANAL_COMPLETO = 3418755
JULIACA = {"latitude": -15.49, "longitude": -70.13}


def esp32():
    from comun import URL_HISTORIAL, SesionFirebase
    r = requests.get(URL_HISTORIAL + ".json", params={"auth": SesionFirebase("f6").token()}, timeout=300)
    r.raise_for_status()
    return {k: v for k, v in (r.json() or {}).items() if isinstance(v, dict) and "origen" not in v}


def thingspeak():
    cfg = yaml.safe_load((AQUI.parent / "ingesta" / "estaciones.yml").read_text(encoding="utf-8"))
    out = {}
    for c in cfg["fuentes"]["thingspeak"]["canales"]:
        url = f"https://api.thingspeak.com/channels/{c['id']}/feeds.json"
        d = requests.get(url, params={"results": 8000}, timeout=120).json()
        feeds, canal = d.get("feeds", []), d.get("channel", {})
        while c["id"] == CANAL_COMPLETO and len(d.get("feeds", [])) == 8000:
            fin = (datetime.fromisoformat(feeds[0]["created_at"].replace("Z", "+00:00")) - timedelta(seconds=1))
            d = requests.get(url, params={"results": 8000, "end": fin.strftime("%Y-%m-%d %H:%M:%S")}, timeout=120).json()
            nuevos = [f for f in d.get("feeds", []) if f["entry_id"] < feeds[0]["entry_id"]]
            if not nuevos:
                break
            feeds = nuevos + feeds
            time.sleep(1)
        out[str(c["id"])] = {"config": c, "canal": canal, "feeds": feeds}
    return out


def open_meteo(desde):
    hasta = date.today()
    archivo = requests.get("https://archive-api.open-meteo.com/v1/archive", timeout=120, params={
        **JULIACA, "start_date": desde.isoformat(), "end_date": hasta.isoformat(),
        "hourly": "temperature_2m,relative_humidity_2m", "timezone": "UTC"}).json()
    reciente = requests.get("https://api.open-meteo.com/v1/forecast", timeout=120, params={
        **JULIACA, "past_days": 14, "forecast_days": 0, "hourly": "temperature_2m,relative_humidity_2m",
        "timezone": "UTC"}).json()
    return {"archivo": archivo, "pronostico_past_days": reciente}


def main():
    FUENTES.mkdir(parents=True, exist_ok=True)
    manifiesto = {"descargado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "fuentes": {}}
    datos = esp32()
    (FUENTES / "f6_esp32_firebase.json").write_text(json.dumps(datos), encoding="utf-8")
    ts = sorted(v["timestamp"] for v in datos.values() if isinstance(v.get("timestamp"), int))
    manifiesto["fuentes"]["esp32_firebase"] = {"lecturas": len(datos), "desde_ms": ts[0], "hasta_ms": ts[-1]}

    tsk = thingspeak()
    (FUENTES / "f6_thingspeak.json").write_text(json.dumps(tsk), encoding="utf-8")
    manifiesto["fuentes"]["thingspeak"] = {cid: {"lecturas": len(c["feeds"]),
                                                 "desde": c["feeds"][0]["created_at"] if c["feeds"] else None,
                                                 "hasta": c["feeds"][-1]["created_at"] if c["feeds"] else None}
                                           for cid, c in tsk.items()}

    inicio = min(datetime.fromisoformat(c["feeds"][0]["created_at"].replace("Z", "+00:00")).date()
                 for c in tsk.values() if c["feeds"])
    om = open_meteo(inicio)
    (FUENTES / "f6_open_meteo_juliaca.json").write_text(json.dumps(om), encoding="utf-8")
    t = om["archivo"].get("hourly", {}).get("temperature_2m", [])
    manifiesto["fuentes"]["open_meteo"] = {"desde": inicio.isoformat(),
                                           "horas_archivo": len(t), "horas_archivo_con_dato": sum(v is not None for v in t),
                                           "horas_pronostico": len(om["pronostico_past_days"].get("hourly", {}).get("time", []))}
    (FUENTES / "f6_manifiesto.json").write_text(json.dumps(manifiesto, indent=1), encoding="utf-8")
    print(json.dumps(manifiesto, indent=1))


if __name__ == "__main__":
    main()
