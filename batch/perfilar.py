"""Perfil estadístico de los datos reales (F2b) -> batch/perfiles/perfil_real.json.

Fuentes (solo lectura):
  - ESP32: Firebase /board1/lecturas/historial, entradas sin campo "origen" (las escribe el firmware).
  - ThingSpeak: historial de los 4 canales de ingesta/estaciones.yml que miden nivelPct (hasta 8000 lecturas c/u).

Uso (en la imagen de ingesta, que tiene las credenciales y requests):
  docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/batch monitoraqua-ingesta:latest \
      python perfilar.py [--sin-descarga]
Las descargas quedan en batch/datos/fuentes/ (ignorado por git) para poder recalcular sin volver a bajar.
"""
import argparse
import json
import math
import statistics as st
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI.parent / "ingesta"))
FUENTES = AQUI / "datos" / "fuentes"
SALIDA = AQUI / "perfiles" / "perfil_real.json"
LIMA = timezone(timedelta(hours=-5))
TANQUE_MAX_CM = 120  # por encima, el sensor del ESP32 no estaba sobre el agua (1-2 oct, fuera del tanque)


def pct(valores, p):
    v = sorted(valores)
    if not v:
        return None
    k = (len(v) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return round(v[lo] + (v[hi] - v[lo]) * (k - lo), 4)


def resumen(valores):
    if not valores:
        return {"n": 0}
    med = st.median(valores)
    return {"n": len(valores), "min": round(min(valores), 4), "max": round(max(valores), 4),
            "media": round(st.fmean(valores), 4), "mediana": round(med, 4),
            "desv_std": round(st.pstdev(valores), 4),
            "mad": round(st.median([abs(x - med) for x in valores]), 4),
            **{f"p{int(p * 100):02d}": pct(valores, p) for p in (0.01, 0.05, 0.25, 0.75, 0.95, 0.99)}}


# --- Descarga ------------------------------------------------------------------------------

def descargar_esp32():
    from comun import URL_HISTORIAL, SesionFirebase
    s = SesionFirebase("perfilar")
    r = requests.get(URL_HISTORIAL + ".json", params={"auth": s.token()}, timeout=300)
    r.raise_for_status()
    datos = {k: v for k, v in (r.json() or {}).items() if isinstance(v, dict) and "origen" not in v}
    (FUENTES / "esp32_historial.json").write_text(json.dumps(datos), encoding="utf-8")
    return datos


def canales_nivel():
    cfg = yaml.safe_load((AQUI.parent / "ingesta" / "estaciones.yml").read_text(encoding="utf-8"))
    return [c for c in cfg["fuentes"]["thingspeak"]["canales"] if "nivelPct" in c["campos"]]


def descargar_thingspeak():
    out = {}
    for c in canales_nivel():
        d = requests.get(f"https://api.thingspeak.com/channels/{c['id']}/feeds.json",
                         params={"results": 8000}, timeout=60).json()
        out[str(c["id"])] = {"nombre": c["nombre"], "campo": c["campos"]["nivelPct"], "feeds": d.get("feeds", [])}
    (FUENTES / "thingspeak_historial.json").write_text(json.dumps(out), encoding="utf-8")
    return out


# --- Perfil ESP32 -------------------------------------------------------------------------

def perfil_esp32(datos):
    filas = sorted(((v.get("timestamp"), k, v) for k, v in datos.items() if isinstance(v.get("timestamp"), int)),
                   key=lambda x: x[0])
    ts = [t for t, _, _ in filas]
    dist = [v.get("distanciaCm") for _, _, v in filas]
    n = len(filas)
    menos1 = sum(1 for d in dist if d == -1)
    fuera = sum(1 for d in dist if d is not None and d != -1 and d > TANQUE_MAX_CM)
    nulos = Counter(m for _, _, v in filas for m in ("distanciaCm", "temperaturaC", "humedadPct")
                    if v.get(m) is None)
    dht_menos1 = Counter(m for _, _, v in filas for m in ("temperaturaC", "humedadPct") if v.get(m) == -1)
    validas = [(t, v) for t, _, v in filas
               if v.get("distanciaCm") not in (None, -1) and 2 <= v["distanciaCm"] <= TANQUE_MAX_CM]

    intervalos = [(b - a) / 1000 for a, b in zip(ts, ts[1:])]
    duplicados_ts = sum(1 for x in intervalos if x == 0)
    huecos = [x for x in intervalos if x > 60]
    # Diferencias entre lecturas válidas consecutivas separadas por <= 7 s (cadencia nominal: 5 s)
    deltas = [b[1]["distanciaCm"] - a[1]["distanciaCm"] for a, b in zip(validas, validas[1:])
              if 0 < b[0] - a[0] <= 7000]
    estables = [d for d in deltas if abs(d) <= 2]  # sin saltos: ruido del sensor
    saltos = [d for d in deltas if abs(d) > 2]

    por_dia = Counter(datetime.fromtimestamp(t / 1000, LIMA).date().isoformat() for t in ts)
    por_hora = defaultdict(lambda: {"t": [], "h": []})
    for t, v in validas:
        h = datetime.fromtimestamp(t / 1000, LIMA).hour
        if v.get("temperaturaC") not in (None, -1):
            por_hora[h]["t"].append(v["temperaturaC"])
        if v.get("humedadPct") not in (None, -1):
            por_hora[h]["h"].append(v["humedadPct"])
    ciclo = {str(h): {"n": len(por_hora[h]["t"]),
                      "temperaturaC_media": round(st.fmean(por_hora[h]["t"]), 2) if por_hora[h]["t"] else None,
                      "humedadPct_media": round(st.fmean(por_hora[h]["h"]), 2) if por_hora[h]["h"] else None}
             for h in range(24)}
    horas_cubiertas = sum(1 for h in range(24) if por_hora[h]["t"])

    # Subidas de nivel = posible llegada de agua. Se trabaja con la mediana de cada bloque de 10 min (filtra
    # los saltos sueltos del sensor) y se exige que la distancia baje > 5 cm y siga baja en el bloque siguiente.
    bloques = defaultdict(list)
    for t, v in validas:
        bloques[t // 600_000].append(v["distanciaCm"])
    med = sorted((b, st.median(v)) for b, v in bloques.items() if len(v) >= 30)
    subidas = sum(1 for (b0, m0), (b1, m1), (b2, m2) in zip(med, med[1:], med[2:])
                  if b1 == b0 + 1 and b2 == b1 + 1 and m0 - m1 > 5 and m0 - m2 > 5)
    return {
        "fuente": "Firebase /board1/lecturas/historial (entradas del firmware, sin 'origen')",
        "sensores": sorted({v.get("sensorId") for _, _, v in filas}),
        "periodo_utc": [datetime.fromtimestamp(ts[0] / 1000, timezone.utc).isoformat(),
                        datetime.fromtimestamp(ts[-1] / 1000, timezone.utc).isoformat()] if ts else None,
        "lecturas_totales": n,
        "lecturas_por_dia_lima": dict(sorted(por_dia.items())),
        "invalidas": {
            "distanciaCm_-1": menos1, "distanciaCm_-1_tasa": round(menos1 / n, 5) if n else None,
            f"distanciaCm_mayor_{TANQUE_MAX_CM}cm_sensor_fuera_del_tanque": fuera,
            "temperatura_o_humedad_-1": dict(dht_menos1),
            "campos_nulos": dict(nulos),
        },
        "distanciaCm_validas": resumen([v["distanciaCm"] for _, v in validas]),
        "intervalo_entre_lecturas_s": {**resumen(intervalos), "timestamps_duplicados": duplicados_ts,
                                       "huecos_mayores_60s": len(huecos),
                                       "hueco_max_s": round(max(huecos), 1) if huecos else 0},
        "delta_distanciaCm_lecturas_consecutivas_le_7s": {
            "todas": resumen(deltas), "ruido_abs_le_2cm": resumen(estables),
            "saltos_abs_gt_2cm": {"n": len(saltos), "tasa": round(len(saltos) / len(deltas), 5) if deltas else None}},
        "ciclo_diario_hora_lima": ciclo,
        "ciclo_diario_horas_cubiertas": horas_cubiertas,
        "subidas_de_nivel_gt_5cm_entre_bloques_10min": subidas,
    }


# --- Perfil térmico de Juliaca (Open-Meteo, últimos 30 días, horario) ---------------------

def descargar_open_meteo():
    d = requests.get("https://api.open-meteo.com/v1/forecast", timeout=60, params={
        "latitude": -15.49, "longitude": -70.13, "hourly": "temperature_2m,relative_humidity_2m",
        "past_days": 30, "forecast_days": 0, "timezone": "America/Lima"}).json()
    (FUENTES / "open_meteo_juliaca_30d.json").write_text(json.dumps(d), encoding="utf-8")
    return d


def perfil_open_meteo(d):
    h = d["hourly"]
    por_hora = defaultdict(lambda: {"t": [], "h": []})
    for t, temp, hum in zip(h["time"], h["temperature_2m"], h["relative_humidity_2m"]):
        if temp is None:
            continue
        hora = int(t[11:13])
        por_hora[hora]["t"].append(temp)
        por_hora[hora]["h"].append(hum)
    minimas = defaultdict(list)
    for t, temp in zip(h["time"], h["temperature_2m"]):
        if temp is not None:
            minimas[t[:10]].append(temp)
    return {
        "fuente": "Open-Meteo forecast API, past_days=30, Juliaca (-15.49, -70.13), hora de Lima",
        "periodo": [h["time"][0], h["time"][-1]],
        "temperaturaC": resumen([x for x in h["temperature_2m"] if x is not None]),
        "minima_diaria_C": resumen([min(v) for v in minimas.values()]),
        "maxima_diaria_C": resumen([max(v) for v in minimas.values()]),
        "ciclo_hora_lima": {str(k): {"temperaturaC_media": round(st.fmean(v["t"]), 2),
                                     "temperaturaC_desv": round(st.pstdev(v["t"]), 2),
                                     "humedadPct_media": round(st.fmean(v["h"]), 2)}
                            for k, v in sorted(por_hora.items())},
    }


# --- Perfil ThingSpeak (tanques reales con nivelPct) ----------------------------------------

def perfil_canal(canal):
    serie = []
    for f in canal["feeds"]:
        try:
            serie.append((datetime.fromisoformat(f["created_at"].replace("Z", "+00:00")), float(f[canal["campo"]])))
        except (TypeError, ValueError, KeyError):
            continue
    serie = [(t, v) for t, v in serie if 0 <= v <= 100]
    if len(serie) < 10:
        return {"nombre": canal["nombre"], "n": len(serie), "nota": "insuficiente"}
    llenados, consumo, i = [], [], 0
    # Llenado: tramo de subida sostenida que acumula > 5 puntos porcentuales
    while i < len(serie) - 1:
        j = i
        while j + 1 < len(serie) and serie[j + 1][1] >= serie[j][1] - 0.3 and \
                (serie[j + 1][0] - serie[j][0]).total_seconds() <= 3600:
            j += 1
        subida = serie[j][1] - serie[i][1]
        if subida > 5 and j > i:
            llenados.append({"inicio": serie[i][0], "fin": serie[j][0], "subida_pp": subida})
            i = j
        else:
            a, b = serie[i], serie[i + 1]
            horas = (b[0] - a[0]).total_seconds() / 3600
            if 0 < horas <= 1 and b[1] < a[1]:
                consumo.append((a[1] - b[1]) / horas)
            i += 1
    dur = [(l["fin"] - l["inicio"]).total_seconds() / 60 for l in llenados]
    entre = [(b["inicio"] - a["fin"]).total_seconds() / 3600 for a, b in zip(llenados, llenados[1:])]
    return {
        "nombre": canal["nombre"], "n": len(serie),
        "periodo_utc": [serie[0][0].isoformat(), serie[-1][0].isoformat()],
        "intervalo_min": resumen([(b[0] - a[0]).total_seconds() / 60 for a, b in zip(serie, serie[1:])]),
        "nivelPct": resumen([v for _, v in serie]),
        "llenados": len(llenados),
        "llenado_duracion_min": resumen(dur),
        "llenado_subida_pp": resumen([l["subida_pp"] for l in llenados]),
        "horas_entre_llenados": resumen(entre),
        "hora_inicio_llenado_utc": dict(sorted(Counter(l["inicio"].hour for l in llenados).items())),
        "consumo_pp_por_hora": resumen(consumo),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sin-descarga", action="store_true", help="usa lo ya descargado en batch/datos/fuentes/")
    args = p.parse_args()
    FUENTES.mkdir(parents=True, exist_ok=True)
    problemas = []
    if args.sin_descarga:
        esp32 = json.loads((FUENTES / "esp32_historial.json").read_text(encoding="utf-8"))
        ts = json.loads((FUENTES / "thingspeak_historial.json").read_text(encoding="utf-8"))
        om = json.loads((FUENTES / "open_meteo_juliaca_30d.json").read_text(encoding="utf-8"))
    else:
        try:
            om = descargar_open_meteo()
        except Exception as ex:  # noqa: BLE001
            om = None
            problemas.append(f"Open-Meteo no disponible: {type(ex).__name__}")
        try:
            esp32 = descargar_esp32()
        except Exception as ex:  # noqa: BLE001
            esp32 = {}
            problemas.append(f"ESP32/Firebase no disponible: {type(ex).__name__}")
        try:
            ts = descargar_thingspeak()
        except Exception as ex:  # noqa: BLE001
            ts = {}
            problemas.append(f"ThingSpeak no disponible: {type(ex).__name__}")

    perfil = {
        "generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "esp32": perfil_esp32(esp32) if esp32 else None,
        "thingspeak_tanques": {cid: perfil_canal(c) for cid, c in ts.items()},
        "juliaca_clima_open_meteo": perfil_open_meteo(om) if om else None,
        "problemas_de_acceso": problemas,
        "limitaciones": [
            "Un solo ESP32 (esp32-tanque-real-01): no hay perfiles por zona ni por tanque propios.",
            "El ESP32 mide distancia, no nivel: la altura útil y la capacidad del tanque no se observan; "
            "en el generador son supuestos configurables.",
            "temperaturaC y humedadPct del ESP32 son del aire (DHT11), no del agua, y solo cubren de 15 a 23 h "
            "(hora de Lima) a 20-25 °C: el ciclo térmico nocturno de Juliaca se toma de Open-Meteo (30 días).",
            "Open-Meteo es un modelo meteorológico (no una estación): sirve de referencia climática, no de medición "
            "dentro del tanque.",
            "Antes del 28-oct el ESP32 midió en tandas (1, 2, 8, 15 y 22-oct); el 1-2 de oct el sensor "
            f"estaba fuera del tanque (distancia > {TANQUE_MAX_CM} cm), excluido de la distribución.",
            "El tanque del ESP32 casi no muestra llenados ni consumo: suministro, llenado y consumo se "
            "estiman con los tanques de ThingSpeak, que son de otros países y climas.",
            "Las horas de ThingSpeak están en UTC y la zona horaria local de cada canal no se conoce.",
            "La franja de llegada de agua en Juliaca no se observa: se toma como supuesto declarado desde "
            "la continuidad reportada por SUNASS para Seda Juliaca.",
        ],
    }
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text(json.dumps(perfil, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"perfil escrito en {SALIDA.relative_to(AQUI.parent)}")


if __name__ == "__main__":
    main()
