"""
Auditoría de huecos en Firebase (/board1/lecturas/historial) por origen y sensor, entre dos instantes. Solo lectura.

Un hueco es un intervalo entre dos lecturas seguidas de un sensor mayor que su umbral:
  esp32-fisico   2 min (el firmware escribe cada ~5 s)
  resto          3 veces el intervalo mediano del propio sensor, mínimo 30 min (EA UK y Open-Meteo cada 15 min,
                 USGS cada 5-15 min, ThingSpeak según el canal)
También informa el hueco al inicio (desde --desde hasta la primera lectura) y al final (hasta --hasta).

Uso (imagen de ingesta, credenciales en .env; horas en Lima):
  docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/ingesta monitoraqua-ingesta:latest \
      python auditar_huecos.py --desde 2026-10-29T01:40 [--hasta 2026-10-29T09:00]
"""
import argparse
import statistics
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import requests

from comun import URL_HISTORIAL, SesionFirebase

LIMA = timezone(timedelta(hours=-5))
MIN = 60_000


def ms(texto):
    return int(datetime.fromisoformat(texto).replace(tzinfo=LIMA).timestamp() * 1000)


def hora(t):
    return datetime.fromtimestamp(t / 1000, LIMA).strftime("%m-%d %H:%M")


def umbral(origen, ts):
    if origen == "esp32-fisico":
        return 2 * MIN
    difs = [b - a for a, b in zip(ts, ts[1:])]
    return max(30 * MIN, 3 * statistics.median(difs)) if difs else 30 * MIN


def main():
    ap = argparse.ArgumentParser(description="Huecos en Firebase por origen y sensor (solo lectura)")
    ap.add_argument("--desde", required=True, help="hora de Lima, p. ej. 2026-10-29T01:40")
    ap.add_argument("--hasta", help="hora de Lima (por defecto, ahora)")
    a = ap.parse_args()
    desde, hasta = ms(a.desde), ms(a.hasta) if a.hasta else int(time.time() * 1000)
    r = requests.get(URL_HISTORIAL + ".json", timeout=300, params={
        "auth": SesionFirebase("auditoria-huecos").token(), "orderBy": '"timestamp"', "startAt": desde, "endAt": hasta})
    r.raise_for_status()
    series = defaultdict(list)
    for v in (r.json() or {}).values():
        if isinstance(v, dict) and isinstance(v.get("timestamp"), int):
            series[(v.get("origen", "esp32-fisico"), v.get("sensorId", "?"))].append(v["timestamp"])

    print(f"Firebase {hora(desde)} -> {hora(hasta)} (Lima) | {sum(map(len, series.values()))} lecturas\n")
    print(f"{'origen':14} {'sensor':24} {'lecturas':>8} {'umbral':>7} {'huecos':>6} {'mayor':>8}  detalle (mayores 3)")
    resumen = defaultdict(lambda: [0, 0, 0])
    for (origen, sid), ts in sorted(series.items()):
        ts = sorted(set(ts))
        u = umbral(origen, ts)
        tramos = [(desde, ts[0])] + list(zip(ts, ts[1:])) + [(ts[-1], hasta)]
        huecos = sorted(((b - a, a, b) for a, b in tramos if b - a > u), reverse=True)
        mayor = huecos[0][0] / MIN if huecos else 0
        detalle = ", ".join(f"{hora(x)}→{hora(y)[-5:]} ({d / MIN:.0f} min)" for d, x, y in huecos[:3])
        print(f"{origen:14} {sid:24} {len(ts):>8} {u / MIN:>5.0f}m {len(huecos):>6} {mayor:>6.0f}m  {detalle}")
        resumen[origen][0] += 1
        resumen[origen][1] += len(ts)
        resumen[origen][2] += bool(huecos)
    print(f"\n{'origen':14} {'sensores':>8} {'lecturas':>9} {'con huecos':>10}")
    for origen, (n, lect, con) in sorted(resumen.items()):
        print(f"{origen:14} {n:>8} {lect:>9} {con:>10}")
    print("\nOrígenes esperados: esp32-fisico, thingspeak, ea-uk, usgs, open-meteo. Si falta uno, no escribió nada.")


if __name__ == "__main__":
    main()
