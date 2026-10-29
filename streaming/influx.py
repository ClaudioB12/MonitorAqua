"""Escritura mínima a InfluxDB 2 por HTTP (line protocol), sin dependencias fuera de la biblioteca estándar.

Un punto con el mismo measurement, tags y timestamp reemplaza al anterior: por eso reescribir una ventana en
modo update, o reprocesar un micro-lote tras un reinicio, no duplica datos en InfluxDB.
"""
import os
import urllib.parse
import urllib.request

INFLUX_URL = os.getenv("INFLUX_URL_INTERNA", "http://influxdb:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "horeb")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "monitoraqua")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
LOTE = 5000


def _tag(valor):
    return str(valor).replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=")


def _campo(valor):
    if isinstance(valor, bool):
        return "true" if valor else "false"
    if isinstance(valor, int):
        return f"{valor}i"
    if isinstance(valor, float):
        return repr(valor)
    return '"' + str(valor).replace("\\", "\\\\").replace('"', '\\"') + '"'


def linea(measurement, tags, campos, ts_ms):
    """Una línea de line protocol; descarta tags y campos nulos. None si no queda ningún campo."""
    t = ",".join(f"{_tag(k)}={_tag(v)}" for k, v in sorted(tags.items()) if v not in (None, ""))
    c = ",".join(f"{_tag(k)}={_campo(v)}" for k, v in campos.items() if v is not None)
    if not c:
        return None
    return f"{_tag(measurement)}{',' + t if t else ''} {c} {int(ts_ms)}"


def escribir(lineas, bucket=None):
    lineas = [l for l in lineas if l]
    url = f"{INFLUX_URL}/api/v2/write?" + urllib.parse.urlencode(
        {"org": INFLUX_ORG, "bucket": bucket or INFLUX_BUCKET, "precision": "ms"})
    for i in range(0, len(lineas), LOTE):
        req = urllib.request.Request(url, data="\n".join(lineas[i:i + LOTE]).encode("utf-8"), method="POST",
                                     headers={"Authorization": f"Token {INFLUX_TOKEN}",
                                              "Content-Type": "text/plain; charset=utf-8"})
        with urllib.request.urlopen(req, timeout=30) as r:
            if r.status not in (200, 204):
                raise RuntimeError(f"InfluxDB respondió {r.status}")
    return len(lineas)
