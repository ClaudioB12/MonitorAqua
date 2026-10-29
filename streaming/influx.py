"""Escritura mínima a InfluxDB 2 por HTTP (line protocol), sin dependencias fuera de la biblioteca estándar.

Un punto con el mismo measurement, tags y timestamp reemplaza al anterior: por eso reescribir una ventana en
modo update, o reprocesar un micro-lote tras un reinicio, no duplica datos en InfluxDB.

Fallas transitorias (DNS, conexión rechazada, tiempo agotado, HTTP 5xx o 429) se reintentan con espera creciente
(1, 2, 4, 8 y 16 s: ~31 s en total) antes de propagar el error. Así una caída breve, como la falla de DNS del
2026-10-29 que detuvo la consulta eventos_servicio, no tumba el job de Spark. Un 4xx (token o datos mal formados)
no se reintenta: reintentar no lo arregla.
"""
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

INFLUX_URL = os.getenv("INFLUX_URL_INTERNA", "http://influxdb:8086")
INFLUX_ORG = os.getenv("INFLUX_ORG", "horeb")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "monitoraqua")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "")
LOTE = 5000
REINTENTOS = int(os.getenv("INFLUX_REINTENTOS", "5"))
ESPERA_INICIAL_S = 1.0


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


def _transitoria(ex):
    if isinstance(ex, urllib.error.HTTPError):  # subclase de URLError: se mira primero
        return ex.code >= 500 or ex.code == 429
    # URLError envuelve la falla de DNS (socket.gaierror) y la conexión rechazada; OSError cubre los tiempos agotados
    return isinstance(ex, (urllib.error.URLError, OSError))


def _enviar(req):
    espera = ESPERA_INICIAL_S
    for intento in range(REINTENTOS + 1):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                if r.status not in (200, 204):
                    raise RuntimeError(f"InfluxDB respondió {r.status}")
                return
        except Exception as ex:  # noqa: BLE001 - se clasifica abajo; lo no transitorio se propaga de inmediato
            if intento == REINTENTOS or not _transitoria(ex):
                raise
            # El mensaje no lleva el token (va en una cabecera, no en la URL)
            print(json.dumps({"service": "influx", "status": "influx_reintento", "intento": intento + 1,
                              "esperaS": espera, "error": f"{type(ex).__name__}: {ex}"}, ensure_ascii=False),
                  flush=True)
            time.sleep(espera)
            espera *= 2


def escribir(lineas, bucket=None):
    lineas = [l for l in lineas if l]
    url = f"{INFLUX_URL}/api/v2/write?" + urllib.parse.urlencode(
        {"org": INFLUX_ORG, "bucket": bucket or INFLUX_BUCKET, "precision": "ms"})
    for i in range(0, len(lineas), LOTE):
        req = urllib.request.Request(url, data="\n".join(lineas[i:i + LOTE]).encode("utf-8"), method="POST",
                                     headers={"Authorization": f"Token {INFLUX_TOKEN}",
                                              "Content-Type": "text/plain; charset=utf-8"})
        _enviar(req)
    return len(lineas)
