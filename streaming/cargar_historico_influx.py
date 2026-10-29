"""
Carga histórica para los tableros de F5 (idempotente: un punto con la misma serie y tiempo se reemplaza).

  real       bronce (lecturas del ESP32, deduplicadas por eventId) -> nivel_tanque y estado_tanque (replay del detector
             congelado); servicio-eventos -> eventos_servicio con los numéricos de detalle aplanados
  aumentado  Parquet de la corrida de referencia -> nivel_tanque, estado_tanque y lecturas_agg (para los KPIs con
             origenLecturas = aumentado); eventos de la validación f4.3 -> eventos_servicio (origenLecturas aumentado)

Lo que llega en vivo lo escribe Spark con las mismas funciones (streaming/tablero.py).

  .venv/Scripts/python streaming/cargar_historico_influx.py [--solo real|aumentado]
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "streaming"))
for linea in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
    if "=" in linea and not linea.lstrip().startswith("#"):
        k, v = linea.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())
os.environ.setdefault("INFLUX_URL_INTERNA", "http://localhost:48086")

import pyarrow.dataset as ds  # noqa: E402

import influx  # noqa: E402
import tablero  # noqa: E402
from detector_servicio import Configuracion, Detector  # noqa: E402

RUN_REFERENCIA = "r2026-45facae2"
# Los datos aumentados van en su propio bucket, sin vencimiento: son una referencia fija con fechas simuladas
# (28-sep a 27-oct) que la retención de 30 días del bucket en vivo rechazaría.
BUCKET_AUMENTADOS = os.getenv("INFLUX_BUCKET_AUMENTADOS", "monitoraqua_aumentados")


def asegurar_bucket(nombre):
    import urllib.request
    url, org, tok = os.environ["INFLUX_URL_INTERNA"], os.getenv("INFLUX_ORG", "horeb"), os.environ["INFLUX_TOKEN"]
    h = {"Authorization": f"Token {tok}", "Content-Type": "application/json"}
    existe = json.load(urllib.request.urlopen(urllib.request.Request(f"{url}/api/v2/buckets?name={nombre}", headers=h)))
    if existe.get("buckets"):
        return "existente"
    org_id = json.load(urllib.request.urlopen(urllib.request.Request(f"{url}/api/v2/orgs?org={org}", headers=h)))["orgs"][0]["id"]
    cuerpo = json.dumps({"orgID": org_id, "name": nombre, "retentionRules": [],
                         "description": "Datos aumentados (flota extrapolada) para los tableros de F5; sin vencimiento"})
    urllib.request.urlopen(urllib.request.Request(f"{url}/api/v2/buckets", data=cuerpo.encode(), headers=h, method="POST"))
    return "creado"
FOTO_CADA_MS = 10 * 60000  # una foto del estado cada 10 min de tiempo de evento


def detector():
    return Detector(Configuracion(str(RAIZ / "streaming" / "reglas_servicio.yml"), str(RAIZ / "ingesta" / "tanques.yml")))


def replay(det, sensor_id, lecturas, origen):
    """Corre el detector congelado y devuelve líneas nivel_tanque y estado_tanque (fotos cada 10 min)."""
    lineas, estado, ultima_foto = [], None, 0
    for i in range(0, len(lecturas), 2000):
        lote = lecturas[i:i + 2000]
        for lec in lote:
            lineas.append(tablero.linea_nivel(det, lec, sensor_id, origen))
        # por sub-lotes de 10 min, para tener fotos del estado con esa resolución
        j = 0
        while j < len(lote):
            k = j
            while k < len(lote) and lote[k]["timestamp"] - lote[j]["timestamp"] < FOTO_CADA_MS:
                k += 1
            _, estado = det.procesar(sensor_id, lote[j:k], estado, origen, int(time.time() * 1000))
            if estado.get("ult_ts", 0) - ultima_foto >= FOTO_CADA_MS:
                lineas.append(tablero.linea_estado(sensor_id, estado, origen))
                ultima_foto = estado["ult_ts"]
            j = k
    return [l for l in lineas if l]


def eventos_kafka():
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:49092"), enable_auto_commit=False,
                      consumer_timeout_ms=5000)
    tps = [TopicPartition("servicio-eventos", p) for p in c.partitions_for_topic("servicio-eventos")]
    c.assign(tps)
    c.seek_to_beginning()
    evs = [json.loads(m.value) for m in c]
    c.close()
    return list({e["eventoId"]: e for e in evs}.values())


def cargar_real(det):
    t = ds.dataset(RAIZ / "data" / "local" / "bronce", format="parquet", partitioning="hive").to_table(
        columns=["eventId", "sensorId", "timestamp", "distanciaCm", "temperaturaC", "lectura_valida"],
        filter=ds.field("sensorId") == "esp32-tanque-real-01").to_pylist()
    lecturas = sorted({r["eventId"]: r for r in t}.values(), key=lambda r: r["timestamp"])
    lineas = replay(det, "esp32-tanque-real-01", lecturas, "real")
    evs = eventos_kafka()
    lineas += [tablero.linea_evento(e) for e in evs if e.get("origenLecturas") == "real"]
    return lineas, {"lecturas_esp32": len(lecturas), "eventos": len(evs)}


def cargar_aumentado(det):
    t = ds.dataset(RAIZ / "batch" / "datos" / "aumentados" / f"runId={RUN_REFERENCIA}", format="parquet",
                   partitioning="hive").to_table(columns=["sensorId", "timestamp", "distanciaCm", "temperaturaC"]).to_pylist()
    por = {}
    for r in t:
        por.setdefault(r["sensorId"], []).append(r)
    lineas = []
    for sid, ls in por.items():
        ls.sort(key=lambda r: r["timestamp"])
        lineas += replay(det, sid, ls, "aumentado")
        # KPIs (lecturas por minuto, % válidas): una ventana de 1 min por lectura, como lecturas_agg de Spark
        for r in ls:
            valida = r["distanciaCm"] is not None and r["distanciaCm"] != -1
            lineas.append(influx.linea("lecturas_agg", {"sensorId": sid, "origen": "flota-extrapolada",
                                                        "fuenteTipo": "aumentado", "ventana": "1m"},
                                       {"n_total": 1, "n_validas": int(valida), "n_invalidas": int(not valida),
                                        "es_dato_real": False}, r["timestamp"]))
    evs = [json.loads(l) for l in open(RAIZ / "docs" / "evidencias" / "f4" / "eventos_con_compensacion.jsonl",
                                        encoding="utf-8") if l.strip()]
    lineas += [tablero.linea_evento(e) for e in evs]
    return lineas, {"lecturas": len(t), "eventos": len(evs)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--solo", choices=["real", "aumentado"])
    a = ap.parse_args()
    det = detector()
    for nombre, f in (("real", cargar_real), ("aumentado", cargar_aumentado)):
        if a.solo and a.solo != nombre:
            continue
        t0 = time.time()
        bucket = BUCKET_AUMENTADOS if nombre == "aumentado" else None
        if bucket:
            info_bucket = asegurar_bucket(bucket)
        lineas, info = f(det)
        n = influx.escribir(lineas, bucket=bucket)
        if bucket:
            info["bucket"] = f"{bucket} ({info_bucket})"
        print(json.dumps({"carga": nombre, "puntos": n, **info, "segundos": round(time.time() - t0, 1)}), flush=True)


if __name__ == "__main__":
    main()
