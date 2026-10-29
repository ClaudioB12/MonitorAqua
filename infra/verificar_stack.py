"""Verifica en un solo comando el stack de MonitorAqua sobre lambda26.

Uso (desde la raíz del repo):  .venv\\Scripts\\python infra\\verificar_stack.py
Lee .env de la raíz. Sale con código 1 si algún chequeo falla.
"""
import base64
from datetime import datetime
import json
import sys
import time
import urllib.request
from pathlib import Path

from kafka import KafkaConsumer, TopicPartition

RAIZ = Path(__file__).resolve().parents[1]
TOPICOS = {"aqua-eventos-real": 3, "servicio-eventos": 3, "aqua-eventos-aumentados": 3}


def leer_env(ruta: Path) -> dict:
    env = {}
    if ruta.exists():
        for linea in ruta.read_text(encoding="utf-8").splitlines():
            linea = linea.strip()
            if linea and not linea.startswith("#") and "=" in linea:
                clave, valor = linea.split("=", 1)
                env[clave.strip()] = valor.strip()
    return env


ENV = leer_env(RAIZ / ".env")
KAFKA = ENV.get("KAFKA_BOOTSTRAP", "localhost:49092")
INFLUX = ENV.get("INFLUX_URL", "http://localhost:48086")
GRAFANA = "http://localhost:43000"
PROMETHEUS = "http://localhost:49090"
EXPORTER = "http://localhost:49308"


def http(url: str, headers: dict | None = None, timeout: float = 5) -> tuple[int, str]:
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def grafana_auth() -> dict:
    cred = f"{ENV.get('GRAFANA_ADMIN_USER', 'admin')}:{ENV.get('GRAFANA_ADMIN_PASSWORD', 'admin')}"
    return {"Authorization": "Basic " + base64.b64encode(cred.encode()).decode()}


def chequear_kafka():
    consumer = KafkaConsumer(bootstrap_servers=KAFKA, request_timeout_ms=10000)
    try:
        topicos = consumer.topics()
        yield True, f"Kafka responde en {KAFKA}", f"{len(topicos)} tópicos"
        for topico, particiones_esperadas in TOPICOS.items():
            particiones = consumer.partitions_for_topic(topico)
            if not particiones:
                yield False, f"Tópico {topico}", "no existe"
                continue
            tps = [TopicPartition(topico, p) for p in particiones]
            mensajes = sum(consumer.end_offsets(tps).values()) - sum(consumer.beginning_offsets(tps).values())
            ok = len(particiones) == particiones_esperadas
            yield ok, f"Tópico {topico}", f"{len(particiones)} particiones, {mensajes} mensajes"
        yield from chequear_frescura(consumer)
    finally:
        consumer.close()


def chequear_frescura(consumer, topico="aqua-eventos-real", max_s=120):
    """Ingesta viva: el último mensaje del tópico tiene menos de max_s segundos (el ESP32 publica cada 5 s)."""
    tps = [TopicPartition(topico, p) for p in consumer.partitions_for_topic(topico)]
    consumer.assign(tps)
    fines = {tp: fin for tp, fin in consumer.end_offsets(tps).items() if fin > 0}
    for tp, fin in fines.items():
        consumer.seek(tp, fin - 1)  # solo el último mensaje de cada partición
    ultimo, pendientes = 0, set(fines)
    for _ in range(5):
        for tp, msgs in consumer.poll(timeout_ms=2000).items():
            ultimo = max([ultimo] + [m.timestamp for m in msgs])
            pendientes.discard(tp)
        if not pendientes:
            break
    edad = time.time() - ultimo / 1000 if ultimo else None
    yield (edad is not None and edad < max_s, "Ingesta viva (puente)",
           f"último mensaje hace {edad:.0f} s" if edad is not None else "sin mensajes")


def chequear_influx():
    _, cuerpo = http(f"{INFLUX}/health")
    salud = json.loads(cuerpo)
    yield salud.get("status") == "pass", f"InfluxDB {INFLUX}", f"{salud.get('status')} · {salud.get('version')}"
    bucket = ENV.get("INFLUX_BUCKET", "monitoraqua")
    _, cuerpo = http(f"{INFLUX}/api/v2/buckets?name={bucket}",
                     {"Authorization": f"Token {ENV.get('INFLUX_TOKEN', '')}"})
    buckets = json.loads(cuerpo).get("buckets", [])
    if buckets:
        segundos = (buckets[0].get("retentionRules") or [{}])[0].get("everySeconds", 0)
        yield True, f"Bucket {bucket}", f"retención {segundos // 86400} días"
    else:
        yield False, f"Bucket {bucket}", "no existe o el token no tiene acceso"


def chequear_spark(max_s=180):
    """Speed layer viva: cada consulta escribió su progreso (spark_progreso) en InfluxDB hace menos de max_s."""
    flux = ('from(bucket:"%s") |> range(start:-1h) |> filter(fn:(r)=> r._measurement=="spark_progreso" '
            'and r._field=="numInputRows") |> group(columns:["consulta"]) |> last()') % ENV.get("INFLUX_BUCKET", "monitoraqua")
    req = urllib.request.Request(f"{INFLUX}/api/v2/query?org={ENV.get('INFLUX_ORG', 'horeb')}", data=flux.encode(),
                                 method="POST", headers={"Authorization": f"Token {ENV.get('INFLUX_TOKEN', '')}",
                                                         "Content-Type": "application/vnd.flux", "Accept": "application/csv"})
    with urllib.request.urlopen(req, timeout=15) as r:
        filas = [l.split(",") for l in r.read().decode().splitlines() if l and not l.startswith(",result")]
    ultimas = {}
    for f in filas:
        if len(f) > 8 and f[5] and f[-1].strip():  # _time en la columna 5, consulta en la última
            ultimas[f[-1].strip()] = f[5]
    if not ultimas:
        yield False, "Spark streaming (spark_progreso)", "sin progreso en la última hora"
        return
    for consulta in ("rapidas_agg", "lentas_agg", "esp32_detalle", "bronce"):
        t = ultimas.get(consulta)
        edad = time.time() - datetime.fromisoformat(t.replace("Z", "+00:00")).timestamp() if t else None
        yield (edad is not None and edad < max_s, f"Spark consulta {consulta}",
               f"último lote hace {edad:.0f} s" if edad is not None else "sin progreso")


def chequear_grafana():
    _, cuerpo = http(f"{GRAFANA}/api/health")
    salud = json.loads(cuerpo)
    yield salud.get("database") == "ok", f"Grafana {GRAFANA}", f"v{salud.get('version')}"
    for uid in ("prometheus", "influxdb"):
        try:
            _, cuerpo = http(f"{GRAFANA}/api/datasources/uid/{uid}/health", grafana_auth(), timeout=15)
            r = json.loads(cuerpo)
            yield r.get("status") == "OK", f"Datasource {uid}", r.get("message", "")
        except Exception as e:  # noqa: BLE001
            yield False, f"Datasource {uid}", str(e)


def chequear_prometheus():
    _, cuerpo = http(f"{PROMETHEUS}/api/v1/targets")
    targets = json.loads(cuerpo)["data"]["activeTargets"]
    yield True, f"Prometheus {PROMETHEUS}", f"{len(targets)} targets"
    for t in targets:
        job = t["labels"]["job"]
        yield t["health"] == "up", f"Target {job}", t["health"] + (f" ({t['lastError']})" if t.get("lastError") else "")


def chequear_exporter():
    _, cuerpo = http(f"{EXPORTER}/metrics")
    brokers = next((l.split()[-1] for l in cuerpo.splitlines() if l.startswith("kafka_brokers ")), None)
    yield brokers not in (None, "0"), f"kafka-exporter {EXPORTER}", f"kafka_brokers={brokers}"


def main() -> int:
    fallos = 0
    print(f"{'':2} {'componente':<44} detalle")
    print("-" * 90)
    for nombre, chequeo in [("Kafka", chequear_kafka), ("InfluxDB", chequear_influx),
                            ("Spark", chequear_spark), ("Grafana", chequear_grafana), ("Prometheus", chequear_prometheus),
                            ("kafka-exporter", chequear_exporter)]:
        try:
            for ok, componente, detalle in chequeo():
                fallos += not ok
                print(f"{'OK' if ok else 'XX':2} {componente:<44} {detalle}")
        except Exception as e:  # noqa: BLE001 - cualquier error de conexión es un fallo del chequeo
            fallos += 1
            print(f"{'XX':2} {nombre:<44} {type(e).__name__}: {e}")
    print("-" * 90)
    print("Stack completo y operativo" if not fallos else f"{fallos} chequeo(s) fallaron")
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
