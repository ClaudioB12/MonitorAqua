"""Auditoría de la ingesta F2: Kafka (aqua-eventos-real) contra Firebase (historial).

Lee el tópico desde los offsets dados (por defecto, todo) y reporta:
  - mensajes por origen,
  - eventId repetidos (en general y del ESP32),
  - lecturas del ESP32 en Firebase dentro del rango auditado que NO llegaron a Kafka.

Corre en el contenedor de ingesta (tiene las credenciales y la red):
  docker run --rm --network lambda26-kafka-net --env-file .env -e KAFKA_BOOTSTRAP=kafka:9092 \
    monitoraqua-ingesta:latest python auditar_ingesta.py --desde 0:2649,1:0,2:0
"""
import argparse
import collections
import json
import os

import requests
from kafka import KafkaConsumer, TopicPartition

from comun import URL_HISTORIAL, SesionFirebase

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aqua-eventos-real")


def leer_kafka(desde):
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False,
                             consumer_timeout_ms=5000)
    tps = [TopicPartition(KAFKA_TOPIC, p) for p in consumer.partitions_for_topic(KAFKA_TOPIC)]
    consumer.assign(tps)
    fin = consumer.end_offsets(tps)
    for tp in tps:
        consumer.seek(tp, desde.get(tp.partition, 0))
    eventos = []
    pendientes = {tp for tp in tps if consumer.position(tp) < fin[tp]}
    while pendientes:
        for tp, msgs in consumer.poll(timeout_ms=3000).items():
            for m in msgs:
                if m.offset < fin[tp]:
                    eventos.append((tp.partition, m.offset, m.key.decode() if m.key else None, json.loads(m.value)))
            if consumer.position(tp) >= fin[tp]:
                pendientes.discard(tp)
    consumer.close()
    return eventos


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--desde", default="", help="offsets iniciales 'particion:offset,...'")
    args = parser.parse_args()
    desde = {int(p): int(o) for p, o in (x.split(":") for x in args.desde.split(",") if x)}

    eventos = leer_kafka(desde)
    print(f"Mensajes auditados en {KAFKA_TOPIC}: {len(eventos)} (desde {desde or 'el inicio'})")

    por_origen = collections.Counter(e["origen"] for *_, e in eventos)
    sensores = collections.defaultdict(set)
    for *_, e in eventos:
        sensores[e["origen"]].add(e["sensorId"])
    print("\nPor origen:")
    for origen, n in por_origen.most_common():
        print(f"  {origen:<14} {n:>6} mensajes  {len(sensores[origen]):>3} sensores")

    claves_ok = sum(1 for _, _, k, e in eventos if k == e["sensorId"])
    print(f"\nClave Kafka = sensorId: {claves_ok}/{len(eventos)}")
    print("Particiones:", dict(collections.Counter(p for p, *_ in eventos)))

    ids = collections.Counter(e["eventId"] for *_, e in eventos)
    repetidos = {i: n for i, n in ids.items() if n > 1}
    esp32 = [e for *_, e in eventos if e["origen"] == "esp32-fisico"]
    rep_esp32 = {e["eventId"] for e in esp32 if ids[e["eventId"]] > 1}
    print(f"\neventId repetidos: {len(repetidos)} (de ellos ESP32: {len(rep_esp32)})")
    for i, n in list(repetidos.items())[:10]:
        print(f"  {i} x{n}")

    if esp32:
        t0, t1 = min(e["timestamp"] for e in esp32), max(e["timestamp"] for e in esp32)
        sesion = SesionFirebase("auditoria")
        r = requests.get(URL_HISTORIAL + ".json", timeout=60, params={
            "auth": sesion.token(), "orderBy": '"timestamp"', "startAt": t0, "endAt": t1})
        r.raise_for_status()
        fb = {k for k, v in (r.json() or {}).items() if isinstance(v, dict) and "origen" not in v}
        en_kafka = {e["eventId"] for e in esp32}
        print(f"\nESP32 en Firebase entre el primer y último timestamp auditado: {len(fb)}")
        print(f"ESP32 en Kafka (eventId únicos): {len(en_kafka)}")
        print(f"En Firebase y no en Kafka: {len(fb - en_kafka)}   En Kafka y no en Firebase: {len(en_kafka - fb)}")
        fallos = sum(1 for e in esp32 if e.get("distanciaCm") == -1)
        print(f"ESP32 con distanciaCm = -1 (pasan tal cual): {fallos}")


if __name__ == "__main__":
    main()
