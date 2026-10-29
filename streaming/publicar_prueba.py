"""Publica eventos de prueba en aqua-eventos-real para verificar watermark y deduplicación (F3).

Los eventos usan sensorId "esp32-prueba-f3" y eventId "prueba-f3-...", para no confundirse nunca con el ESP32
real (esp32-tanque-real-01). Cumplen el contrato, salvo que no pasan por Firebase.

Corre con la imagen de ingesta (tiene kafka-python):
  docker run --rm --network lambda26-kafka-net -v "$PWD/streaming:/s:ro" monitoraqua-ingesta:latest \
    python /s/publicar_prueba.py tardio --horas 2
  ... python /s/publicar_prueba.py duplicado --veces 2
"""
import argparse
import json
import os
import time
import uuid

from kafka import KafkaProducer

SENSOR_PRUEBA = "esp32-prueba-f3"


def evento(event_id, ts_ms):
    return {"tipoEvento": "aqua.tanque.lectura", "eventId": event_id, "sensorId": SENSOR_PRUEBA,
            "origen": "esp32-fisico", "fuenteTipo": "iot-propio", "timestamp": ts_ms,
            "ingestaTs": int(time.time() * 1000), "distanciaCm": 42.0, "temperaturaC": 20.0, "humedadPct": 50.0}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("modo", choices=["tardio", "duplicado"])
    p.add_argument("--horas", type=float, default=2, help="tardio: antigüedad del timestamp")
    p.add_argument("--veces", type=int, default=2, help="duplicado: cuántas veces se publica el mismo eventId")
    args = p.parse_args()

    prod = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), acks="all",
                         key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    ahora = int(time.time() * 1000)
    if args.modo == "tardio":
        ev = evento(f"prueba-f3-tardio-{uuid.uuid4().hex[:8]}", ahora - int(args.horas * 3600 * 1000))
        envios = [ev]
    else:
        ev = evento(f"prueba-f3-duplicado-{uuid.uuid4().hex[:8]}", ahora)
        envios = [ev] * args.veces
    for e in envios:
        meta = prod.send("aqua-eventos-real", key=e["sensorId"], value=e).get(timeout=15)
        print(json.dumps({"publicado": e["eventId"], "timestamp": e["timestamp"],
                          "particion": meta.partition, "offset": meta.offset}))
    prod.flush()


if __name__ == "__main__":
    main()
