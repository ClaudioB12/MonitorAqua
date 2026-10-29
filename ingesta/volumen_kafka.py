"""
Volumen de Kafka por tópico (F5, para los costos de F8): cada INTERVALO_S escribe en InfluxDB
(measurement kafka_volumen, tag topico) los bytes en disco (describe_log_dirs) y los offsets (mensajes publicados y
retenidos). kafka-exporter no expone bytes; Prometheus ya tiene los offsets, pero aquí quedan junto a los bytes.

  python volumen_kafka.py            en bucle (servicio aqua-volumen)
  python volumen_kafka.py --una-vez  una medición
"""
import argparse
import os
import time
import urllib.parse
import urllib.request

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

from comun import log

SERVICIO = "aqua-volumen"
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INTERVALO_S = int(os.getenv("VOLUMEN_INTERVALO_S", "300"))
INFLUX_URL = os.getenv("INFLUX_URL_INTERNA", "http://influxdb:8086")


def medir():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    bytes_por_topico = {}
    for broker in admin.describe_log_dirs():
        for d in broker["log_dirs"]:
            for t in d["topics"]:
                bytes_por_topico[t["name"]] = bytes_por_topico.get(t["name"], 0) + sum(
                    p["partition_size"] for p in t["partitions"])
    admin.close()
    consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP)
    filas = []
    for topico, b in sorted(bytes_por_topico.items()):
        if topico.startswith("__"):
            continue
        tps = [TopicPartition(topico, p) for p in (consumer.partitions_for_topic(topico) or [])]
        fin, ini = consumer.end_offsets(tps), consumer.beginning_offsets(tps)
        filas.append((topico, b, sum(fin.values()), sum(fin.values()) - sum(ini.values()), len(tps)))
    consumer.close()
    return filas


def escribir(filas):
    ahora = int(time.time() * 1000)
    texto = "\n".join(f"kafka_volumen,topico={t} bytes={b}i,mensajes_publicados={pub}i,mensajes_retenidos={ret}i,"
                      f"particiones={n}i {ahora}" for t, b, pub, ret, n in filas)
    url = f"{INFLUX_URL}/api/v2/write?" + urllib.parse.urlencode(
        {"org": os.getenv("INFLUX_ORG", "horeb"), "bucket": os.getenv("INFLUX_BUCKET", "monitoraqua"), "precision": "ms"})
    req = urllib.request.Request(url, data=texto.encode(), method="POST",
                                 headers={"Authorization": f"Token {os.getenv('INFLUX_TOKEN', '')}"})
    urllib.request.urlopen(req, timeout=30).close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--una-vez", action="store_true")
    a = ap.parse_args()
    while True:
        try:
            filas = medir()
            escribir(filas)
            log(SERVICIO, status="medido", topicos={t: {"bytes": b, "retenidos": r} for t, b, _, r, _ in filas})
        except Exception as ex:  # noqa: BLE001 - una medición fallida no detiene el servicio
            log(SERVICIO, status="error", error=f"{type(ex).__name__}: {ex}")
        if a.una_vez:
            return
        time.sleep(INTERVALO_S)


if __name__ == "__main__":
    main()
