"""Solo lectura: ¿todo lo que guarda Kafka está en bronce (y en Firebase)? Consumidor sin grupo, no confirma offsets."""
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd
from kafka import KafkaConsumer, TopicPartition

R = Path(r"C:\upeu\Big Data\MonitorAqua")


def leer(topico):
    c = KafkaConsumer(bootstrap_servers="localhost:49092", enable_auto_commit=False, consumer_timeout_ms=15000,
                      max_poll_records=5000)
    tps = [TopicPartition(topico, p) for p in c.partitions_for_topic(topico)]
    c.assign(tps)
    c.seek_to_beginning(*tps)
    fin = c.end_offsets(tps)
    out = []
    while any(c.position(tp) < fin[tp] for tp in tps):
        for _, msgs in c.poll(timeout_ms=5000).items():
            for m in msgs:
                try:
                    out.append((json.loads(m.value), m.timestamp))
                except Exception:
                    out.append(({"_ilegible": True}, m.timestamp))
    c.close()
    return out


real = leer("aqua-eventos-real")
ids = {e.get("eventId") for e, _ in real}
print("aqua-eventos-real mensajes", len(real), "eventIds únicos", len(ids))
print("rango kafka_ts", pd.to_datetime(min(t for _, t in real), unit="ms"), pd.to_datetime(max(t for _, t in real), unit="ms"))
b = pd.read_parquet(R / "data/local/bronce", columns=["eventId", "kafka_ts", "origen"])
bid = set(b.eventId)
falt = [e for e, t in real if e.get("eventId") not in bid]
print("bronce filas", len(b), "únicos", len(bid), "| en Kafka y NO en bronce:", len(falt))
print(Counter(e.get("origen") for e in falt))
if falt:
    ts = sorted(t for e, t in real if e.get("eventId") not in bid)
    print("kafka_ts de los faltantes", pd.to_datetime(ts[0], unit="ms"), pd.to_datetime(ts[-1], unit="ms"))
json.dump(sorted(i for i in ids if i), open(sys.argv[1], "w"))
serv = leer("servicio-eventos")
print("servicio-eventos", len(serv), Counter(e.get("tipoEvento") for e, _ in serv))
json.dump([e for e, _ in serv], open(sys.argv[2], "w"), ensure_ascii=False)
