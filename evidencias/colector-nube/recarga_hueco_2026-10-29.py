"""Recarga puntual del hueco nocturno de las APIs (2026-10-29): el colector en modo ventana, filtrado a
(01:47:58, 09:15] hora de Lima. Las lecturas hasta 01:47:58 ya estaban publicadas antes del apagado."""
import sys
from datetime import datetime, timedelta, timezone
sys.path.insert(0, "/repo/ingesta")
import yaml
import colector_apis as ca
L = timezone(timedelta(hours=-5))
INI = int(datetime(2026, 10, 29, 1, 47, 58, tzinfo=L).timestamp() * 1000)
FIN = int(datetime(2026, 10, 29, 9, 15, tzinfo=L).timestamp() * 1000)
ventana = (ca.ahora_ms() - INI) // 60000 + 5
cfg = yaml.safe_load(ca.CONFIG_PATH.read_text(encoding="utf-8"))["fuentes"]
col = ca.Colector(seco="--seco" in sys.argv, ventana_min=ventana)
# Solo claves que aún no existen en Firebase: las que ya existen pueden estar publicadas en Kafka y el puente
# las repetiría (su memoria de claves vistas solo cubre 3 h)
import requests
r = requests.get(ca.URL_HISTORIAL + ".json", timeout=300, params={"auth": ca.SesionFirebase("recarga").token(),
                 "orderBy": '"timestamp"', "startAt": INI, "endAt": FIN})
r.raise_for_status()
existentes = set(r.json() or {})
print({"claves_existentes_en_rango": len(existentes)})
total = 0
for fuente, c in cfg.items():
    if not c.get("habilitada", True):
        continue
    evs = sorted((e for e in ca.LECTORES[fuente](c, ventana) if INI < e["timestamp"] <= FIN and e["eventId"] not in existentes), key=lambda e: e["timestamp"])
    col.escribir_lote(evs, fuente)
    total += len(evs)
print({"ventana_min": ventana, "lecturas": total, **col.contadores})
