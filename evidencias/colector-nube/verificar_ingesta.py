"""Cuenta, por origen, las entradas de Firebase con ingestaTs de los últimos N minutos (solo imprime conteos)."""
import sys
import time
from collections import Counter, defaultdict

import requests

sys.path.insert(0, "/repo/ingesta")
from comun import URL_HISTORIAL, SesionFirebase  # noqa: E402

minutos = int(sys.argv[1]) if len(sys.argv) > 1 else 5
ahora = int(time.time() * 1000)
# Índice solo en "timestamp": se piden las lecturas de las últimas 3 h (la ventana del colector es 2 h)
r = requests.get(URL_HISTORIAL + ".json", timeout=120, params={
    "auth": SesionFirebase("verificacion").token(), "orderBy": '"timestamp"', "startAt": ahora - 3 * 3_600_000})
r.raise_for_status()
datos = r.json() or {}
recientes, sin_ingesta, total = Counter(), Counter(), Counter()
rango = defaultdict(lambda: [None, None])
for v in datos.values():
    o = v.get("origen", "esp32-fisico (firmware)")
    total[o] += 1
    ing = v.get("ingestaTs")
    if ing is None:
        sin_ingesta[o] += 1
    elif ing >= ahora - minutos * 60_000:
        recientes[o] += 1
        lo, hi = rango[o]
        rango[o] = [min(lo or ing, ing), max(hi or ing, ing)]
print(f"ventana ingestaTs: últimos {minutos} min | entradas con timestamp en las últimas 3 h: {sum(total.values())}")
print(f"{'origen':28} {'ingestaTs<5min':>14} {'sin ingestaTs':>14} {'total 3 h':>10}  ingestaTs (s atrás)")
for o in sorted(total):
    lo, hi = rango[o]
    txt = f"{(ahora - hi) // 1000}-{(ahora - lo) // 1000}" if lo else "-"
    print(f"{o:28} {recientes[o]:>14} {sin_ingesta[o]:>14} {total[o]:>10}  {txt}")
print(f"{'TOTAL':28} {sum(recientes.values()):>14}")
