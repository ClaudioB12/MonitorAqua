"""
Tablas en Markdown de la salida de streaming/comparar_versiones.py (consistencia, sensibilidad e historial de diseño).

  .venv/Scripts/python streaming/tablas_comparativa.py docs/evidencias/f4/f4.4/comparativa_f43_f44.json \
      > docs/evidencias/f4/f4.4/tablas_f44.md
"""
import json
import sys
from collections import Counter

r = json.load(open(sys.argv[1], encoding="utf-8"))
v43, v44 = r["versiones"]
out = []
w = out.append
fmt = lambda p, ic: "—" if p is None else f"{p:.2f} [{ic[0]:.2f}-{ic[1]:.2f}]"

w("# Tablas f4.4 frente a f4.3\n")
w(f"Semillas: {', '.join(map(str, r['semillas']))}. Versión vieja tomada de git: `{r['ref_vieja']}`.\n")
w("## 1. Consistencia y sensibilidad (datos aumentados, conteos sumados de las semillas)\n")
w("| Variante | Anomalía → evento | Episodios | f4.3 precisión | f4.3 recall | f4.4 precisión | f4.4 recall |")
w("|---|---|---|---|---|---|---|")
for var, x in r["consistencia"].items():
    m3 = {f["anomalia"]: f for f in x[v43]["metricas"]}
    m4 = {f["anomalia"]: f for f in x[v44]["metricas"]}
    for an in m3:
        a, b = m3[an], m4[an]
        if an == "deriva_termica":
            w(f"| {var} | deriva_termica → alerta_fuga falsa | {a.get('episodios')} | — | {a.get('episodios_con_alerta_falsa', a.get('consistencia_recall'))} | — | {b.get('episodios_con_alerta_falsa', b.get('consistencia_recall'))} |")
            continue
        w(f"| {var} | {an} → {a.get('evento', '')} | {a.get('episodios')} | "
          f"{fmt(a.get('consistencia_precision'), a.get('consistencia_precision_ic95'))} | {fmt(a.get('consistencia_recall'), a.get('consistencia_recall_ic95'))} | "
          f"{fmt(b.get('consistencia_precision'), b.get('consistencia_precision_ic95'))} | {fmt(b.get('consistencia_recall'), b.get('consistencia_recall_ic95'))} |")
    l3, l4 = x[v43]["llegadas_efectivas"], x[v44]["llegadas_efectivas"]
    w(f"| {var} | todas las llegadas → llegada_agua | {l3['llegadas']} | — | {fmt(l3['recall'], l3['recall_ic95'])} | — | {fmt(l4['recall'], l4['recall_ic95'])} |")
    d3, d4 = x[v43]["deriva_termica"], x[v44]["deriva_termica"]
    w(f"| {var} | deriva_termica → alerta_fuga FALSA | {d3['episodios']} | — | {d3['con_alerta_fuga_falsa']} con alerta falsa | — | {d4['con_alerta_fuga_falsa']} con alerta falsa |")

w("\n## 2. Historial real (conjunto de DISEÑO, no rendimiento)\n")
w("| Tanque | Lecturas | Eventos f4.3 | Eventos f4.4 |")
w("|---|---|---|---|")
for sid, x in r["historial_diseno"].items():
    c = lambda evs: ", ".join(f"{k} {n}" for k, n in sorted(Counter(e["tipo"] for e in evs).items())) or "—"
    w(f"| {sid} | {x['lecturas']:,} | {c(x[v43])} | {c(x[v44])} |")

w("\n### ESP32: eventos que desaparecen o aparecen\n")
x = r["historial_diseno"].get("esp32-tanque-real-01", {})
clave = lambda e: (e["tipo"], e["inicioLima"])
s3, s4 = {clave(e): e for e in x.get(v43, [])}, {clave(e): e for e in x.get(v44, [])}
w("| Evento | Inicio (Lima) | f4.3 | f4.4 | Criterio / referencia |")
w("|---|---|---|---|---|")
for k in sorted(set(s3) | set(s4), key=lambda k: k[1]):
    if k[0] == "falla_sensor" and k in s3 and k in s4:
        continue
    e = s3.get(k) or s4.get(k)
    w(f"| {k[0]} | {k[1]} | {'sí' if k in s3 else '—'} | {'sí' if k in s4 else '—'} | {e.get('criterio') or e.get('referencia') or ''} |")
n_ff = sum(1 for k in s3 if k[0] == "falla_sensor" and k in s4)
w(f"\n(`falla_sensor` iguales en las dos versiones: {n_ff}.)")
print("\n".join(out))
