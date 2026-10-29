"""Consistencia detector-generador: eventos detectados sobre datos aumentados vs. anomalías etiquetadas (F4).

  python streaming/evaluar_detector.py <eventos.jsonl> [--episodios <_episodios.json>] [--tolerancia-min 30]

Qué mide: el generador (ingesta/generador.py) y el detector (streaming/detector_servicio.py) comparten
definiciones (el -1 de falla, la franja horaria de llegada, la escala de los llenados). Estas cifras miden si el
detector reconoce lo que el generador inyectó con esas mismas definiciones: CONSISTENCIA DE IMPLEMENTACIÓN, no
rendimiento real. El rendimiento real se mide contra la bitácora de pruebas con el ESP32
(streaming/comparar_bitacora.py).

Emparejamiento: mismo sensor e intervalo [inicioTs, finTs] superpuesto al del episodio (± tolerancia).
  fuga_lenta       -> alerta_fuga
  corte_prolongado -> corte_prolongado
  llegada_atipica  -> llegada_agua con detalle.fueraDeFranja = true
  falla_sensor     -> falla_sensor (solo los episodios; los -1 sueltos no forman una racha de N lecturas)
  deriva_termica   -> cuántos episodios provocaron una alerta_fuga (falso positivo)

consistencia_precision = eventos que coinciden con un episodio / eventos emitidos de ese tipo
consistencia_recall    = episodios con al menos un evento / episodios
Cada proporción va con su intervalo de confianza de 95 % de Wilson (con pocos episodios es muy ancho).
"""
import argparse
import json
import math
from collections import Counter
from pathlib import Path

PARES = [("fuga_lenta", "alerta_fuga", None),
         ("corte_prolongado", "corte_prolongado", None),
         ("llegada_atipica", "llegada_agua", "fueraDeFranja"),
         ("falla_sensor", "falla_sensor", None)]


def wilson(k, n, z=1.96):
    """Intervalo de confianza de 95 % de Wilson para k/n; None si n = 0."""
    if n == 0:
        return None
    p = k / n
    den = 1 + z * z / n
    centro = (p + z * z / (2 * n)) / den
    margen = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return [round(max(0.0, centro - margen), 3), round(min(1.0, centro + margen), 3)]


def leer_eventos(ruta):
    texto = Path(ruta).read_text(encoding="utf-8").strip()
    if texto.startswith("["):
        return json.loads(texto)
    return [json.loads(l) for l in texto.splitlines() if l.strip()]


def solapa(ev, ep, tol_ms):
    return ev["sensorId"] == ep["sensorId"] and ev["inicioTs"] <= ep["fin"] + tol_ms and ev["finTs"] >= ep["inicio"] - tol_ms


def conteos(eventos, episodios, tol_min=30):
    """Conteos crudos (sumables entre corridas) por tipo de anomalía."""
    tol = tol_min * 60000
    eventos = list({e["eventoId"]: e for e in eventos}.values())  # un reproceso puede reemitir el mismo eventoId
    out = {}
    for tipo_ep, tipo_ev, filtro in PARES:
        evs = [e for e in eventos if e["tipo"] == tipo_ev and (not filtro or (e.get("detalle") or {}).get(filtro))]
        eps = [p for p in episodios if p["tipo"] == tipo_ep]
        out[tipo_ep] = {"evento": tipo_ev + (f" ({filtro})" if filtro else ""), "episodios": len(eps), "eventos": len(evs),
                        "eventos_correctos": sum(1 for e in evs if any(solapa(e, p, tol) for p in eps)),
                        "episodios_detectados": sum(1 for p in eps if any(solapa(e, p, tol) for e in evs))}
    fugas = [e for e in eventos if e["tipo"] == "alerta_fuga"]
    derivas = [p for p in episodios if p["tipo"] == "deriva_termica"]
    out["deriva_termica"] = {"episodios": len(derivas),
                             "con_alerta_fuga_falsa": sum(1 for p in derivas if any(solapa(e, p, tol) for e in fugas))}
    out["_eventos_por_tipo"] = dict(Counter(e["tipo"] for e in eventos))
    return out


def metricas(c):
    """Proporciones e IC 95 % de Wilson a partir de conteos (de una corrida o sumados)."""
    filas = []
    for tipo_ep, _, _ in PARES:
        x = c[tipo_ep]
        filas.append({"anomalia": tipo_ep, "evento": x["evento"], "episodios": x["episodios"], "eventos": x["eventos"],
                      "consistencia_precision": round(x["eventos_correctos"] / x["eventos"], 3) if x["eventos"] else None,
                      "consistencia_precision_ic95": wilson(x["eventos_correctos"], x["eventos"]),
                      "consistencia_recall": round(x["episodios_detectados"] / x["episodios"], 3) if x["episodios"] else None,
                      "consistencia_recall_ic95": wilson(x["episodios_detectados"], x["episodios"])})
    d = c["deriva_termica"]
    return {"que_mide": "consistencia detector-generador (definiciones compartidas), no rendimiento real",
            "metricas": filas,
            "deriva_termica": {**d, "ic95": wilson(d["con_alerta_fuga_falsa"], d["episodios"])},
            "eventos_por_tipo": c.get("_eventos_por_tipo")}


def sumar(lista):
    total = {}
    for c in lista:
        for k, v in c.items():
            if k == "_eventos_por_tipo":
                continue
            t = total.setdefault(k, {kk: (vv if isinstance(vv, str) else 0) for kk, vv in v.items()})
            for kk, vv in v.items():
                if not isinstance(vv, str):
                    t[kk] += vv
    return total


def fmt(p, ic):
    return "—" if p is None else f"{p:.2f} [{ic[0]:.2f}-{ic[1]:.2f}]"


def imprimir(res, titulo="Consistencia detector-generador"):
    print(f"{titulo} (no es rendimiento real)")
    print(f"{'anomalía':<18} {'evento':<30} {'epis.':>5} {'event.':>6}  {'precisión [IC95]':<19} {'recall [IC95]':<19}")
    for f in res["metricas"]:
        print(f"{f['anomalia']:<18} {f['evento']:<30} {f['episodios']:>5} {f['eventos']:>6}  "
              f"{fmt(f['consistencia_precision'], f['consistencia_precision_ic95']):<19} "
              f"{fmt(f['consistencia_recall'], f['consistencia_recall_ic95']):<19}")
    d = res["deriva_termica"]
    ic = d["ic95"] or [0, 0]
    print(f"deriva_termica: {d['con_alerta_fuga_falsa']} de {d['episodios']} episodios con alerta_fuga falsa "
          f"[IC95 {ic[0]:.2f}-{ic[1]:.2f}]")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("eventos")
    ap.add_argument("--episodios", default="batch/datos/aumentados/runId=r2026-45facae2/_episodios.json")
    ap.add_argument("--tolerancia-min", type=int, default=30)
    ap.add_argument("--json", help="guarda el resultado en este archivo")
    a = ap.parse_args()
    res = metricas(conteos(leer_eventos(a.eventos), json.loads(Path(a.episodios).read_text(encoding="utf-8")),
                           a.tolerancia_min))
    imprimir(res)
    print("eventos por tipo:", res["eventos_por_tipo"])
    if a.json:
        Path(a.json).write_text(json.dumps(res, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
