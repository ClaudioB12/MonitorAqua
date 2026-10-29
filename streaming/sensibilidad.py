"""
Análisis de sensibilidad del detector de eventos de servicio (F4).

Rompe a propósito los supuestos que el generador y el detector comparten y mide cómo cambia la consistencia:
  base             ruido del perfil real (x1)
  ruido_x2/x3      σ de las lecturas y frecuencia de saltos > 2 cm multiplicados por 2 y 3
  llenado_umbral   llenado a 5 L/min: en 15 min sube ~6.8 % un tanque de 1100 L (umbral 6 %), 12.5 % uno de 600 L
                   y solo 3 % uno de 2500 L
  borde_franja     llegadas atípicas entre las 8:00 y las 9:30: justo después de franja + tolerancia en la zona A
                   (4-7 h, +1 h) y DENTRO de la tolerancia en la zona B (5-8 h, +1 h)

Cada variante corre con varias semillas (los conteos se suman) y el MISMO detector (streaming/detector_servicio.py,
la clase que corre en Spark). Además de las cuatro parejas anomalía-evento, mide el recall de TODAS las llegadas
efectivas (normales y atípicas), usando la agenda de suministro del generador.

  .venv/Scripts/python streaming/sensibilidad.py [--semillas 2026 7 11] [--json docs/evidencias/f4/sensibilidad.json]
"""
import argparse
import json
import sys
import time
from argparse import Namespace
from pathlib import Path

import numpy as np

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "ingesta"))
sys.path.insert(0, str(RAIZ / "streaming"))
import generador  # noqa: E402
from detector_servicio import Configuracion, Detector  # noqa: E402
from evaluar_detector import conteos, metricas, sumar, wilson  # noqa: E402

VARIANTES = {
    "base": {},
    "ruido_x2": {"factor_ruido": 2.0},
    "ruido_x3": {"factor_ruido": 3.0},
    "llenado_umbral": {"caudal_llenado": 5.0},
    "borde_franja": {"franja_atipica": (8.0, 9.5)},
    # Modelo f2b.2 del generador: fugas persistentes (1-4 días) y toda llegada omitida etiquetada como corte
    "f2b2": {"config": str(RAIZ / "ingesta" / "generador_f2b2.yml")},
    # Modelo f2b.3: f2b.2 con la compensación térmica en el firmware, como el ESP32 real (DHT11 ±2 °C, redondeado).
    # Para el detector, los tanques de la flota se declaran con compensacion_en_firmware (solo en memoria)
    "f2b3": {"config": str(RAIZ / "ingesta" / "generador_f2b3.yml"), "firmware_compensa": True},
}
FIRMWARE_DESDE = {"desde": "2000-01-01T00:00:00-05:00"}


def llegadas_efectivas(fila, tiempos):
    """Intervalos de suministro en los que el tanque realmente subió (>= 3 cm): las llegadas que se podían ver."""
    s = fila["suministro"].astype(np.int8)
    bordes = np.flatnonzero(np.diff(np.concatenate([[0], s, [0]])))
    out = []
    for a, b in zip(bordes[::2], bordes[1::2]):
        if fila["dist_real"][a] - fila["dist_real"][b - 1] >= 3:
            out.append((int(tiempos[a]), int(tiempos[b - 1])))
    return out


def correr(variante, semilla, detector):
    args = Namespace(config=str(RAIZ / "ingesta" / "generador.yml"), tanques=None, dias=None, intervalo_s=None,
                     semilla=semilla, anomalias=None, factor_ruido=None, caudal_llenado=None, franja_atipica=None,
                     **{k: None for k in ()})
    for k, v in VARIANTES[variante].items():
        if k != "firmware_compensa":
            setattr(args, k, v)  # "config" cambia el YAML del generador (modelos f2b.2 y f2b.3)
    cfg, perfil, run_id = generador.cargar_config(args)
    tiempos, filas, episodios = generador.generar(cfg, perfil, run_id)
    tanques_archivo = detector.cfg.refrescar().tanques
    if VARIANTES[variante].get("firmware_compensa"):
        detector.cfg.tanques = {sid: ({**g, "compensacion_en_firmware": FIRMWARE_DESDE} if sid.startswith("flota-") else g)
                                for sid, g in tanques_archivo.items()}
    try:
        return _detectar(detector, cfg, run_id, tiempos, filas, episodios)
    finally:
        detector.cfg.tanques = tanques_archivo


def _detectar(detector, cfg, run_id, tiempos, filas, episodios):
    eventos, llegadas, llegadas_vistas = [], 0, 0
    ahora = int(time.time() * 1000)
    for f in filas:
        sid = cfg["tanques"][f["t"]]["sensorId"]
        lecturas = [{"timestamp": int(ts), "distanciaCm": float(d), "temperaturaC": float(t)}
                    for ts, d, t in zip(tiempos, f["dist"], f["temp"])]
        estado, evs = None, []
        for i in range(0, len(lecturas), 5000):  # en lotes, como Spark, con el estado serializado entre lotes
            nuevos, estado = detector.procesar(sid, lecturas[i:i + 5000], json.loads(json.dumps(estado)) if estado else None,
                                               "aumentado", ahora)
            evs += nuevos
        eventos += evs
        lleg = [e for e in evs if e["tipo"] == "llegada_agua"]
        for a, b in llegadas_efectivas(f, tiempos):
            llegadas += 1
            llegadas_vistas += any(e["inicioTs"] <= b + 1_800_000 and e["finTs"] >= a - 1_800_000 for e in lleg)
    c = conteos(eventos, episodios)
    c["_omitidas_sin_etiqueta"] = filas[0].get("omitidas_sin_etiqueta", 0)
    c["llegadas_efectivas"] = {"evento": "llegada_agua", "episodios": llegadas, "episodios_detectados": llegadas_vistas,
                               "eventos": sum(1 for e in eventos if e["tipo"] == "llegada_agua")}
    return run_id, c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--semillas", type=int, nargs="+", default=[2026, 7, 11])
    ap.add_argument("--variantes", nargs="+", default=[v for v in VARIANTES if v not in ("f2b2", "f2b3")])
    ap.add_argument("--json")
    a = ap.parse_args()
    detector = Detector(Configuracion(str(RAIZ / "streaming" / "reglas_servicio.yml"), str(RAIZ / "ingesta" / "tanques.yml")))
    resultados = {}
    for v in a.variantes:
        por_semilla, runs = [], []
        for s in a.semillas:
            t0 = time.time()
            run_id, c = correr(v, s, detector)
            por_semilla.append(c)
            runs.append(run_id)
            print(f"{v:<15} semilla {s:<5} {run_id}  {time.time() - t0:5.0f} s", flush=True)
        total = sumar([{k: x for k, x in c.items() if k not in ("llegadas_efectivas", "_omitidas_sin_etiqueta")}
                       for c in por_semilla])
        res = metricas(total)
        ll = [c["llegadas_efectivas"] for c in por_semilla]
        n, k = sum(x["episodios"] for x in ll), sum(x["episodios_detectados"] for x in ll)
        res["llegadas_efectivas"] = {"llegadas": n, "detectadas": k, "recall": round(k / n, 3) if n else None,
                                     "recall_ic95": wilson(k, n), "eventos_llegada_agua": sum(x["eventos"] for x in ll)}
        res["semillas"], res["runIds"], res["variante"] = a.semillas, runs, VARIANTES[v]
        res["llegadas_omitidas_sin_etiqueta"] = sum(c["_omitidas_sin_etiqueta"] for c in por_semilla)
        resultados[v] = res

    print("\nConsistencia detector-generador por variante (conteos sumados de las semillas; IC 95 % de Wilson)")
    cab = f"{'variante':<15} {'anomalía':<17} {'epis.':>5} {'event.':>6}  {'precisión [IC95]':<19} {'recall [IC95]':<19}"
    print(cab)
    fmt = lambda p, ic: "—" if p is None else f"{p:.2f} [{ic[0]:.2f}-{ic[1]:.2f}]"  # noqa: E731
    for v, res in resultados.items():
        for f in res["metricas"]:
            print(f"{v:<15} {f['anomalia']:<17} {f['episodios']:>5} {f['eventos']:>6}  "
                  f"{fmt(f['consistencia_precision'], f['consistencia_precision_ic95']):<19} "
                  f"{fmt(f['consistencia_recall'], f['consistencia_recall_ic95']):<19}")
        ll, d = res["llegadas_efectivas"], res["deriva_termica"]
        print(f"{v:<15} {'llegadas (todas)':<17} {ll['llegadas']:>5} {ll['eventos_llegada_agua']:>6}  {'':<19} "
              f"{fmt(ll['recall'], ll['recall_ic95']):<19}")
        print(f"{v:<15} {'deriva -> fuga':<17} {d['episodios']:>5} {d['con_alerta_fuga_falsa']:>6}  (episodios con alerta falsa)")
    if a.json:
        Path(a.json).write_text(json.dumps(resultados, ensure_ascii=False, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
