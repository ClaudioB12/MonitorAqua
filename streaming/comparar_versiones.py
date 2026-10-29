"""
Comparación lado a lado de dos versiones del detector de eventos de servicio (p. ej. f4.3 congelada y f4.4).

  Consistencia y sensibilidad  las variantes de streaming/sensibilidad.py (base, f2b2, ruido x2/x3, llenado en el
                               umbral, borde de franja), varias semillas, MISMOS datos para las dos versiones;
                               métricas con IC 95 % de Wilson (streaming/evaluar_detector.py)
  Historial real (DISEÑO)      las lecturas reales de la capa plata (ESP32 desde Firebase y ThingSpeak) por las dos
                               versiones: qué eventos desaparecen y cuáles aparecen. Es el conjunto de diseño con el
                               que se hicieron las reglas: NO se reporta como rendimiento real.

La versión vieja se toma de git (código y reglas en --ref) y la nueva del árbol de trabajo (o de --reglas-nuevas).

  .venv/Scripts/python streaming/comparar_versiones.py --ref main --reglas-nuevas <ruta> --json <salida>
"""
import argparse
import importlib.util
import json
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "streaming"))
sys.path.insert(0, str(RAIZ / "ingesta"))
import sensibilidad  # noqa: E402
from evaluar_detector import metricas, sumar, wilson  # noqa: E402

LIMA = timezone(timedelta(hours=-5))
TANQUES_REALES = ["esp32-tanque-real-01", "thingspeak-1233810", "thingspeak-1233815", "thingspeak-283977",
                  "thingspeak-3418755", "thingspeak-480876"]


def detector_de_git(ref, carpeta):
    """Carga detector_servicio.py y reglas_servicio.yml de un commit de git como módulo independiente."""
    codigo = subprocess.run(["git", "show", f"{ref}:streaming/detector_servicio.py"], cwd=RAIZ, check=True,
                            capture_output=True).stdout
    reglas = subprocess.run(["git", "show", f"{ref}:streaming/reglas_servicio.yml"], cwd=RAIZ, check=True,
                            capture_output=True).stdout
    (carpeta / "detector_viejo.py").write_bytes(codigo)
    (carpeta / "reglas_viejas.yml").write_bytes(reglas)
    spec = importlib.util.spec_from_file_location("detector_viejo", carpeta / "detector_viejo.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.Detector(mod.Configuracion(str(carpeta / "reglas_viejas.yml"), str(RAIZ / "ingesta" / "tanques.yml")))


def detector_nuevo(reglas):
    from detector_servicio import Configuracion, Detector
    return Detector(Configuracion(str(reglas), str(RAIZ / "ingesta" / "tanques.yml")))


def consistencia(detectores, variantes, semillas):
    out = {}
    for variante in variantes:
        out[variante] = {}
        for nombre, det in detectores.items():
            res = [sensibilidad.correr(variante, s, det)[1] for s in semillas]
            # Igual que sensibilidad.py: se suman las parejas anomalía-evento; las llegadas efectivas, aparte
            total = sumar([{k: x for k, x in c.items() if k not in ("llegadas_efectivas", "_omitidas_sin_etiqueta")}
                           for c in res])
            ll = [c["llegadas_efectivas"] for c in res]
            n, k = sum(x["episodios"] for x in ll), sum(x["episodios_detectados"] for x in ll)
            out[variante][nombre] = {**metricas(total), "llegadas_efectivas": {
                "llegadas": n, "detectadas": k, "recall": round(k / n, 3) if n else None, "recall_ic95": wilson(k, n),
                "eventos_llegada_agua": sum(x["eventos"] for x in ll)}}
            print(json.dumps({"variante": variante, "version": nombre, "listo": True}), flush=True)
    return out


def lecturas_plata(sensor_id):
    import pyarrow.dataset as ds
    t = ds.dataset(RAIZ / "batch" / "datos" / "plata", format="parquet", partitioning="hive").to_table(
        columns=["eventId", "sensorId", "timestamp", "distanciaCm", "nivelPct_fuente", "nivelM", "temperaturaC"],
        filter=ds.field("sensorId") == sensor_id).to_pylist()
    for r in t:
        r["nivelPct"] = r.pop("nivelPct_fuente")
    return sorted({r["eventId"]: r for r in t}.values(), key=lambda r: r["timestamp"])


def historial(detectores):
    out = {}
    ahora = int(time.time() * 1000)
    for sid in TANQUES_REALES:
        lecturas = lecturas_plata(sid)
        if not lecturas:
            continue
        out[sid] = {"lecturas": len(lecturas)}
        for nombre, det in detectores.items():
            evs, estado = [], None
            for i in range(0, len(lecturas), 5000):
                nuevos, estado = det.procesar(sid, lecturas[i:i + 5000], json.loads(json.dumps(estado)) if estado else None,
                                              "real", ahora)
                evs += nuevos
            out[sid][nombre] = [{"tipo": e["tipo"], "inicioLima": datetime.fromtimestamp(e["inicioTs"] / 1000, LIMA)
                                 .strftime("%m-%d %H:%M"), "valor": e["valor"], "criterio": (e.get("detalle") or {}).get("criterio"),
                                 "referencia": (e.get("detalle") or {}).get("referencia")} for e in evs]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default="main", help="commit de la versión vieja (código y reglas)")
    ap.add_argument("--reglas-nuevas", default=str(RAIZ / "streaming" / "reglas_servicio.yml"))
    ap.add_argument("--semillas", type=int, nargs="+", default=[2026, 7, 11])
    ap.add_argument("--variantes", nargs="+", default=list(sensibilidad.VARIANTES))
    ap.add_argument("--solo-historial", action="store_true")
    ap.add_argument("--json")
    a = ap.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        viejo = detector_de_git(a.ref, Path(tmp))
        nuevo = detector_nuevo(a.reglas_nuevas)
        detectores = {str(viejo.cfg.reglas["version"]): viejo, str(nuevo.cfg.reglas["version"]): nuevo}
        res = {"versiones": list(detectores), "ref_vieja": a.ref, "semillas": a.semillas,
               "historial_diseno": historial(detectores)}
        if not a.solo_historial:
            res["consistencia"] = consistencia(detectores, a.variantes, a.semillas)
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json).write_text(json.dumps(res, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print(json.dumps({"listo": True, "versiones": list(detectores)}))


if __name__ == "__main__":
    main()
