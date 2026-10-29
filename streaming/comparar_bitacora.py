"""
Validación con datos reales (F4): compara la bitácora de pruebas con el ESP32 contra los eventos detectados.

A diferencia de evaluar_detector.py (consistencia detector-generador), aquí la referencia es lo que una persona hizo
u observó en el tanque real, así que estas cifras sí miden rendimiento real, con la limitación de pocos casos.

Bitácora (docs/evidencias/f4/bitacora_real.csv), una fila por acción:
  timestamp       ISO 8601 con zona horaria (p. ej. 2026-10-29T10:15:00-05:00): cuándo empezó la acción
  accion          llenado | vaciado | falla
  sensorId        por defecto esp32-tanque-real-01
  timestamp_fin   opcional: cuándo terminó (vaciado completo, sensor reparado)
  observador      quién la registró ("sistema" = registrada desde los datos, no es independiente)
  nota            texto libre

Correspondencia: llenado -> llegada_agua, vaciado -> autonomia_baja, falla -> falla_sensor.
accion = no_verificable: nadie puede decir qué pasó; la fila y los eventos a ±antes de ella quedan fuera del cálculo.

Periodo de rendimiento real, por sensor: desde la más tardía entre el congelamiento de reglas
(streaming/reglas_servicio.yml -> congelamiento.desde) y el cambio a geometría real (ingesta/tanques.yml ->
geometria_desde). Mientras la geometría sea provisional (por_medir), el sensor no tiene periodo de rendimiento real.
Lo que queda fuera es el CONJUNTO DE DISEÑO de las reglas y se muestra aparte.
Un evento corresponde a una acción si es del mismo sensor y tipo y su inicioTs cae entre la acción - antes y
fin (o la acción) + despues. Los eventos de esos tipos dentro del periodo de la bitácora que no corresponden a
ninguna acción se listan como "sin registro" (posibles falsos positivos, o acciones que faltó anotar).

  .venv/Scripts/python streaming/comparar_bitacora.py [--eventos <jsonl>] [--antes-min 5] [--despues-min 20]
Sin --eventos lee servicio-eventos desde el principio (solo origenLecturas = real).
"""
import argparse
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from evaluar_detector import wilson  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
CORRESPONDE = {"llenado": "llegada_agua", "vaciado": "autonomia_baja", "falla": "falla_sensor"}
FALLA_ACTIVA_MAX_MIN = 120  # una falla anotada tarde se empareja con un episodio iniciado hasta 2 h antes
LIMA = timezone(timedelta(hours=-5))


def ms(texto):
    return int(datetime.fromisoformat(texto).timestamp() * 1000)


def hora(t):
    return datetime.fromtimestamp(t / 1000, LIMA).strftime("%d-%m %H:%M:%S")


def leer_bitacora(ruta):
    filas = []
    with open(ruta, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            if not r.get("timestamp", "").strip():
                continue
            accion = r["accion"].strip().lower()
            if accion not in CORRESPONDE and accion != "no_verificable":
                raise SystemExit(f"acción desconocida en la bitácora: {accion!r} "
                                 "(usa llenado, vaciado, falla o no_verificable)")
            filas.append({"ts": ms(r["timestamp"].strip()), "accion": accion,
                          "sensorId": (r.get("sensorId") or "esp32-tanque-real-01").strip(),
                          "fin": ms(r["timestamp_fin"].strip()) if (r.get("timestamp_fin") or "").strip() else None,
                          "observador": (r.get("observador") or "").strip(), "nota": (r.get("nota") or "").strip()})
    return sorted(filas, key=lambda x: x["ts"])


def leer_eventos_kafka():
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:49092"), enable_auto_commit=False,
                      consumer_timeout_ms=5000)
    tps = [TopicPartition("servicio-eventos", p) for p in c.partitions_for_topic("servicio-eventos")]
    c.assign(tps)
    c.seek_to_beginning()
    eventos = [json.loads(m.value) for m in c]
    c.close()
    return eventos


def reprocesar_bronce(sensores):
    """Corre el detector ACTUAL (reglas vigentes) sobre las lecturas reales guardadas en bronce.

    Sirve para comparar una versión nueva de las reglas con la bitácora sin esperar nuevas pruebas: mismas lecturas
    reales, misma clase Detector que corre en Spark. Deduplica por eventId como Spark (bronce guarda las copias).
    """
    import time
    import pyarrow.dataset as ds
    from detector_servicio import Configuracion, Detector
    det = Detector(Configuracion(str(RAIZ / "streaming" / "reglas_servicio.yml"), str(RAIZ / "ingesta" / "tanques.yml")))
    t = ds.dataset(RAIZ / "data" / "local" / "bronce", format="parquet", partitioning="hive").to_table(
        columns=["eventId", "sensorId", "timestamp", "distanciaCm", "nivelPct", "nivelM", "temperaturaC", "lectura_valida"],
        filter=ds.field("sensorId").isin(list(sensores))).to_pylist()
    eventos = []
    for sid in sensores:
        lecturas = sorted({r["eventId"]: r for r in t if r["sensorId"] == sid}.values(), key=lambda r: r["timestamp"])
        evs, _ = det.procesar(sid, lecturas, None, "real", int(time.time() * 1000))
        eventos += evs
    return eventos, det.cfg.reglas["version"]


def congelamiento():
    """(version, epoch ms) del congelamiento de reglas en streaming/reglas_servicio.yml; (None, None) si no hay."""
    import yaml
    c = (yaml.safe_load(open(RAIZ / "streaming" / "reglas_servicio.yml", encoding="utf-8")) or {}).get("congelamiento")
    return (c["version"], ms(c["desde"])) if c else (None, None)


def inicio_real(sensor_id, desde_congelamiento):
    """Desde cuándo cuenta el rendimiento real de un sensor: la más tardía entre el congelamiento de reglas y el
    cambio a geometría real (ingesta/tanques.yml -> geometria_desde). Sin geometría real (por_medir o sin fecha), nunca.
    """
    import yaml
    t = ((yaml.safe_load(open(RAIZ / "ingesta" / "tanques.yml", encoding="utf-8")) or {}).get("tanques", {})
         .get(sensor_id, {}))
    if "sensor_al_fondo_cm" in t and (t.get("por_medir") or not t.get("geometria_desde")):
        return None  # geometría provisional: los niveles (llegada, autonomía) no son todavía medidas reales
    geo = ms(str(t["geometria_desde"])) if t.get("geometria_desde") else None
    return max(x for x in (desde_congelamiento, geo) if x is not None)


def comparar(bitacora, eventos, antes_min=5, despues_min=20):
    antes, despues = antes_min * 60000, despues_min * 60000
    eventos = [e for e in {e["eventoId"]: e for e in eventos}.values() if e.get("origenLecturas") == "real"]
    # "no_verificable": nadie sabe qué pasó; esas filas no cuentan y los eventos a ±antes de ellas tampoco
    dudosos = [b for b in bitacora if b["accion"] == "no_verificable"]
    bitacora = [b for b in bitacora if b["accion"] != "no_verificable"]
    excluidos = {e["eventoId"] for e in eventos for b in dudosos
                 if e["sensorId"] == b["sensorId"] and abs(e["inicioTs"] - b["ts"]) <= antes}
    eventos = [e for e in eventos if e["eventoId"] not in excluidos]
    usados, casos = set(), []
    for b in bitacora:
        tipo = CORRESPONDE[b["accion"]]
        limite = (b["fin"] or b["ts"]) + despues
        cand = sorted((e for e in eventos if e["tipo"] == tipo and e["sensorId"] == b["sensorId"]
                       and b["ts"] - antes <= e["inicioTs"] <= limite), key=lambda e: e["inicioTs"])
        if not cand and b["accion"] == "falla":
            # Con histéresis (f4.2), una falla intermitente es UN episodio que puede haber empezado antes de que
            # alguien lo anotara: cuenta el último falla_sensor iniciado hasta FALLA_ACTIVA_MAX_MIN antes.
            previos = [e for e in eventos if e["tipo"] == tipo and e["sensorId"] == b["sensorId"]
                       and b["ts"] - FALLA_ACTIVA_MAX_MIN * 60000 <= e["inicioTs"] < b["ts"] - antes]
            cand = sorted(previos, key=lambda e: e["inicioTs"])[-1:]
        ev = cand[0] if cand else None
        if ev:
            usados.add(ev["eventoId"])
        casos.append({**b, "evento": ev["eventoId"] if ev else None,
                      "retraso_deteccion_s": round((ev["detectadoTs"] - b["ts"]) / 1000) if ev else None,
                      "inicio_evento_menos_accion_s": round((ev["inicioTs"] - b["ts"]) / 1000) if ev else None})
    if bitacora:
        desde = bitacora[0]["ts"] - antes
        hasta = max((b["fin"] or b["ts"]) for b in bitacora) + despues
    else:
        desde = hasta = 0
    sensores = {b["sensorId"] for b in bitacora}
    tipos = set(CORRESPONDE.values())
    en_periodo = [e for e in eventos if e["tipo"] in tipos and e["sensorId"] in sensores and desde <= e["inicioTs"] <= hasta]
    sin_registro = [e for e in en_periodo if e["eventoId"] not in usados]
    resumen = {}
    for accion, tipo in CORRESPONDE.items():
        n = sum(1 for c in casos if c["accion"] == accion)
        k = sum(1 for c in casos if c["accion"] == accion and c["evento"])
        m = sum(1 for e in en_periodo if e["tipo"] == tipo)
        mk = sum(1 for e in en_periodo if e["tipo"] == tipo and e["eventoId"] in usados)
        resumen[accion] = {"evento": tipo, "acciones": n, "detectadas": k, "recall": round(k / n, 3) if n else None,
                           "recall_ic95": wilson(k, n), "eventos_en_periodo": m, "eventos_con_registro": mk,
                           "precision": round(mk / m, 3) if m else None, "precision_ic95": wilson(mk, m)}
    independientes = sum(1 for c in casos if c["observador"] != "sistema")
    return {"que_mide": "rendimiento real contra la bitácora de pruebas con el ESP32",
            "no_verificables": len(dudosos), "eventos_excluidos_por_no_verificable": sorted(excluidos),
            "acciones": len(casos), "acciones_independientes": independientes,
            "periodo": [hora(desde), hora(hasta)] if bitacora else None,
            "resumen": resumen, "casos": casos,
            "sin_registro": [{"eventoId": e["eventoId"], "tipo": e["tipo"], "inicio": hora(e["inicioTs"])} for e in sin_registro]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bitacora", default=str(RAIZ / "docs" / "evidencias" / "f4" / "bitacora_real.csv"))
    ap.add_argument("--eventos", help="JSONL de eventos; si no, lee servicio-eventos")
    ap.add_argument("--reproceso", action="store_true",
                    help="en vez de servicio-eventos, corre las reglas vigentes sobre las lecturas reales de bronce")
    ap.add_argument("--antes-min", type=int, default=5)
    ap.add_argument("--despues-min", type=int, default=20)
    ap.add_argument("--json")
    a = ap.parse_args()
    bitacora = leer_bitacora(a.bitacora)
    if a.reproceso:
        eventos, version = reprocesar_bronce({b["sensorId"] for b in bitacora})
        fuente = f"reproceso de bronce con las reglas vigentes ({version})"
    elif a.eventos:
        eventos, fuente = [json.loads(l) for l in open(a.eventos, encoding="utf-8") if l.strip()], a.eventos
    else:
        eventos, fuente = leer_eventos_kafka(), "servicio-eventos (lo publicado en vivo)"
    version, desde = congelamiento()
    print(f"Eventos: {fuente}\nReglas congeladas: {version} desde {hora(desde) if desde else '—'} (hora de Lima)")
    salida = {"fuente_eventos": fuente, "congelamiento": {"version": version, "desde_ms": desde}}
    if desde:
        # Inicio del rendimiento real por sensor: max(congelamiento, geometria_desde); None = todavía no empieza
        sensores = {b["sensorId"] for b in bitacora} | {e["sensorId"] for e in eventos}
        inicio = {sid: inicio_real(sid, desde) for sid in sensores}
        for sid in sorted({b["sensorId"] for b in bitacora}):
            print(f"Rendimiento real de {sid}: " + (f"desde {hora(inicio[sid])}" if inicio[sid] else
                  "todavía no empieza (geometría provisional en tanques.yml)"))
        real = lambda sid, t: inicio[sid] is not None and t >= inicio[sid]  # noqa: E731
        salida["inicio_rendimiento_real_ms"] = inicio
        # Conjunto de diseño y periodos sin geometría real: se muestran, pero NO son rendimiento real
        diseno = comparar([b for b in bitacora if not real(b["sensorId"], b["ts"])],
                          [e for e in eventos if not real(e["sensorId"], e["inicioTs"])], a.antes_min, a.despues_min)
        print(f"\n=== CONJUNTO DE DISEÑO (antes del congelamiento o sin geometría real): {diseno['acciones']} "
              f"acciones, {diseno['no_verificables']} no verificables. NO es rendimiento real")
        mostrar(diseno)
        salida["diseno"] = diseno
        bitacora = [b for b in bitacora if real(b["sensorId"], b["ts"])]
        eventos = [e for e in eventos if real(e["sensorId"], e["inicioTs"])]
    res = comparar(bitacora, eventos, a.antes_min, a.despues_min)
    print(f"\n=== RENDIMIENTO REAL (después del congelamiento y con geometría real): {res['acciones']} acciones, "
          f"{res['acciones_independientes']} observadas por una persona, {res['no_verificables']} no verificables")
    if res["acciones"]:
        mostrar(res)
    else:
        print("  Todavía no hay acciones en el periodo de rendimiento real: no hay cifra de rendimiento real.")
    salida["evaluacion"] = res
    if a.json:
        Path(a.json).write_text(json.dumps(salida, ensure_ascii=False, indent=1), encoding="utf-8")


def mostrar(res):
    for c in res["casos"]:
        estado = (f"DETECTADA por {c['evento']} (inicio {c['inicio_evento_menos_accion_s']:+d} s respecto de la acción)"
                  if c["evento"] else "NO detectada")
        print(f"  {hora(c['ts'])}  {c['accion']:<8} {c['sensorId']:<22} [{c['observador'] or '?'}]  {estado}")
    fmt = lambda p, ic: "—" if p is None else f"{p:.2f} [{ic[0]:.2f}-{ic[1]:.2f}]"  # noqa: E731
    print(f"  {'acción':<9} {'evento':<16} {'acciones':>8} {'detect.':>7}  {'recall [IC95]':<19} {'precisión [IC95]':<19}")
    for accion, r in res["resumen"].items():
        print(f"  {accion:<9} {r['evento']:<16} {r['acciones']:>8} {r['detectadas']:>7}  "
              f"{fmt(r['recall'], r['recall_ic95']):<19} {fmt(r['precision'], r['precision_ic95']):<19}")
    if res["sin_registro"]:
        print("  Eventos del periodo sin acción registrada (posibles falsos positivos o acciones sin anotar):")
        for e in res["sin_registro"]:
            print(f"    {e['inicio']}  {e['tipo']:<16} {e['eventoId']}")
    if res["eventos_excluidos_por_no_verificable"]:
        print(f"  Excluidos por estar junto a una fila no_verificable: {res['eventos_excluidos_por_no_verificable']}")

if __name__ == "__main__":
    main()
