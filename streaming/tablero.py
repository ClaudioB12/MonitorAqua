"""
Measurements de InfluxDB para los tableros de F5 (Python puro, sin Spark).

Los usan Spark (en vivo) y streaming/cargar_historico_influx.py (historial real y datos aumentados), así que ambos
escriben exactamente lo mismo. No modifican las reglas congeladas: llaman a Detector.nivel() y leen el estado que el
detector ya mantiene.

  nivel_tanque    tags sensorId, origenLecturas, geometria (real | por_medir)
                  campos distanciaCm, distanciaCompensadaCm, compensada, nivelPct, litros, temperaturaC
  estado_tanque   tags sensorId, origenLecturas
                  campos tempAguaEstimadaC (provisional, tau del detector), horasResidencia, nivelPct, baseFugaCmH
  eventos_servicio  tags tipo, sensorId, zona, severidad, origenLecturas; campos del evento + los numéricos de
                  detalle aplanados (caudalLlenadoLpm, fueraDeFranja, tasaCmH, litrosPerdidos, ...)
"""
import json
import statistics as st

import influx

NUMERICOS_DETALLE = ("caudalLlenadoLpm", "subidaPct", "tasaCmH", "baseCmH", "litrosPerdidos", "fraccionCompensada",
                     "tempAguaEstimadaC", "tempAireC", "horaCruceTs", "huecoDeDatosH", "nivelPct", "litros")
BOOLEANOS_DETALLE = ("fueraDeFranja", "posibleHuecoDeDatos")


def linea_nivel(detector, lectura, sensor_id, origen_lecturas):
    """Una lectura de tanque -> punto nivel_tanque (None si la lectura no da nivel)."""
    cfg = detector.cfg.refrescar()
    geom = cfg.tanques.get(sensor_id)
    if not geom:
        return None
    valida, pct, nivel_cm, litros, dist_c, compensada = detector.nivel(lectura, geom, cfg.reglas)
    if not valida or pct is None:
        return None
    return influx.linea("nivel_tanque",
                        {"sensorId": sensor_id, "origenLecturas": origen_lecturas,
                         "geometria": "por_medir" if geom.get("por_medir") else "real"},
                        {"distanciaCm": _f(lectura.get("distanciaCm")), "distanciaCompensadaCm": _f(dist_c),
                         "compensada": bool(compensada), "nivelPct": _f(pct), "litros": _f(litros),
                         "temperaturaC": _f(lectura.get("temperaturaC"))},
                        lectura["timestamp"])


def linea_estado(sensor_id, estado, origen_lecturas):
    """Foto del estado del detector -> punto estado_tanque, en el tiempo de la última lectura procesada."""
    if not estado or not estado.get("ult_ts"):
        return None
    ultima = estado.get("ultima_llegada_ts")
    return influx.linea("estado_tanque", {"sensorId": sensor_id, "origenLecturas": origen_lecturas},
                        {"tempAguaEstimadaC": _f(estado.get("tw")),
                         # residencia = horas desde el último llenado detectado (adecuación D8); sin llenado, nula
                         "horasResidencia": _f((estado["ult_ts"] - ultima) / 3_600_000) if ultima else None,
                         "nivelPct": _f(st.median(estado["ult3"])) if estado.get("ult3") else None,
                         "baseFugaCmH": _f(estado.get("base_cm_h"))},
                        estado["ult_ts"])


def linea_evento(ev):
    detalle = ev.get("detalle") or {}
    campos = {"eventoId": ev["eventoId"], "valor": ev.get("valor"), "umbral": ev.get("umbral"),
              "unidad": ev.get("unidad"), "finTs": ev.get("finTs"), "detectadoTs": ev.get("detectadoTs"),
              "versionRegla": ev.get("versionRegla"), "detalle": json.dumps(detalle, ensure_ascii=False),
              "duracionMin": round((ev["finTs"] - ev["inicioTs"]) / 60000, 2)}
    for k in NUMERICOS_DETALLE:
        if isinstance(detalle.get(k), (int, float)) and not isinstance(detalle.get(k), bool):
            campos[k] = float(detalle[k])
    for k in BOOLEANOS_DETALLE:
        if isinstance(detalle.get(k), bool):
            campos[k] = detalle[k]
    return influx.linea("eventos_servicio",
                        {"tipo": ev["tipo"], "sensorId": ev["sensorId"], "zona": ev["zona"],
                         "severidad": ev["severidad"], "origenLecturas": ev["origenLecturas"]},
                        campos, ev["inicioTs"])


def _f(v):
    return None if v is None else round(float(v), 4)
