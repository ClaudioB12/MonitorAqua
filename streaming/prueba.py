"""Sensores de prueba (lista única: ingesta/sensores_prueba.yml): por sensorId exacto o por prefijo (F8)."""
import yaml


def cargar(ruta):
    """(sensorId exactos, prefijos). Sin archivo: nada que excluir."""
    try:
        with open(ruta, encoding="utf-8") as f:
            d = yaml.safe_load(f) or {}
    except FileNotFoundError:
        return [], []
    return ([s["sensorId"] for s in d.get("sensores") or []], [p["prefijo"] for p in d.get("prefijos") or []])


def es_prueba(sensor_id, ids, prefijos):
    return sensor_id in ids or any(str(sensor_id or "").startswith(p) for p in prefijos)


def filtro_spark(col, ids, prefijos):
    """Condición de Spark que deja pasar solo lo que NO es de prueba (None si no hay nada que excluir)."""
    cond = None
    if ids:
        cond = ~col.isin(ids)
    for p in prefijos:
        c = ~col.startswith(p)
        cond = c if cond is None else cond & c
    return cond
