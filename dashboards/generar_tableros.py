"""
Genera los tableros de F5 como JSON versionado (Grafana los carga por provisioning desde dashboards/).

  python dashboards/generar_tableros.py      escribe bi_salud_agua.json y pipeline_salud.json

No se edita nada a mano en la interfaz: para cambiar un panel se cambia este script y se vuelve a generar.

Variables del tablero BI:
  origen    "real" -> bucket monitoraqua (en vivo, 30 días)  ·  "aumentado" -> bucket monitoraqua_aumentados
            (flota extrapolada, 28-sep a 27-oct). ${origen} es el bucket y ${origen:text} la etiqueta.
  sensorId  tanques con nivel (nivel_tanque) en ese bucket.
"""
import json
from pathlib import Path

AQUI = Path(__file__).resolve().parent
INFLUX = {"type": "influxdb", "uid": "influxdb"}
PROM = {"type": "prometheus", "uid": "prometheus"}
RANGO = 'range(start: v.timeRangeStart, stop: v.timeRangeStop)'
SENSOR = 'filter(fn: (r) => r.sensorId =~ /^${sensorId:regex}$/)'

_id = [0]


def nid():
    _id[0] += 1
    return _id[0]


def flux(q, ref="A"):
    return {"refId": ref, "datasource": INFLUX, "query": q.strip()}


def prom(expr, leyenda, ref="A", intervalo=None):
    t = {"refId": ref, "datasource": PROM, "expr": expr, "legendFormat": leyenda}
    if intervalo:
        t["interval"] = intervalo
    return t


def panel(tipo, titulo, x, y, w, h, targets, desc="", unidad=None, opciones=None, campo=None, transf=None,
          sin_datos=None):
    if sin_datos:  # mensaje del panel cuando la consulta no devuelve filas (en vez de "No data")
        campo = {**(campo or {}), "noValue": sin_datos}
    p = {"id": nid(), "type": tipo, "title": titulo, "description": desc, "gridPos": {"x": x, "y": y, "w": w, "h": h},
         "targets": targets, "datasource": targets[0]["datasource"] if targets else None,
         "fieldConfig": {"defaults": {**({"unit": unidad} if unidad else {}), **(campo or {})}, "overrides": []},
         "options": opciones or {}}
    if transf:
        p["transformations"] = transf
    return p


def texto(titulo, md, x, y, w, h):
    return {"id": nid(), "type": "text", "title": titulo, "gridPos": {"x": x, "y": y, "w": w, "h": h},
            "options": {"mode": "markdown", "content": md}}


def fila(titulo, y):
    return {"id": nid(), "type": "row", "title": titulo, "collapsed": False, "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
            "panels": []}


def prediccion_f7(titulo, dim, campos, x, y, w, h, desc, unidades=None):
    """F7: última predicción de cada tanque (measurement predicciones), con sus bandas, advertencias y el motivo
    cuando el modelo no aplica. Una fila por tanque y versión del modelo."""
    filtro = " or ".join(f'r._field == "{c}"' for c in campos + ["motivo"])
    q = f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "{dim}" and (r.tipo == "estimacion" or r.tipo == "motivo"))
  |> {SENSOR}
  |> filter(fn: (r) => {filtro})
  |> group(columns: ["sensorId", "_field"])
  |> last()
  |> map(fn: (r) => ({{r with _value: string(v: r._value)}}))
  |> group()
  |> pivot(rowKey: ["sensorId", "version_modelo"], columnKey: ["_field"], valueColumn: "_value")
'''
    # Sin "noValue": en una tabla, Grafana lo pondría en cada celda vacía (p. ej. las cifras de un tanque con motivo)
    t = panel("table", titulo, x, y, w, h, [flux(q)], desc, None, {"showHeader": True, "cellHeight": "sm"})
    orden = {"sensorId": 0, "version_modelo": 1, **{c: i + 2 for i, c in enumerate(campos)}, "motivo": len(campos) + 2}
    t["transformations"] = [{"id": "organize", "options": {"indexByName": orden}}]
    return t


def verificacion_f7(titulo, dim, campos, x, y, w, h, desc, unidad):
    """F7: pronóstico vs real. Los puntos 'verificacion' se escriben en el instante objetivo, cuando ya se conoce el
    valor real: el error en vivo de cada predicción pasada."""
    filtro = " or ".join(f'r._field == "{c}"' for c in campos)
    return panel("timeseries", titulo, x, y, w, h, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "{dim}" and r.tipo == "verificacion")
  |> {SENSOR}
  |> filter(fn: (r) => {filtro})
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''')], desc, unidad, {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "count"]},
                         "tooltip": {"mode": "multi"}},
        {"custom": {"drawStyle": "points", "pointSize": 5}}, sin_datos="Sin verificaciones todavía en el periodo")


def anotacion(nombre, tipo, color, bucket="${origen}", activa=True):
    q = f'''
from(bucket: "{bucket}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "{tipo}" and r._field == "valor")
  |> filter(fn: (r) => r.sensorId =~ /^${{sensorId:regex}}$/)
  |> map(fn: (r) => ({{_time: r._time, text: "{tipo} · " + r.sensorId + " · " + string(v: r._value), tags: r.severidad}}))
'''
    return {"name": nombre, "datasource": INFLUX, "enable": activa, "iconColor": color, "target": flux(q)}


# ---------------------------------------------------------------------------------------------------------------
# A. Tablero BI "Salud del servicio de agua – Juliaca" (criterio 6)
# ---------------------------------------------------------------------------------------------------------------

def tablero_bi():
    _id[0] = 0
    p, y = [], 0
    p.append(texto("", "**Salud del servicio de agua – Juliaca.** Variable `origen`: **real** (en vivo, bucket "
                   "`monitoraqua`) o **aumentado** (flota extrapolada, 28-sep a 27-oct: elija ese periodo en el "
                   "selector de tiempo). Los eventos de servicio aparecen como anotaciones sobre las series. "
                   "Ver `docs/tableros.md`.", 0, y, 24, 2))
    y += 2
    p.append(fila("KPIs", y)); y += 1
    p.append(panel("stat", "Lecturas por minuto (promedio del periodo)", 0, y, 6, 5, [flux(f'''
import "date"
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "lecturas_agg" and r._field == "n_total")
  |> filter(fn: (r) => date.second(t: r._time) == 0)
  |> group()
  |> sum()
  |> map(fn: (r) => ({{_value: float(v: r._value) / (float(v: int(v: v.timeRangeStop) - int(v: v.timeRangeStart)) / 60000000000.0)}}))
''')], "Todas las lecturas de todos los orígenes / minutos del periodo. Ventanas de 1 min alineadas al minuto "
       "(sin la superposición de la ventana deslizante) y de 15 min para las fuentes lentas.", "short",
        {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "none"}, {"decimals": 1}))
    p.append(panel("stat", "Lecturas válidas (%)", 6, y, 6, 5, [flux(f'''
import "date"
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "lecturas_agg" and (r._field == "n_total" or r._field == "n_validas"))
  |> filter(fn: (r) => date.second(t: r._time) == 0)
  |> group(columns: ["_field"])
  |> sum()
  |> group()
  |> map(fn: (r) => ({{r with k: 1}}))
  |> pivot(rowKey: ["k"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({{_value: float(v: r.n_validas) / float(v: r.n_total) * 100.0}}))
''')], "Lecturas válidas según la regla de calidad de F3 (sin -1, dentro del rango físico).", "percent",
        {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background"},
        {"decimals": 1, "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                                     {"color": "orange", "value": 80},
                                                                     {"color": "green", "value": 95}]}}))
    p.append(panel("bargauge", "Sensores activos por origen (última hora del periodo)", 12, y, 6, 5, [flux('''
import "date"
from(bucket: "${origen}")
  |> range(start: date.sub(d: 2h, from: v.timeRangeStop), stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "lecturas_agg" and r._field == "n_total")
  |> group(columns: ["origen", "sensorId"])
  |> count()
  |> group(columns: ["origen"])
  |> count()
''')], "Sensores con al menos una ventana en las 2 h finales del periodo (EA y USGS llegan con 25-54 min de "
       "atraso y en ventanas de 15 min, por eso 2 h).", "short",
        {"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]}},
        {"displayName": "${__field.labels.origen}"}))
    p.append(panel("bargauge", "Eventos de servicio en el periodo, por tipo (24 h por defecto)", 18, y, 6, 5, [flux('''
from(bucket: "${origen}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r._field == "valor")
  |> group(columns: ["tipo"])
  |> count()
''')], "Eventos de servicio (F4) por tipo dentro del periodo del selector de tiempo.", "short",
        {"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]}},
        {"displayName": "${__field.labels.tipo}"}, sin_datos="Sin eventos en el periodo"))
    y += 5

    # D2 ----------------------------------------------------------------------------------------------------------
    p.append(fila("D2 · Claudio Bustinza · Autonomía del tanque", y)); y += 1
    nivel = panel("timeseries", "Nivel del tanque (% y litros)", 0, y, 13, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and (r._field == "nivelPct" or r._field == "litros"))
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
''')], "Nivel calculado con la geometría de ingesta/tanques.yml y la distancia compensada con el DHT11. "
       "Si la geometría es provisional (panel de la derecha), % y litros son aproximados.", None,
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, sin_datos="Sin lecturas de nivel en el periodo")
    nivel["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byRegexp", "options": ".*nivelPct.*"}, "properties": [{"id": "unit", "value": "percent"}, {"id": "min", "value": 0}, {"id": "max", "value": 100}]},
        {"matcher": {"id": "byRegexp", "options": ".*litros.*"}, "properties": [{"id": "unit", "value": "litre"}, {"id": "custom.axisPlacement", "value": "right"}]}]
    p.append(nivel)
    p.append(panel("stat", "Geometría del tanque", 13, y, 3, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "nivelPct")
  |> {SENSOR}
  |> group(columns: ["sensorId"])
  |> last()
  |> map(fn: (r) => ({{sensorId: r.sensorId, _value: r.geometria}}))
''')], "por_medir = medidas provisionales en ingesta/tanques.yml: el nivel en % y litros es aproximado.", None,
        {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "/.*/"}, "colorMode": "background", "textMode": "value_and_name"},
        {"mappings": [{"type": "value", "options": {
            "por_medir": {"text": "GEOMETRÍA POR MEDIR", "color": "orange"},
            "real": {"text": "geometría real", "color": "green"}}}]}))
    p.append(prediccion_f7("F7 · Horas de autonomía (perfil de consumo)", "D2",
                           ["autonomia_texto", "autonomia_p10_h", "autonomia_p90_h", "autonomia_consumo_medio_h",
                            "nivel_pct", "advertencia"], 16, y, 8, 8,
                           "D2 (d2.2): horas hasta el 10 % con el perfil de consumo P50, hasta 12 h (\"más de 12 h\" si "
                           "la supera); banda P10-P90 calibrada; el consumo medio como comparación. En ThingSpeak el "
                           "perfil tiende a ser optimista. Si no aplica, el motivo."))
    y += 8

    # D4 ----------------------------------------------------------------------------------------------------------
    p.append(fila("D4 · Wilbert Mayta · Llegada del agua", y)); y += 1
    p.append(panel("table", "Llegadas de agua y cortes", 0, y, 10, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and (r.tipo == "llegada_agua" or r.tipo == "corte_prolongado"))
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "valor" or r._field == "unidad" or r._field == "caudalLlenadoLpm" or r._field == "fueraDeFranja" or r._field == "duracionMin")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "tipo", "sensorId", "severidad", "valor", "unidad", "caudalLlenadoLpm", "fueraDeFranja"])
  |> sort(columns: ["_time"], desc: true)
''')], "Línea de tiempo de llegada_agua (valor = subida en %) y corte_prolongado (valor = horas sin llegada).",
        None, {"showHeader": True}, sin_datos="Sin llegadas ni cortes en el periodo"))
    p.append(panel("timeseries", "Horas entre llegadas", 10, y, 7, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua" and r._field == "valor")
  |> {SENSOR}
  |> group(columns: ["sensorId"])
  |> sort(columns: ["_time"])
  |> elapsed(unit: 1m)
  |> map(fn: (r) => ({{_time: r._time, sensorId: r.sensorId, _value: float(v: r.elapsed) / 60.0}}))
''')], "Horas desde la llegada anterior del mismo tanque (intervalo entre llegadas).", "h",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"custom": {"drawStyle": "points", "pointSize": 8}}))
    p.append(panel("timeseries", "Caudal de llenado estimado", 17, y, 7, 4, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua" and r._field == "caudalLlenadoLpm")
  |> {SENSOR}
  |> keep(columns: ["_time", "_value", "sensorId"])
''')], "Caudal = subida × capacidad / Δt (adecuación de D4). Con geometría provisional es aproximado.", "L/min",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"custom": {"drawStyle": "points", "pointSize": 8}}))
    p.append(prediccion_f7("F7 · Próxima llegada del agua", "D4",
                           ["horas_moda", "ventana_ini_h", "ventana_fin_h", "prob_6h", "prob_12h", "prob_24h"],
                           17, y + 4, 7, 4,
                           "D4 (d4.1): horas hasta la llegada más probable, ventana P10-P90 calibrada y probabilidad de "
                           "llegada en 6/12/24 h. Si no aplica (tanque de lluvia, sin modelo), el motivo."))
    y += 8

    # D6 ----------------------------------------------------------------------------------------------------------
    p.append(fila("D6 · Joel Huillca · Fugas", y)); y += 1
    p.append(panel("timeseries", "Distancia cruda vs compensada (DHT11)", 0, y, 12, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and (r._field == "distanciaCm" or r._field == "distanciaCompensadaCm"))
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
''')], "Distancia medida por el HC-SR04 y la misma compensada por la temperatura del aire: × c(T)/c(20 °C). "
       "Incertidumbre ±0.18 cm a 50 cm por el ±2 °C del DHT11.", "lengthmm",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"unit": "cm"}))
    p.append(panel("table", "Alertas de fuga", 12, y, 6, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "alerta_fuga")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "valor" or r._field == "tasaCmH" or r._field == "litrosPerdidos" or r._field == "baseCmH")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "sensorId", "severidad", "valor", "tasaCmH", "baseCmH", "litrosPerdidos"])
  |> sort(columns: ["_time"], desc: true)
''')], "alerta_fuga: descenso nocturno (cm) sobre la distancia compensada; tasa frente a la tasa nocturna habitual.",
        None, {"showHeader": True}, sin_datos="Sin alertas de fuga en el periodo"))
    p.append(prediccion_f7("F7 · Pérdida proyectada por fuga (última noche)", "D6",
                           ["noche", "tasa_lph", "exceso_lph", "p_fuga", "perdida_24h_l", "perdida_24h_p10_l",
                            "perdida_24h_p90_l"], 18, y, 6, 8,
                           "D6 (d6.1), una vez por noche: tasa nocturna (L/h), exceso sobre lo habitual, P(fuga) y "
                           "pérdida a 24 h con su banda P10-P90 (sin la de 7 días). Complementa a alerta_fuga. Si no "
                           "aplica (geometría provisional, canal sin distancia), el motivo."))
    y += 8

    # D8 ----------------------------------------------------------------------------------------------------------
    p.append(fila("D8 · Josué Ochoa · Temperatura y residencia del agua", y)); y += 1
    temps = panel("timeseries", "Temperaturas: aire de la cámara, Juliaca y agua estimada (provisional)", 0, y, 12, 8, [
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "temperaturaC")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _field: "aire cámara (DHT11)"}}))
''', "A"),
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "lecturas_agg" and r.sensorId == "open-meteo-juliaca" and r._field == "temperaturaC_media")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _field: "Juliaca (Open-Meteo)"}}))
''', "B"),
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "estado_tanque" and r._field == "tempAguaEstimadaC")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _field: "agua estimada (provisional)"}}))
''', "C")],
        "Agua estimada: inercia de primer orden desde el aire de la cámara (τ = 25.2 h, ThingSpeak 3418755), "
        "versión provisional hasta el modelo de F6. Open-Meteo solo existe para origen = real.", "celsius",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}})
    p.append(temps)
    p.append(panel("timeseries", "Horas de residencia del agua", 12, y, 6, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "estado_tanque" and r._field == "horasResidencia")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
''')], "Horas desde el último llenado detectado (llegada_agua). Sin llenados detectados no hay valor.", "h",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}))
    p.append(prediccion_f7("F7 · Agua estimada, cruce del umbral y residencia", "D8",
                           ["agua_c", "agua_p10_c", "agua_p90_c", "agua_6h_c", "agua_12h_c", "agua_12h_p10_c",
                            "agua_12h_p90_c", "prob_cruce_bajo_12h", "prob_cruce_alto_12h", "residencia_h",
                            "advertencia"], 18, y, 6, 8,
                           "D8 (d8.1): agua estimada ahora y a 6/12 h con banda P10-P90, probabilidad de cruzar 4 °C o "
                           "25 °C en 12 h y residencia (edad media del agua). Estimación sin sensor de agua: error de "
                           "transferencia de 1-2 °C."))
    y += 8

    # F7: pronóstico vs real ----------------------------------------------------------------------------------------
    p.append(fila("F7 · Pronóstico vs real (error en vivo de las predicciones pasadas)", y)); y += 1
    p.append(verificacion_f7("D2 · Nivel a 60 min: real vs pronosticado", "D2", ["nivel_real_pct", "nivel_pred_pct"],
                             0, y, 12, 7, "Cada pronóstico del nivel a 60 min, comparado con el nivel real cuando "
                             "llegó ese instante.", "percent"))
    p.append(verificacion_f7("D2 · Error del nivel a 60 min (pp)", "D2", ["error_pp"], 12, y, 12, 7,
                             "Real − pronosticado. La leyenda muestra el error medio y cuántos pronósticos se "
                             "verificaron.", "short"))
    y += 7
    p.append(verificacion_f7("D4 · Error de la hora de llegada (h)", "D4", ["error_h"], 0, y, 8, 7,
                             "En cada llegada observada: llegada real − hora más probable del último pronóstico. "
                             "Negativo = llegó antes.", "h"))
    p.append(verificacion_f7("D6 · Pérdida a 24 h: pronosticada vs observada (L)", "D6",
                             ["perdida_pred_l", "perdida_real_proxy_l"], 8, y, 8, 7,
                             "La pérdida pronosticada en una noche frente al exceso observado la noche siguiente × 24 h "
                             "(aproximación observable: no hay fugas reales etiquetadas).", "litre"))
    p.append(verificacion_f7("D8 · Agua: real vs pronosticada (6 y 12 h)", "D8", ["agua_real_c", "agua_pred_c"],
                             16, y, 8, 7, "Solo ThingSpeak 3418755 mide el agua (DS18B20): sus pronósticos a 6 y 12 "
                             "h frente al agua real.", "celsius"))
    y += 7

    variables = [
        {"name": "origen", "label": "origenLecturas", "type": "custom",
         "query": "real : monitoraqua, aumentado : monitoraqua_aumentados",
         "current": {"text": "real", "value": "monitoraqua"},
         "options": [{"text": "real", "value": "monitoraqua", "selected": True},
                     {"text": "aumentado", "value": "monitoraqua_aumentados", "selected": False}]},
        {"name": "sensorId", "label": "sensorId", "type": "query", "datasource": INFLUX, "refresh": 2,
         "multi": True, "includeAll": True, "allValue": ".*", "current": {"text": "All", "value": "$__all"},
         "query": 'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "${origen}", tag: "sensorId", '
                  'predicate: (r) => r._measurement == "nivel_tanque" or r._measurement == "predicciones", '
                  'start: -120d)'},
    ]
    anotaciones = [anotacion("llegada_agua", "llegada_agua", "blue"),
                   anotacion("corte_prolongado", "corte_prolongado", "red"),
                   anotacion("alerta_fuga", "alerta_fuga", "orange"),
                   anotacion("autonomia_baja", "autonomia_baja", "purple"),
                   anotacion("falla_sensor", "falla_sensor", "#8e8e8e", activa=False),  # muchas: se activa a mano
                   anotacion("umbral_termico", "umbral_termico", "yellow")]
    return {"uid": "monitoraqua-bi", "title": "Salud del servicio de agua – Juliaca",
            "tags": ["monitoraqua", "f5", "f7", "bi", "criterio-6", "criterio-7"], "timezone": "America/Lima", "schemaVersion": 39,
            "version": 1, "editable": False, "graphTooltip": 1, "refresh": "30s",
            "time": {"from": "now-24h", "to": "now"},
            "templating": {"list": variables}, "annotations": {"list": anotaciones}, "panels": p}


# ---------------------------------------------------------------------------------------------------------------
# B. Tablero "Salud del pipeline" (criterio 4)
# ---------------------------------------------------------------------------------------------------------------
TRIGGER_MS = ('(if r.consulta == "rapidas_agg" or r.consulta == "latencias" or r.consulta == "predicciones" then 30000.0 '
              'else if r.consulta == "lentas_agg" or r.consulta == "bronce" then 60000.0 else 10000.0)')


def tablero_pipeline():
    _id[0] = 0
    p, y = [], 0
    p.append(texto("", "**Salud del pipeline** (criterio 4): Kafka (Prometheus/kafka-exporter), Spark "
                   "(`spark_progreso`), frescura y latencia por fuente, volumen por tópico y correlación en un mismo "
                   "eje de tiempo. Los `falla_sensor` aparecen como anotaciones. Ver `docs/tableros.md`.", 0, y, 24, 2))
    y += 2
    p.append(fila("Kafka", y)); y += 1
    p.append(panel("timeseries", "Mensajes por segundo por tópico", 0, y, 12, 7, [prom(
        'sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[2m]))', "{{topic}}")],
        "Tasa de publicación por tópico (derivada del offset final de cada partición).", "short",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}))
    p.append(panel("timeseries", "Lag por grupo de consumidores", 12, y, 12, 7, [prom(
        'sum by (consumergroup, topic) (kafka_consumergroup_lag)', "{{consumergroup}} · {{topic}}")],
        "Mensajes publicados que el grupo aún no confirmó. Spark no registra grupo (su avance está en la fila "
        "Spark). aqua-monitor-group (S09) se borró en F5 por estar abandonado.", "short",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 10}]},
         "custom": {"thresholdsStyle": {"mode": "line"}}}))
    y += 7

    p.append(fila("Spark Structured Streaming (spark_progreso)", y)); y += 1
    p.append(panel("timeseries", "Filas de entrada por segundo, por consulta", 0, y, 12, 7, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "inputRowsPerSecond")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
''')], "inputRowsPerSecond de cada micro-lote.", "short", {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}))
    p.append(panel("timeseries", "Duración del lote frente al intervalo del trigger (%)", 12, y, 12, 7, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "batchDurationMs")
  |> map(fn: (r) => ({{r with _value: float(v: r._value) / {TRIGGER_MS} * 100.0}}))
  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)
''')], "batchDurationMs / trigger × 100. Sobre 100 % la consulta no alcanza a procesar un lote antes del "
       "siguiente. Triggers: rapidas_agg 30 s, lentas_agg 60 s, esp32_detalle 10 s, bronce 60 s, "
       "eventos_servicio 10 s, latencias 30 s.", "percent",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 100}]},
         "custom": {"thresholdsStyle": {"mode": "line"}}}))
    y += 7
    p.append(panel("timeseries", "Filas descartadas por watermark", 0, y, 12, 6, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "numRowsDroppedByWatermark")
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
''')], "Lecturas que llegaron más tarde que el watermark (7 min rápidas, 90 min lentas) y se descartaron.",
        "short", {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"custom": {"drawStyle": "bars"}}))
    p.append(panel("timeseries", "Duplicados descartados (eventId)", 12, y, 12, 6, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "numDroppedDuplicateRows")
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
''')], "dropDuplicatesWithinWatermark(eventId).", "short",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"custom": {"drawStyle": "bars"}}))
    y += 6

    p.append(fila("Frescura y latencia por fuente", y)); y += 1
    p.append(panel("bargauge", "Minutos desde la última lectura, por origen", 0, y, 8, 8, [flux('''
from(bucket: "monitoraqua")
  |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "latencia_ingesta" and r._field == "s")
  |> group(columns: ["origen"])
  |> last()
  |> map(fn: (r) => ({origen: r.origen, _value: float(v: int(v: now()) - int(v: r._time)) / 60000000000.0}))
''')], "Ahora menos el tiempo de medición de la última lectura procesada de cada origen. Incluye el atraso propio "
       "de cada fuente (EA y USGS publican con 25-54 min de atraso).", "m",
        {"orientation": "horizontal", "displayMode": "gradient", "reduceOptions": {"calcs": ["lastNotNull"]}},
        {"displayName": "${__field.labels.origen}", "decimals": 1, "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                                     {"color": "orange", "value": 10},
                                                                     {"color": "red", "value": 90}]}}))
    lat = panel("table", "Latencia de ingesta por origen: p50 y p95 (s)", 8, y, 7, 8, [
        flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "latencia_ingesta" and r._field == "s")
  |> group(columns: ["origen"])
  |> quantile(q: 0.5)
  |> map(fn: (r) => ({{origen: r.origen, p50: r._value}}))
  |> group()
''', "A"),
        flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "latencia_ingesta" and r._field == "s")
  |> group(columns: ["origen"])
  |> quantile(q: 0.95)
  |> map(fn: (r) => ({{origen: r.origen, p95: r._value}}))
  |> group()
''', "B")],
        "latencia_ingesta_s = ingestaTs − timestamp, por lectura (consulta latencias). Watermark de las rápidas: "
        "420 s.", "s", {"showHeader": True}, {"decimals": 1},
        [{"id": "merge", "options": {}}])
    p.append(lat)
    p.append(panel("timeseries", "Latencia p95 por origen (ventanas de 30 min)", 15, y, 9, 8, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "latencia_ingesta" and r._field == "s")
  |> group(columns: ["origen"])
  |> aggregateWindow(every: 30m, fn: (column, tables=<-) => tables |> quantile(q: 0.95, column: column), createEmpty: false)
''')], "p95 de la latencia de ingesta por origen. Línea roja: watermark de las fuentes rápidas (420 s).", "s",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 420}]},
         "custom": {"thresholdsStyle": {"mode": "line"}}}))
    y += 8

    p.append(fila("Volumen por tópico (para los costos de F8)", y)); y += 1
    p.append(panel("timeseries", "Mensajes por día por tópico", 0, y, 8, 7, [prom(
        'sum by (topic) (increase(kafka_topic_partition_current_offset{topic!~"__.*"}[1d]))', "{{topic}}",
        intervalo="1d")], "Mensajes publicados por día (Prometheus, kafka-exporter).", "short",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}, {"custom": {"drawStyle": "bars", "fillOpacity": 60}}))
    p.append(panel("timeseries", "Bytes por día por tópico", 8, y, 8, 7, [flux('''
from(bucket: "monitoraqua")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "kafka_volumen" and r._field == "bytes")
  |> aggregateWindow(every: 1d, fn: last, createEmpty: false)
  |> difference(nonNegative: false)
''')], "Variación diaria del tamaño en disco (describe_log_dirs, servicio aqua-volumen). Con retención de 7 días "
       "el tamaño se estabiliza: el crecimiento diario es lo que entra; la retención borra segmentos viejos.",
        "decbytes", {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"custom": {"drawStyle": "bars", "fillOpacity": 60}},
        sin_datos="Hacen falta 2 días de mediciones de aqua-volumen (empezó el 2026-10-29)"))
    p.append(panel("table", "Tamaño actual por tópico", 16, y, 8, 7, [flux('''
from(bucket: "monitoraqua")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "kafka_volumen")
  |> last()
  |> pivot(rowKey: ["topico"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> map(fn: (r) => ({topico: r.topico, bytes: r.bytes, mensajes_retenidos: r.mensajes_retenidos,
                      bytes_por_mensaje: if r.mensajes_retenidos > 0 then float(v: r.bytes) / float(v: r.mensajes_retenidos) else 0.0}))
''')], "Último registro de aqua-volumen: bytes en disco, mensajes retenidos y bytes por mensaje.", None,
        {"showHeader": True}))
    y += 7

    p.append(fila("Correlación (mismo eje de tiempo; anotaciones: falla_sensor)", y)); y += 1
    p.append(panel("timeseries", "Entrada de Spark (filas/s)", 0, y, 24, 5, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "inputRowsPerSecond")
  |> filter(fn: (r) => r.consulta == "rapidas_agg" or r.consulta == "eventos_servicio")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
''')], "", "short", {"legend": {"displayMode": "list", "placement": "right"}}))
    y += 5
    p.append(panel("timeseries", "Duración del lote (ms)", 0, y, 24, 5, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "spark_progreso" and r._field == "batchDurationMs")
  |> filter(fn: (r) => r.consulta == "rapidas_agg" or r.consulta == "eventos_servicio")
  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)
''')], "", "ms", {"legend": {"displayMode": "list", "placement": "right"}}))
    y += 5
    p.append(panel("timeseries", "Lag y mensajes/s del tópico real", 0, y, 24, 5, [
        prom('sum by (consumergroup) (kafka_consumergroup_lag)', "lag {{consumergroup}}", "A"),
        prom('sum(rate(kafka_topic_partition_current_offset{topic="aqua-eventos-real"}[2m]))', "aqua-eventos-real msg/s", "B")],
        "", "short", {"legend": {"displayMode": "list", "placement": "right"}}))
    y += 5
    p.append(panel("timeseries", "Lecturas del ESP32 por minuto (huecos = sin datos)", 0, y, 24, 5, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "latencia_ingesta" and r.origen == "esp32-fisico" and r._field == "s")
  |> aggregateWindow(every: 1m, fn: count, createEmpty: true)
''')], "Debería rondar 12/min (una cada 5 s). Un hueco a cero es el ESP32 o la PC sin datos (p. ej. una "
       "suspensión); al volver, la ingesta se pone al día con un pico.", "short",
        {"legend": {"displayMode": "list", "placement": "right"}}))
    y += 5

    p.append(fila("Inferencia en vivo (F7, consulta predicciones)", y)); y += 1
    p.append(panel("timeseries", "Latencia de la inferencia por lote (ms)", 0, y, 12, 6, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "inferencia" and r._field == "duracion_ms")
  |> aggregateWindow(every: v.windowPeriod, fn: max, createEmpty: false)
''')], "Tiempo de los modelos de F6 en cada micro-lote de la consulta predicciones (trigger 30 s). La "
       "duración total del lote está en la fila Spark.", "ms",
        {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}, {"color": "red", "value": 30000}]},
         "custom": {"thresholdsStyle": {"mode": "line"}}}))
    p.append(panel("timeseries", "Predicciones escritas por lote", 12, y, 12, 6, [flux(f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "inferencia" and (r._field == "puntos_real" or r._field == "puntos_aumentado" or r._field == "recargas"))
  |> aggregateWindow(every: v.windowPeriod, fn: sum, createEmpty: false)
''')], "Puntos de predicción al bucket real y al de aumentados, y recargas de modelos (cambio de archivo sin "
       "reiniciar el job).", "short", {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        {"custom": {"drawStyle": "bars", "fillOpacity": 60}}))
    y += 6

    anotaciones = [anotacion("falla_sensor", "falla_sensor", "red", bucket="monitoraqua")]
    for a in anotaciones:  # en este tablero no hay variable sensorId
        a["target"]["query"] = a["target"]["query"].replace('  |> filter(fn: (r) => r.sensorId =~ /^${sensorId:regex}$/)\n', "")
    return {"uid": "monitoraqua-pipeline", "title": "Salud del pipeline", "tags": ["monitoraqua", "f5", "criterio-4"],
            "timezone": "America/Lima", "schemaVersion": 39, "version": 1, "editable": False, "graphTooltip": 1,
            "refresh": "30s", "time": {"from": "now-24h", "to": "now"}, "templating": {"list": []},
            "annotations": {"list": anotaciones}, "panels": p}


NOMBRES = {  # título del panel -> nombre de cada serie (variables de Grafana sobre las etiquetas de Flux)
    "Nivel del tanque (% y litros)": "${__field.labels.sensorId} · ${__field.name}",
    "Geometría del tanque": "${__field.labels.sensorId}",
    "Horas entre llegadas": "${__field.labels.sensorId}",
    "Caudal de llenado estimado": "${__field.labels.sensorId}",
    "Distancia cruda vs compensada (DHT11)": "${__field.labels.sensorId} · ${__field.name}",
    "Temperaturas: aire de la cámara, Juliaca y agua estimada (provisional)": "${__field.name} · ${__field.labels.sensorId}",
    "Horas de residencia del agua": "${__field.labels.sensorId}",
    "Filas de entrada por segundo, por consulta": "${__field.labels.consulta}",
    "Duración del lote frente al intervalo del trigger (%)": "${__field.labels.consulta}",
    "Filas descartadas por watermark": "${__field.labels.consulta}",
    "Duplicados descartados (eventId)": "${__field.labels.consulta}",
    "Latencia p95 por origen (ventanas de 30 min)": "${__field.labels.origen}",
    "Bytes por día por tópico": "${__field.labels.topico}",
    "Entrada de Spark (filas/s)": "${__field.labels.consulta}",
    "Duración del lote (ms)": "${__field.labels.consulta}",
    "Lecturas del ESP32 por minuto (huecos = sin datos)": "lecturas del ESP32 por minuto",
    "D2 · Nivel a 60 min: real vs pronosticado": "${__field.labels.sensorId} · ${__field.name}",
    "D2 · Error del nivel a 60 min (pp)": "${__field.labels.sensorId}",
    "D4 · Error de la hora de llegada (h)": "${__field.labels.sensorId}",
    "D6 · Pérdida a 24 h: pronosticada vs observada (L)": "${__field.labels.sensorId} · ${__field.name}",
    "D8 · Agua: real vs pronosticada (6 y 12 h)": "${__field.labels.sensorId} · ${__field.name}",
    "Latencia de la inferencia por lote (ms)": "duración de la inferencia",
    "Predicciones escritas por lote": "${__field.name}",
}


def nombrar(t):
    for x in t["panels"]:
        if x.get("title") in NOMBRES:
            x["fieldConfig"]["defaults"]["displayName"] = NOMBRES[x["title"]]
    return t


def main():
    for nombre, t in (("bi_salud_agua.json", nombrar(tablero_bi())), ("pipeline_salud.json", nombrar(tablero_pipeline()))):
        (AQUI / nombre).write_text(json.dumps(t, ensure_ascii=False, indent=1), encoding="utf-8")
        print(nombre, len([x for x in t["panels"] if x["type"] not in ("row", "text")]), "paneles")


if __name__ == "__main__":
    main()
