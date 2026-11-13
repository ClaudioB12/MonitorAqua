"""
Genera el tablero de la dimensión D6 (pérdida proyectada por fuga) como JSON versionado.
Grafana lo carga por provisioning desde dashboards/, igual que los demás tableros del proyecto.

  python dashboards/generar_tablero_d6.py      escribe dashboards/d6_fugas.json

Reutiliza las funciones de generar_tableros.py. No modifica los otros tableros.
Responsable de D6: Joel Huillca Lima.

Fuentes (InfluxDB, escritas por el job de Spark):
  nivel_tanque      distanciaCm, distanciaCompensadaCm (compensada con el DHT11) y temperaturaC
  eventos_servicio  tipo = alerta_fuga (detector f4.4) y llegada_agua (anotación: un llenado no es fuga)
  predicciones      dimension = D6: estimacion (una por noche, modelo d6.1), verificacion (pérdida a 24 h), motivo
"""
import json

from generar_tableros import (AQUI, INFLUX, RANGO, SENSOR, anotacion, fila, flux, panel, prediccion_f7, texto,
                              verificacion_f7)

LEYENDA = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
STAT = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none",
        "graphMode": "none", "textMode": "value"}
SEMAFORO = {**STAT, "colorMode": "value"}
ESTIMACION = ' and r.dimension == "D6" and r.tipo == "estimacion"'
ABS = '''
  |> map(fn: (r) => ({r with _value: if float(v: r._value) < 0.0 then -float(v: r._value) else float(v: r._value)}))'''

INFERENCIA = """
**En vivo.** Si *Última lectura recibida hace* está en **verde**, la cadena ESP32 → Firebase → puente → Kafka → Spark →
InfluxDB → Grafana funciona. La distancia es del sensor al agua: **más distancia = menos agua**.

**Distancia cruda vs compensada.** El ultrasónico mide con la velocidad del sonido, que cambia con la temperatura. La
distancia **compensada** corrige con el aire del DHT11 (× c(T)/c(20 °C)); la fuga se busca sobre la compensada para que
un cambio de temperatura no parezca una bajada del nivel.

**Noches.** La fuga se mide de **00:00 a 03:30**, cuando casi no hay consumo: si el nivel baja de noche más que en las
noches habituales, sobra un **exceso** (L/h). El modelo d6.1 convierte ese exceso en **P(fuga)** y en la **pérdida
proyectada a 24 h**, con su banda P10-P90. Una llegada de agua (marca azul) no es fuga.

**Alertas de fuga.** La regla del detector f4.4 (marca roja): descenso nocturno por encima de la tasa habitual. El
modelo la complementa con una probabilidad y con litros.

**Pronóstico vs real.** No hay fugas reales etiquetadas: la pérdida pronosticada en una noche se compara con el exceso
observado la noche siguiente × 24 h (aproximación observable).

**Modelo.** d6.1 en `batch/modelo_d6.py` (y `batch/modelo_d6_mllib.py`); validación en `docs/modelo-d6.md`. Con
geometría provisional (ESP32) o un canal que informa nivel y no distancia, la tabla muestra el **motivo**.
"""


def ultimo(medida, campo, extra=""):
    """Último valor de un campo en los últimos 30 días (extra = condición adicional del filtro)."""
    return f'''
from(bucket: "${{origen}}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "{medida}" and r._field == "{campo}"{extra})
  |> {SENSOR}
  |> group()
  |> last()
'''


def tablero_d6():
    p, y = [], 0

    p.append(fila("En vivo · distancia y última noche", y)); y += 1
    p.append(panel("stat", "Última lectura recibida hace", 0, y, 4, 4, [flux('''
from(bucket: "${origen}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "distanciaCm")
  |> ''' + SENSOR + '''
  |> group()
  |> last()
  |> map(fn: (r) => ({_time: r._time, _value: float(v: int(v: now()) - int(v: r._time)) / 1000000000.0}))
''')], "Segundos desde la última lectura. Verde: llega en vivo; rojo: más de 5 min sin datos.", "s", SEMAFORO,
        {"decimals": 0, "color": {"mode": "thresholds"},
         "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                      {"color": "orange", "value": 60},
                                                      {"color": "red", "value": 300}]}},
        sin_datos="Sin lecturas"))
    p.append(panel("stat", "Distancia compensada", 4, y, 4, 4, [flux(ultimo("nivel_tanque", "distanciaCompensadaCm"))],
                   "Del sensor al agua, corregida por la temperatura del aire (DHT11).", "suffix: cm", STAT,
                   {"decimals": 1}, sin_datos="Sin lecturas"))
    p.append(panel("stat", "Temperatura del aire", 8, y, 4, 4, [flux(ultimo("nivel_tanque", "temperaturaC"))],
                   "DHT11 de la cámara del tanque (±2 °C): es la que se usa para compensar.", "celsius", STAT,
                   {"decimals": 1}, sin_datos="Sin lecturas"))
    p.append(panel("stat", "P(fuga) de la última noche", 12, y, 4, 4,
                   [flux(ultimo("predicciones", "p_fuga", ESTIMACION))],
                   "Probabilidad de fuga del modelo d6.1 (umbral de decisión 0,70).", "percentunit", SEMAFORO,
                   {"decimals": 0, "min": 0, "max": 1, "color": {"mode": "thresholds"},
                    "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                                 {"color": "orange", "value": 0.4},
                                                                 {"color": "red", "value": 0.7}]}},
                   sin_datos="Sin noche evaluada (ver motivo)"))
    p.append(panel("stat", "Pérdida proyectada a 24 h", 16, y, 4, 4,
                   [flux(ultimo("predicciones", "perdida_24h_l", ESTIMACION))],
                   "Litros que se perderían en 24 h si la fuga sigue (P × exceso × 24 h × persistencia).", "litre",
                   STAT, {"decimals": 1}, sin_datos="Sin noche evaluada"))
    p.append(panel("stat", "Alertas de fuga", 20, y, 4, 4, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "alerta_fuga" and r._field == "valor")
  |> {SENSOR}
  |> group()
  |> count()
''')], "Eventos alerta_fuga del detector f4.4 en el periodo elegido.", "none", STAT, sin_datos="0"))
    y += 4

    p.append(fila("Lo observado", y)); y += 1
    dist = panel("timeseries", "Distancia cruda vs compensada (con llegadas y alertas de fuga)", 0, y, 16, 9, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and (r._field == "distanciaCm" or r._field == "distanciaCompensadaCm"))
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''')], "Más distancia = menos agua. La fuga se busca de noche sobre la compensada; marca azul = llegada de agua, "
       "marca roja = alerta_fuga.", "suffix: cm", LEYENDA, sin_datos="Sin distancia en el periodo")
    dist["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p.append(dist)
    p.append(panel("table", "Alertas de fuga", 16, y, 8, 9, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "alerta_fuga")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "valor" or r._field == "tasaCmH" or r._field == "baseCmH" or r._field == "litrosPerdidos")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "sensorId", "severidad", "valor", "tasaCmH", "baseCmH", "litrosPerdidos"])
  |> rename(columns: {{_time: "hora", valor: "descenso_cm"}})
  |> sort(columns: ["hora"], desc: true)
''')], "Descenso nocturno (cm), su tasa frente a la tasa nocturna habitual y los litros estimados.", None,
        {"showHeader": True, "cellHeight": "sm"}))
    y += 9

    p.append(fila("Predicción · pérdida por fuga (modelo d6.1, una vez por noche)", y)); y += 1
    noches = panel("timeseries", "Tasa nocturna, referencia y exceso por noche (L/h)", 0, y, 14, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D6" and r.tipo == "estimacion")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "tasa_lph" or r._field == "referencia_lph" or r._field == "exceso_lph")
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''')], "Cada punto es una noche: la tasa de bajada (L/h), la referencia (mediana de las noches anteriores) y el "
       "exceso = tasa − referencia. Un exceso sostenido es lo que el modelo convierte en P(fuga).", "short",
        LEYENDA, {"custom": {"drawStyle": "points", "pointSize": 8}}, sin_datos="Sin noches evaluadas en el periodo")
    noches["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p.append(noches)
    p.append(prediccion_f7("Última noche por tanque (y motivo si no aplica)", "D6",
                           ["noche", "tasa_lph", "exceso_lph", "p_fuga", "perdida_24h_l", "perdida_24h_p10_l",
                            "perdida_24h_p90_l"], 14, y, 10, 8,
                           "D6 (d6.1): tasa nocturna, exceso, P(fuga) y pérdida a 24 h con su banda P10-P90. Si no "
                           "aplica (geometría provisional, canal sin distancia, faltan noches), el motivo."))
    y += 8

    p.append(fila("Pronóstico vs real · ¿acertó el modelo?", y)); y += 1
    VERIF = f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D6" and r.tipo == "verificacion" and r._field == "error_l")
  |> {SENSOR}'''
    p.append(panel("stat", "Noches verificadas", 0, y, 6, 4, [flux(VERIF + '''
  |> group()
  |> count()
''')], "Noches cuya pérdida pronosticada ya tiene la noche siguiente para compararla.", "none", STAT,
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Error medio de la pérdida a 24 h (MAE)", 6, y, 6, 4, [flux(VERIF + ABS + '''
  |> group()
  |> mean()
''')], "Promedio de |pérdida observada (aprox.) − pronosticada| en litros.", "litre", STAT, {"decimals": 1},
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Sesgo (observado − pronosticado)", 12, y, 6, 4, [flux(VERIF + '''
  |> group()
  |> mean()
''')], "Negativo: el modelo pronosticó más pérdida de la que se observó.", "litre", STAT, {"decimals": 1},
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("table", "Error por tanque", 18, y, 6, 4, [flux(VERIF + ABS + '''
  |> group(columns: ["sensorId"])
  |> reduce(identity: {verificadas: 0, suma: 0.0},
            fn: (r, accumulator) => ({verificadas: accumulator.verificadas + 1, suma: accumulator.suma + r._value}))
  |> map(fn: (r) => ({sensorId: r.sensorId, verificadas: r.verificadas, MAE_l: r.suma / float(v: r.verificadas)}))
  |> group()
''')], "MAE de la pérdida a 24 h por tanque.", None, {"showHeader": True, "cellHeight": "sm"}, {"decimals": 1}))
    y += 4
    v1 = verificacion_f7("Pérdida a 24 h: pronosticada vs observada (L)", "D6",
                         ["perdida_pred_l", "perdida_real_proxy_l"], 0, y, 12, 8,
                         "La pérdida pronosticada en una noche frente al exceso observado la noche siguiente × 24 h "
                         "(aproximación observable: no hay fugas reales etiquetadas).", "litre")
    v2 = verificacion_f7("Error de la pérdida a 24 h (L)", "D6", ["error_l"], 12, y, 12, 8,
                         "Observado − pronosticado. Cerca de 0 = acierto.", "litre")
    for v in (v1, v2):
        v["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p += [v1, v2]
    y += 8

    p.append(fila("Qué se infiere de cada vista", y)); y += 1
    p.append(texto("Lectura del tablero", INFERENCIA.strip(), 0, y, 24, 12))

    variables = [
        {"name": "origen", "label": "origenLecturas", "type": "custom",
         "query": "real : monitoraqua, aumentado : monitoraqua_aumentados",
         "current": {"text": "real", "value": "monitoraqua"},
         "options": [{"text": "real", "value": "monitoraqua", "selected": True},
                     {"text": "aumentado", "value": "monitoraqua_aumentados", "selected": False}]},
        {"name": "sensorId", "label": "sensorId", "type": "query", "datasource": INFLUX, "refresh": 2,
         "multi": True, "includeAll": True, "allValue": ".*",
         "current": {"text": ["All"], "value": ["$__all"]},
         "query": 'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "${origen}", tag: "sensorId", '
                  'predicate: (r) => r._measurement == "nivel_tanque", start: -120d)'},
    ]
    return {"uid": "monitoraqua-d6", "title": "D6 · Pérdida por fuga",
            "tags": ["monitoraqua", "d6", "bi"], "timezone": "America/Lima", "schemaVersion": 39,
            "version": 1, "editable": False, "graphTooltip": 1, "refresh": "10s",
            "time": {"from": "now-7d", "to": "now"},
            "templating": {"list": variables},
            "annotations": {"list": [anotacion("llegada_agua", "llegada_agua", "blue"),
                                     anotacion("alerta_fuga", "alerta_fuga", "red")]},
            "panels": p}


def main():
    t = tablero_d6()
    (AQUI / "d6_fugas.json").write_text(json.dumps(t, ensure_ascii=False, indent=1), encoding="utf-8")
    print("d6_fugas.json", len([x for x in t["panels"] if x["type"] not in ("row", "text")]), "paneles")


if __name__ == "__main__":
    main()
