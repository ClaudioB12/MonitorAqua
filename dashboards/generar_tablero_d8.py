"""
Genera el tablero de la dimensión D8 (temperatura del agua estimada, cruce del umbral y residencia) como JSON versionado.
Grafana lo carga por provisioning desde dashboards/, igual que los demás tableros del proyecto.

  python dashboards/generar_tablero_d8.py      escribe dashboards/d8_temperatura.json

Reutiliza las funciones de generar_tableros.py. No modifica los otros tableros.
Responsable de D8: Josué Gabriel Ochoa Mamani.

Fuentes (InfluxDB, escritas por el job de Spark):
  nivel_tanque      temperaturaC (aire de la cámara, DHT11)
  lecturas_agg      sensorId = open-meteo-juliaca, temperaturaC_media (aire exterior de Juliaca)
  eventos_servicio  tipo = umbral_termico (detector f4.4) y llegada_agua (anotación: un llenado renueva el agua)
  predicciones      dimension = D8: estimacion (agua ahora y a 6/12 h, cruce, residencia; modelo d8.1),
                    verificacion (agua real de ThingSpeak 3418755 frente a lo pronosticado), motivo
"""
import json

from generar_tableros import (AQUI, INFLUX, RANGO, SENSOR, anotacion, fila, flux, panel, prediccion_f7, texto,
                              verificacion_f7)

LEYENDA = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
STAT = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none",
        "graphMode": "none", "textMode": "value"}
SEMAFORO = {**STAT, "colorMode": "value"}
ESTIMACION = ' and r.dimension == "D8" and r.tipo == "estimacion"'
ABS = '''
  |> map(fn: (r) => ({r with _value: if float(v: r._value) < 0.0 then -float(v: r._value) else float(v: r._value)}))'''

INFERENCIA = """
**En vivo.** El DHT11 mide el **aire de la cámara** del tanque, no el agua. Open-Meteo da el aire exterior de Juliaca.
Si *Última lectura recibida hace* está en **verde**, la cadena ESP32 → Firebase → puente → Kafka → Spark → InfluxDB →
Grafana funciona.

**Agua estimada.** No hay sensor de agua en Juliaca: el modelo d8.1 la **estima** como una inercia de primer orden
desde el aire de la cámara y el aire de Open-Meteo (τ = 24,3 h): el agua sigue al aire con horas de atraso y menos
amplitud. La **banda P10-P90** es el rango probable. Error de transferencia esperado: 1-2 °C, porque el recipiente del
ESP32 es mucho más pequeño que el tanque donde se validó.

**Cruce del umbral.** Probabilidad de que el agua baje de **4 °C** o suba de **25 °C** en las próximas 12 h. La marca roja
es **umbral_termico** del detector f4.4.

**Residencia.** Edad media del agua en el tanque (horas), con los llenados de D4: cada llegada (marca azul) renueva el
agua y baja la edad.

**Pronóstico vs real.** Solo ThingSpeak 3418755 mide el agua de verdad (DS18B20): ahí se comparan los pronósticos a 6 y
12 h con el agua real. En la prueba de F6 el error fue 0,24 °C a 6 h y 0,41 °C a 12 h.

**Modelo.** d8.1 en `batch/modelo_d8.py` y `batch/termico.py`; validación en `docs/modelo-d8.md`. Si no aplica, la tabla
muestra el **motivo**.
"""


def ultimo(medida, campo, extra="", sensor=True):
    """Último valor de un campo en los últimos 30 días (extra = condición adicional del filtro)."""
    filtro = f"\n  |> {SENSOR}" if sensor else ""
    return f'''
from(bucket: "${{origen}}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "{medida}" and r._field == "{campo}"{extra}){filtro}
  |> group()
  |> last()
'''


def tablero_d8():
    p, y = [], 0

    p.append(fila("En vivo · aire, agua estimada y residencia", y)); y += 1
    p.append(panel("stat", "Última lectura recibida hace", 0, y, 4, 4, [flux('''
from(bucket: "${origen}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "temperaturaC")
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
    p.append(panel("stat", "Aire de la cámara (DHT11)", 4, y, 4, 4, [flux(ultimo("nivel_tanque", "temperaturaC"))],
                   "Temperatura del aire dentro del tanque (±2 °C).", "celsius", STAT, {"decimals": 1},
                   sin_datos="Sin lecturas"))
    p.append(panel("stat", "Aire de Juliaca (Open-Meteo)", 8, y, 4, 4, [flux(ultimo(
        "lecturas_agg", "temperaturaC_media", ' and r.sensorId == "open-meteo-juliaca"', sensor=False))],
                   "Temperatura exterior de Juliaca (Open-Meteo, solo en origen = real).", "celsius", STAT,
                   {"decimals": 1}, sin_datos="Sin datos de Open-Meteo"))
    p.append(panel("stat", "Agua estimada ahora", 12, y, 4, 4, [flux(ultimo("predicciones", "agua_c", ESTIMACION))],
                   "Temperatura del agua estimada por d8.1 (sin sensor de agua; error de transferencia 1-2 °C).",
                   "celsius", SEMAFORO, {"decimals": 1, "color": {"mode": "thresholds"},
                                         "thresholds": {"mode": "absolute", "steps": [
                                             {"color": "blue", "value": None}, {"color": "green", "value": 4},
                                             {"color": "red", "value": 25}]}},
                   sin_datos="Sin estimación (ver motivo)"))
    p.append(panel("stat", "Edad del agua (residencia)", 16, y, 4, 4,
                   [flux(ultimo("predicciones", "residencia_h", ESTIMACION))],
                   "Edad media del agua del tanque, renovada por cada llenado.", "h", STAT, {"decimals": 1},
                   sin_datos="Edad desconocida (falta un llenado)"))
    p.append(panel("stat", "Alertas de umbral térmico", 20, y, 4, 4, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "umbral_termico" and r._field == "valor")
  |> {SENSOR}
  |> group()
  |> count()
''')], "Eventos umbral_termico del detector f4.4 en el periodo elegido.", "none", STAT, sin_datos="0"))
    y += 4

    p.append(fila("Lo observado y lo estimado", y)); y += 1
    temps = panel("timeseries", "Aire de la cámara, aire de Juliaca y agua estimada (con su banda)", 0, y, 16, 9, [
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "temperaturaC")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _field: "aire cámara (DHT11)"}}))
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''', "A"),
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "lecturas_agg" and r.sensorId == "open-meteo-juliaca" and r._field == "temperaturaC_media")
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> map(fn: (r) => ({{r with _field: "Juliaca (Open-Meteo)"}}))
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''', "B"),
        flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "estimacion")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "agua_c" or r._field == "agua_p10_c" or r._field == "agua_p90_c")
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''', "C")],
        "El agua sigue al aire con horas de atraso y menos amplitud. agua_c = estimación de d8.1; agua_p10_c y "
        "agua_p90_c = su banda. Marca azul = llenado; marca roja = umbral_termico.", "celsius", LEYENDA,
        sin_datos="Sin temperaturas en el periodo")
    temps["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p.append(temps)
    res = panel("timeseries", "Edad del agua (residencia, h)", 16, y, 8, 9, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "estimacion" and r._field == "residencia_h")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
  |> keep(columns: ["_time", "_value", "sensorId"])
''')], "Sube mientras no llega agua y baja con cada llenado (marca azul).", "h", LEYENDA,
        sin_datos="Sin residencia en el periodo")
    res["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId}"
    p.append(res)
    y += 9

    p.append(fila("Predicción · agua a 6 y 12 h y cruce del umbral (modelo d8.1)", y)); y += 1
    cruce = panel("timeseries", "Probabilidad de cruzar 4 °C o 25 °C en 12 h", 0, y, 10, 8, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "estimacion")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "prob_cruce_bajo_12h" or r._field == "prob_cruce_alto_12h")
  |> aggregateWindow(every: v.windowPeriod, fn: last, createEmpty: false)
  |> keep(columns: ["_time", "_value", "_field", "sensorId"])
''')], "prob_cruce_bajo_12h: que el agua baje de 4 °C; prob_cruce_alto_12h: que suba de 25 °C.", "percentunit",
        LEYENDA, {"min": 0, "max": 1}, sin_datos="Sin estimaciones en el periodo")
    cruce["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p.append(cruce)
    p.append(prediccion_f7("Última estimación por tanque (y motivo si no aplica)", "D8",
                           ["agua_c", "agua_p10_c", "agua_p90_c", "agua_6h_c", "agua_12h_c", "agua_12h_p10_c",
                            "agua_12h_p90_c", "prob_cruce_bajo_12h", "prob_cruce_alto_12h", "residencia_h",
                            "advertencia"], 10, y, 14, 8,
                           "D8 (d8.1): agua ahora y a 6/12 h con banda P10-P90, probabilidad de cruzar 4 °C o 25 °C en "
                           "12 h y residencia. Estimación sin sensor de agua: error de transferencia de 1-2 °C."))
    y += 8

    p.append(fila("Pronóstico vs real · ¿acertó el modelo? (agua real: ThingSpeak 3418755)", y)); y += 1
    VERIF = f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "verificacion" and r._field == "error_c")
  |> {SENSOR}'''
    p.append(panel("stat", "Pronósticos verificados", 0, y, 6, 4, [flux(VERIF + '''
  |> group()
  |> count()
''')], "Pronósticos a 6 y 12 h que ya tienen el agua real medida (solo donde hay sensor de agua).", "none", STAT,
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Error medio del agua (MAE)", 6, y, 6, 4, [flux(VERIF + ABS + '''
  |> group()
  |> mean()
''')], "Promedio de |agua real − pronosticada| en °C.", "celsius", STAT, {"decimals": 2},
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Sesgo (real − pronosticado)", 12, y, 6, 4, [flux(VERIF + '''
  |> group()
  |> mean()
''')], "Positivo: el agua real quedó más caliente que el pronóstico.", "celsius", STAT, {"decimals": 2},
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("table", "Error por tanque", 18, y, 6, 4, [flux(VERIF + ABS + '''
  |> group(columns: ["sensorId"])
  |> reduce(identity: {verificados: 0, suma: 0.0},
            fn: (r, accumulator) => ({verificados: accumulator.verificados + 1, suma: accumulator.suma + r._value}))
  |> map(fn: (r) => ({sensorId: r.sensorId, verificados: r.verificados, MAE_c: r.suma / float(v: r.verificados)}))
  |> group()
''')], "MAE del agua por tanque (6 y 12 h juntos).", None, {"showHeader": True, "cellHeight": "sm"}, {"decimals": 2}))
    y += 4
    v1 = verificacion_f7("Agua: real vs pronosticada (6 y 12 h)", "D8", ["agua_real_c", "agua_pred_c"], 0, y, 12, 8,
                         "Solo ThingSpeak 3418755 mide el agua (DS18B20): sus pronósticos a 6 y 12 h frente al agua "
                         "real.", "celsius")
    v2 = verificacion_f7("Error del agua (°C)", "D8", ["error_c"], 12, y, 12, 8,
                         "Real − pronosticado. Cerca de 0 = acierto.", "celsius")
    for v in (v1, v2):
        v["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId} · ${__field.name}"
    p += [v1, v2]
    y += 8

    p.append(fila("Qué se infiere de cada vista", y)); y += 1
    p.append(texto("Lectura del tablero", INFERENCIA.strip(), 0, y, 24, 13))

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
    return {"uid": "monitoraqua-d8", "title": "D8 · Temperatura y residencia del agua",
            "tags": ["monitoraqua", "d8", "bi"], "timezone": "America/Lima", "schemaVersion": 39,
            "version": 1, "editable": False, "graphTooltip": 1, "refresh": "10s",
            "time": {"from": "now-7d", "to": "now"},
            "templating": {"list": variables},
            "annotations": {"list": [anotacion("llegada_agua", "llegada_agua", "blue"),
                                     anotacion("umbral_termico", "umbral_termico", "red")]},
            "panels": p}


def main():
    t = tablero_d8()
    (AQUI / "d8_temperatura.json").write_text(json.dumps(t, ensure_ascii=False, indent=1), encoding="utf-8")
    print("d8_temperatura.json", len([x for x in t["panels"] if x["type"] not in ("row", "text")]), "paneles")


if __name__ == "__main__":
    main()
