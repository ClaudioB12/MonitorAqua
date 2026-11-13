"""
Genera el tablero de la dimensión D4 (llegada del agua y caudal de llenado) del sensor real, como JSON versionado.
Grafana lo carga por provisioning desde dashboards/, igual que los demás tableros del proyecto.

  python dashboards/generar_tablero_d4.py      escribe dashboards/d4_llegadas.json

Reutiliza las funciones de generar_tableros.py. No modifica los otros tableros.
Responsable de D4: Wilbert Alex Mayta Arotaype.

Fuentes (InfluxDB, escritas por el job de Spark):
  nivel_tanque      nivelPct, litros, distanciaCm y temperaturaC de cada lectura del ESP32
  eventos_servicio  tipo = llegada_agua (detector congelado f4.4): llegadas observadas y caudalLlenadoLpm
"""
import json

from generar_tableros import (AQUI, INFLUX, RANGO, SENSOR, anotacion, fila, flux, panel, prediccion_f7, prom,
                              texto, verificacion_f7)

LEYENDA = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
PUNTOS = {"custom": {"drawStyle": "points", "pointSize": 8}}
STAT = {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none",
        "graphMode": "none", "textMode": "value"}
SEMAFORO = {**STAT, "colorMode": "value"}
LLEGADAS = f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua"'''

INFERENCIA = """
**En vivo.** Si *Última lectura recibida hace* está en **verde** (menos de 1 min), toda la cadena funciona:
ESP32 → Firebase → puente → Kafka → Spark → InfluxDB → Grafana. En **rojo** (más de 5 min), el sensor dejó de
publicar: sin WiFi o sin energía. El nivel, los litros y la distancia son la última foto del tanque.

**Huecos del sensor.** La **franja gris** marca cuándo el ESP32 no envió lecturas (sin WiFi o sin energía). La **línea verde punteada** son **datos agregados**: su **reconstrucción rotulada** (días análogos del mismo sensor, `u3/reconstruir_huecos_esp32.py`): sirve para ver la serie continua, pero **no es una medición** y no entra en ninguna métrica.\n\n**Nivel del tanque.** Una **subida brusca** del nivel es una **llegada de agua** (marca azul): el servicio de la red
volvió. Una **bajada gradual** es el consumo de la casa. Los picos aislados hacia abajo son ecos falsos del sensor
ultrasónico, que el detector descarta con la mediana de 5 lecturas. Los tramos rectos y planos son huecos: el
sensor no envió datos.

**Llegadas detectadas e intervalo mediano.** Cuántas veces llegó el agua en el periodo y cada cuánto tiempo:
es el **ritmo del servicio** en ese tanque. Si el intervalo es estable, la próxima llegada se puede anticipar
(es lo que aprende el modelo d4.1 en el notebook).

**A qué hora del día llega.** La franja horaria en la que llega el agua. En Juliaca el suministro suele llegar de
madrugada; con pocas llegadas observadas, el gráfico es todavía indicativo.

**Caudal de llenado.** Qué tan rápido entra el agua (L/min = subida × capacidad / minutos). Un caudal bajo indica
baja presión de red. En el ESP32 es **aproximado**: la geometría del tanque es provisional (por medir).

**Predicción de la próxima llegada (modelo d4.1).** En el gráfico de nivel: **franja naranja** = ventana del 80 %,\n**marca roja** = hora más probable, **marca morada** = dónde se había pronosticado una llegada ya\nverificada (compárala con la marca azul real). La fila **Pronóstico vs real** da el error de cada\npronóstico, el MAE en vivo y el % de llegadas dentro de la ventana, igual que In [11] del notebook. Spark aplica el modelo cada 15 min: con las horas desde la
última llegada y la hora del día calcula la **hora más probable** de la próxima llegada, la **ventana del 80 %**
(P10-P90 calibrada) y la **probabilidad** de que llegue en 6 y 12 h. Si el agua no llega dentro de la ventana, es
señal de un corte. Con *origenLecturas = aumentado* y *sensorId = All* se ve la flota; en *real*, el tanque
ThingSpeak 1233810. Donde el modelo no aplica (ESP32 con pocas llegadas, tanque de lluvia) se muestra el **motivo**.
La validación y las métricas están en el notebook `notebooks/u2/d4_llegadas.ipynb`.

**U3 · Ejecución integrada.** La fila U3 muestra la última corrida de `u3/d4_ejecucion_integrada.py`: latencia de
ingesta del ESP32 y horas entre llegadas como **p50 y p95** (el promedio se ve solo para mostrar cuánto engaña), el
error del modelo en producción y la **antigüedad** de la corrida: si está en rojo, los valores son de hace días.
"""


def ultimo(campo):
    return f'''
from(bucket: "${{origen}}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "{campo}")
  |> {SENSOR}
  |> group()
  |> last()
'''


def anotacion_prediccion():
    """Ventana del 80 % de la última predicción d4.1 de cada tanque, como región sobre los gráficos de tiempo.
    Los *_ts de 'predicciones' están en milisegundos (streaming/inferencia.py)."""
    q = '''
from(bucket: "${origen}")
  |> range(start: -60d)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "estimacion")
  |> ''' + SENSOR + '''
  |> filter(fn: (r) => r._field == "objetivo_ts" or r._field == "ventana_ini_ts" or r._field == "ventana_fin_ts" or r._field == "horas_moda" or r._field == "prob_6h")
  |> group(columns: ["sensorId", "_field"])
  |> last()
  |> keep(columns: ["_time", "sensorId", "_field", "_value"])
  |> group()
  |> pivot(rowKey: ["sensorId", "_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
      _time: time(v: int(v: r.ventana_ini_ts) * 1000000),
      timeEnd: time(v: int(v: r.ventana_fin_ts) * 1000000),
      text: "Predicción d4.1 · " + r.sensorId + " · llegada más probable en " + string(v: r.horas_moda)
            + " h · ventana 80 % · P(llega en 6 h) = " + string(v: r.prob_6h),
      tags: "prediccion"}))
'''
    return {"name": "Predicción d4.1 (ventana 80 %)", "datasource": INFLUX, "enable": True, "iconColor": "orange",
            "target": flux(q),
            "mappings": {"time": {"source": "field", "value": "_time"},
                         "timeEnd": {"source": "field", "value": "timeEnd"},
                         "text": {"source": "field", "value": "text"},
                         "tags": {"source": "field", "value": "tags"}}}


def anotacion_mas_probable():
    """Hora más probable (moda) de la última predicción d4.1 de cada tanque."""
    q = '''
from(bucket: "${origen}")
  |> range(start: -60d)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "estimacion")
  |> ''' + SENSOR + '''
  |> filter(fn: (r) => r._field == "objetivo_ts")
  |> group(columns: ["sensorId"])
  |> last()
  |> group()
  |> map(fn: (r) => ({_time: time(v: int(v: r._value) * 1000000),
      text: "Llegada más probable (d4.1) · " + r.sensorId, tags: "prediccion"}))
'''
    return {"name": "Llegada más probable (d4.1)", "datasource": INFLUX, "enable": True, "iconColor": "red",
            "target": flux(q)}


def anotacion_sin_conexion():
    """Franja gris donde el ESP32 no envió lecturas por más de 30 min: el hueco queda a la vista como un hecho, no
    como un error del tablero (la línea punteada, si existe, es su reconstrucción rotulada)."""
    q = f'''
from(bucket: "monitoraqua")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "nivelPct" and r.sensorId == "esp32-tanque-real-01")
  |> aggregateWindow(every: 1m, fn: last, createEmpty: false)
  |> elapsed(unit: 1m)
  |> filter(fn: (r) => r.elapsed > 30)
  |> map(fn: (r) => ({{_time: time(v: int(v: r._time) - r.elapsed * 60000000000), timeEnd: r._time,
      text: "Sensor sin conexión: " + string(v: r.elapsed / 60) + " h " + string(v: r.elapsed % 60) + " min sin lecturas",
      tags: "sin_conexion"}}))
'''
    return {"name": "Sensor sin conexión", "datasource": INFLUX, "enable": True, "iconColor": "#8E9AA6",
            "target": flux(q),
            "mappings": {"time": {"source": "field", "value": "_time"},
                         "timeEnd": {"source": "field", "value": "timeEnd"},
                         "text": {"source": "field", "value": "text"},
                         "tags": {"source": "field", "value": "tags"}}}


def anotacion_pronosticos_pasados():
    """Dónde había pronosticado el modelo cada llegada ya verificada (para compararla con la marca azul real)."""
    q = f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "verificacion")
  |> {SENSOR}
  |> filter(fn: (r) => r._field == "llegada_pred_ts" or r._field == "error_h")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> map(fn: (r) => ({{_time: time(v: int(v: r.llegada_pred_ts) * 1000000),
      text: "Pronóstico verificado · " + r.sensorId + " · error " + string(v: r.error_h) + " h",
      tags: "pronostico"}}))
'''
    return {"name": "Pronósticos pasados (verificados)", "datasource": INFLUX, "enable": True,
            "iconColor": "purple", "target": flux(q)}


def tablero_d4():
    p, y = [], 0

    p.append(fila("En vivo · sensor real", y)); y += 1
    p.append(panel("stat", "Última lectura recibida hace", 0, y, 4, 4, [flux('''
from(bucket: "${origen}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "nivelPct")
  |> ''' + SENSOR + '''
  |> group()
  |> last()
  |> map(fn: (r) => ({_time: r._time, _value: float(v: int(v: now()) - int(v: r._time)) / 1000000000.0}))
''')], "Segundos desde la última lectura. Verde: llega en vivo (el ESP32 publica cada 5 s); rojo: más de 5 min "
          "sin datos.", "s", SEMAFORO,
        {"decimals": 0, "color": {"mode": "thresholds"},
         "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                      {"color": "orange", "value": 60},
                                                      {"color": "red", "value": 300}]}},
        sin_datos="Sin lecturas"))
    p.append(panel("stat", "Nivel actual", 4, y, 4, 4, [flux(ultimo("nivelPct"))],
                   "Último nivel calculado con la geometría del tanque (provisional).", "percent", STAT,
                   {"decimals": 1}, sin_datos="Sin lecturas"))
    p.append(panel("stat", "Litros actuales", 8, y, 4, 4, [flux(ultimo("litros"))],
                   "Litros = nivel × capacidad (1100 L, provisional).", "litre", STAT, {"decimals": 0},
                   sin_datos="Sin lecturas"))
    p.append(panel("stat", "Distancia medida", 12, y, 4, 4, [flux(ultimo("distanciaCm"))],
                   "Del sensor a la superficie del agua, compensada por temperatura en el firmware. Menos de 25 cm "
                   "= zona ciega del JSN-SR04T.", "suffix: cm", STAT, {"decimals": 1}, sin_datos="Sin lecturas"))
    p.append(panel("stat", "Temperatura del aire", 16, y, 4, 4, [flux(ultimo("temperaturaC"))],
                   "Aire de la cámara del tanque (DHT11, ±2 °C).", "celsius", STAT, {"decimals": 1},
                   sin_datos="Sin lecturas"))
    p.append(panel("stat", "Última llegada de agua", 20, y, 4, 4, [flux('''
from(bucket: "${origen}")
  |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua" and r._field == "valor")
  |> ''' + SENSOR + '''
  |> group()
  |> last()
  |> map(fn: (r) => ({_time: r._time, _value: float(v: int(v: r._time)) / 1000000.0}))
''')], "Hace cuánto se detectó la última llegada (últimos 30 días).", "dateTimeFromNow", STAT,
        sin_datos="Sin llegadas en 30 días"))
    y += 4

    p.append(fila("Llegadas de agua en el periodo elegido", y)); y += 1
    p.append(panel("stat", "Llegadas detectadas", 0, y, 8, 4, [flux(LLEGADAS + ''' and r._field == "valor")
  |> ''' + SENSOR + '''
  |> group()
  |> count()
''')], "Eventos llegada_agua del detector en el periodo.", "none", STAT, sin_datos="Sin llegadas"))
    p.append(panel("stat", "Intervalo mediano entre llegadas", 8, y, 8, 4, [flux(LLEGADAS + ''' and r._field == "valor")
  |> ''' + SENSOR + '''
  |> group(columns: ["sensorId"])
  |> sort(columns: ["_time"])
  |> elapsed(unit: 1m)
  |> map(fn: (r) => ({_time: r._time, _value: float(v: r.elapsed) / 60.0}))
  |> group()
  |> median()
''')], "Mediana de las horas entre dos llegadas seguidas.", "h", STAT, {"decimals": 1},
        sin_datos="Se necesitan 2 llegadas"))
    p.append(panel("stat", "Caudal de llenado mediano", 16, y, 8, 4, [flux(LLEGADAS + ''' and r._field == "caudalLlenadoLpm")
  |> ''' + SENSOR + '''
  |> group()
  |> median()
''')], "Mediana del caudal estimado de cada llegada. En el ESP32 es aproximado (geometría provisional).", "L/min",
        STAT, {"decimals": 1}, sin_datos="Sin caudal"))
    y += 4

    p.append(fila("Lo observado", y)); y += 1
    p.append(panel("timeseries", "Nivel del tanque (%) con las llegadas marcadas", 0, y, 16, 9, [flux(f'''
from(bucket: "${{origen}}")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "nivelPct")
  |> {SENSOR}
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "sensorId"])
'''), flux(f'''
from(bucket: "monitoraqua_aumentados")
  |> {RANGO}
  |> filter(fn: (r) => r._measurement == "nivel_tanque" and r._field == "nivelPct")
  |> filter(fn: (r) => r.sensorId =~ /^(${{sensorId:regex}})-reconstruido$/)
  |> aggregateWindow(every: v.windowPeriod, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value", "sensorId"])
''', "B")], "Subida brusca = llegada de agua (marca azul); bajada gradual = consumo. Línea verde PUNTEADA = "
         "DATOS AGREGADOS: RECONSTRUCCIÓN rotulada de los huecos del sensor (no es una medición; u3/reconstruir_huecos_esp32.py); "
         "franja gris = sensor sin conexión. Franja naranja = ventana del 80 % de la próxima llegada predicha por "
         "d4.1; marca roja = hora más probable; marca morada = dónde se había pronosticado una llegada ya "
         "verificada.", "percent", LEYENDA,
        {"min": 0, "max": 100}, sin_datos="Sin nivel en el periodo"))
    # Sin esto Grafana une con una recta la última lectura antes de un hueco y la primera después (parecía que el
    # sensor midió un nivel constante durante días). Con insertNulls la línea medida se corta en huecos de más de 1 h:
    # ahí queda la franja gris "Sensor sin conexión" y, si se cargó, la reconstrucción punteada.
    p[-1]["fieldConfig"]["defaults"]["custom"] = {"insertNulls": 3600000, "spanNulls": False}
    p[-1]["fieldConfig"]["overrides"] = [{
        "matcher": {"id": "byRegexp", "options": ".*reconstruido.*"},
        "properties": [{"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}},
                       {"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}},
                       {"id": "displayName", "value": "${__field.labels.sensorId} (datos agregados, no medido)"}]}]
    p.append(panel("barchart", "A qué hora del día llega el agua", 16, y, 8, 9, [flux(LLEGADAS + ''' and r._field == "valor")
  |> ''' + SENSOR + '''
  |> map(fn: (r) => ({hora: (int(v: r._time) / 3600000000000 - 5) % 24, _value: 1}))
  |> group(columns: ["hora"])
  |> sum()
  |> group()
  |> sort(columns: ["hora"])
  |> map(fn: (r) => ({hora: string(v: r.hora) + " h", llegadas: r._value}))
''')], "Llegadas por hora de Lima (UTC-5).", "none",
        {"xField": "hora", "orientation": "vertical", "showValue": "auto", "legend": {"showLegend": False}}))
    y += 9
    p.append(panel("table", "Llegadas detectadas", 0, y, 16, 7, [flux(LLEGADAS + ''')
  |> ''' + SENSOR + '''
  |> filter(fn: (r) => r._field == "valor" or r._field == "caudalLlenadoLpm" or r._field == "duracionMin")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()
  |> keep(columns: ["_time", "sensorId", "valor", "duracionMin", "caudalLlenadoLpm", "severidad"])
  |> rename(columns: {_time: "hora", valor: "subida_pct", duracionMin: "duracion_min", caudalLlenadoLpm: "caudal_L_min"})
  |> sort(columns: ["hora"], desc: true)
''')], "Cada llegada: cuánto subió el nivel (% de la capacidad), en cuántos minutos y con qué caudal.", None,
        {"showHeader": True, "cellHeight": "sm"}))
    p.append(panel("timeseries", "Horas entre llegadas", 16, y, 8, 7, [flux(LLEGADAS + ''' and r._field == "valor")
  |> ''' + SENSOR + '''
  |> group(columns: ["sensorId"])
  |> sort(columns: ["_time"])
  |> elapsed(unit: 1m)
  |> map(fn: (r) => ({_time: r._time, sensorId: r.sensorId, _value: float(v: r.elapsed) / 60.0}))
''')], "Horas desde la llegada anterior.", "h", LEYENDA, PUNTOS, sin_datos="Se necesitan 2 llegadas"))
    y += 7

    p.append(fila("Predicción · próxima llegada del agua (modelo d4.1)", y)); y += 1
    p.append(panel("table", "Hora estimada de la próxima llegada", 0, y, 14, 7, [flux('''
from(bucket: "${origen}")
  |> range(start: -60d)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "estimacion")
  |> ''' + SENSOR + '''
  |> filter(fn: (r) => r._field == "horas_moda" or r._field == "ventana_ini_h" or r._field == "ventana_fin_h" or r._field == "prob_6h" or r._field == "prob_12h")
  |> group(columns: ["sensorId", "_field"])
  |> last()
  |> keep(columns: ["_time", "sensorId", "_field", "_value"])
  |> group()
  |> pivot(rowKey: ["sensorId", "_time"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
      sensorId: r.sensorId,
      calculada: r._time,
      llegada_probable: time(v: int(v: r._time) + int(v: float(v: r.horas_moda) * 3600000000000.0)),
      ventana_desde: time(v: int(v: r._time) + int(v: float(v: r.ventana_ini_h) * 3600000000000.0)),
      ventana_hasta: time(v: int(v: r._time) + int(v: float(v: r.ventana_fin_h) * 3600000000000.0)),
      prob_6h: float(v: r.prob_6h),
      prob_12h: float(v: r.prob_12h)}))
''')], "Última predicción de cada tanque: hora de cálculo + horas hasta la llegada más probable (moda) y la "
          "ventana del 80 % (P10-P90 calibrada con conformal). prob_6h / prob_12h: probabilidad de que llegue en "
          "las próximas 6 / 12 h.", None, {"showHeader": True, "cellHeight": "sm"},
        {"decimals": 2}, [{"id": "organize", "options": {"indexByName": {
            "sensorId": 0, "llegada_probable": 1, "ventana_desde": 2, "ventana_hasta": 3, "prob_6h": 4,
            "prob_12h": 5, "calculada": 6}}}]))
    det = prediccion_f7("Detalle del modelo y motivo", "D4",
                        ["horas_moda", "ventana_ini_h", "ventana_fin_h", "prob_6h", "prob_12h", "prob_24h"],
                        14, y, 10, 7,
                        "D4 (d4.1): horas hasta la llegada más probable, ventana P10-P90 calibrada y probabilidad de "
                        "llegada en 6/12/24 h. Si no aplica (tanque de lluvia, sin modelo), el motivo.")
    det["targets"][0]["query"] = det["targets"][0]["query"].replace(RANGO, "range(start: -60d)")
    p.append(det)
    y += 7

    # Pronóstico vs real: lo mismo que la celda In [11] del notebook (MAE en vivo por tanque)
    VERIF = '''
from(bucket: "${origen}")
  |> ''' + RANGO + '''
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "verificacion")
  |> ''' + SENSOR
    ABS = '''
  |> filter(fn: (r) => r._field == "error_h")
  |> map(fn: (r) => ({r with _value: if float(v: r._value) < 0.0 then -float(v: r._value) else float(v: r._value)}))'''
    p.append(fila("Pronóstico vs real · ¿acertó el modelo? (como In [11] del notebook)", y)); y += 1
    p.append(panel("stat", "Pronósticos verificados", 0, y, 6, 4, [flux(VERIF + ABS + '''
  |> group()
  |> count()
''')], "Cuántas predicciones ya tienen su llegada real para compararlas.", "none", STAT,
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Error promedio en vivo (MAE)", 6, y, 6, 4, [flux(VERIF + ABS + '''
  |> group()
  |> mean()
''')], "Promedio de |hora real − hora pronosticada|, en horas.", "h", STAT, {"decimals": 2},
        sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("stat", "Llegadas dentro de la ventana del 80 %", 12, y, 6, 4, [flux(VERIF + '''
  |> filter(fn: (r) => r._field == "dentro_ventana")
  |> map(fn: (r) => ({r with _value: if r._value == true then 100.0 else 0.0}))
  |> group()
  |> mean()
''')], "Porcentaje de llegadas reales que cayeron dentro de la ventana predicha (meta: 80 %).", "percent", STAT,
        {"decimals": 1}, sin_datos="Sin verificaciones en el periodo"))
    p.append(panel("table", "Error por tanque", 18, y, 6, 4, [flux(VERIF + ABS + '''
  |> group(columns: ["sensorId"])
  |> reduce(identity: {verificados: 0, suma: 0.0},
            fn: (r, accumulator) => ({verificados: accumulator.verificados + 1, suma: accumulator.suma + r._value}))
  |> map(fn: (r) => ({sensorId: r.sensorId, verificados: r.verificados, MAE_h: r.suma / float(v: r.verificados)}))
  |> group()
''')], "MAE en vivo por tanque (igual que la tabla de In [11]).", None, {"showHeader": True, "cellHeight": "sm"},
        {"decimals": 2}))
    y += 4
    p.append(panel("table", "Cada pronóstico frente a la llegada real", 0, y, 14, 8, [flux(VERIF + '''
  |> filter(fn: (r) => r._field == "llegada_real_ts" or r._field == "llegada_pred_ts" or r._field == "error_h" or r._field == "dentro_ventana")
  |> keep(columns: ["_time", "sensorId", "_field", "_value"])
  |> map(fn: (r) => ({r with _value: string(v: r._value)}))
  |> group()
  |> pivot(rowKey: ["_time", "sensorId"], columnKey: ["_field"], valueColumn: "_value")
  |> map(fn: (r) => ({
      sensorId: r.sensorId,
      pronosticada: time(v: int(v: float(v: r.llegada_pred_ts)) * 1000000),
      llego: time(v: int(v: float(v: r.llegada_real_ts)) * 1000000),
      error_h: float(v: r.error_h),
      dentro_ventana: r.dentro_ventana}))
  |> sort(columns: ["llego"], desc: true)
''')], "Una fila por llegada verificada: cuándo dijo el modelo que llegaría, cuándo llegó y el error en horas "
          "(positivo = llegó más tarde de lo pronosticado).", None, {"showHeader": True, "cellHeight": "sm"},
        {"decimals": 2}))
    err = verificacion_f7("Error de cada pronóstico (h)", "D4", ["error_h"], 14, y, 10, 8,
                          "Cada punto es una llegada verificada: error = hora real − hora pronosticada. Cerca de 0 = "
                          "acierto; los puntos lejos de 0 son los días de corte (llegó al día siguiente).", "h")
    err["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId}"
    p.append(err)
    y += 8

    # U3 (DataOps): resultados de la última ejecución integrada de D4 (u3/d4_ejecucion_integrada.py), publicados en el
    # Pushgateway (job monitoraqua_d4) y leídos de Prometheus. Percentiles en vez del promedio (S13, 3.4).
    p.append(fila("U3 · Ejecución integrada de D4 (DataOps: compuertas, percentiles, manifiesto)", y)); y += 1
    G = 'job="monitoraqua_d4"'
    p.append(panel("stat", "Latencia de ingesta del ESP32: p50 frente a p95", 0, y, 6, 5, [
        prom(f"monitoraqua_d4_latencia_ingesta_esp32_p50_s{{{G}}}", "p50 (lectura típica)", "A"),
        prom(f"monitoraqua_d4_latencia_ingesta_esp32_p95_s{{{G}}}", "p95 (las más lentas)", "B"),
        prom(f"monitoraqua_d4_latencia_ingesta_esp32_promedio_s{{{G}}}", "promedio (engaña)", "C")],
        "Segundos entre la medición del ESP32 y su llegada al puente. El p50 es lo que vive una lectura típica; "
        "el p95, las atrasadas por reconexiones. El promedio se muestra solo para ver cuánto engaña.", "s",
        {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none",
         "graphMode": "none", "textMode": "value_and_name"}, {"decimals": 1}, sin_datos="Sin ejecución integrada"))
    p.append(panel("stat", "Horas entre llegadas: p50 frente a p95", 6, y, 6, 5, [
        prom(f"monitoraqua_d4_intervalo_llegadas_p50_h{{{G}}}", "p50 (espera típica)", "A"),
        prom(f"monitoraqua_d4_intervalo_llegadas_p95_h{{{G}}}", "p95 (cortes)", "B")],
        "KPI de tiempo de D4. Compuerta de plausibilidad: p50 entre 6 y 48 h (servicio diario intermitente).", "h",
        {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}, "colorMode": "none",
         "graphMode": "none", "textMode": "value_and_name"}, {"decimals": 1}, sin_datos="Sin ejecución integrada"))
    p.append(panel("stat", "Error del modelo en producción (flota)", 12, y, 4, 5, [
        prom(f"monitoraqua_d4_mae_vivo_flota_h{{{G}}}", "MAE en vivo", "A")],
        "MAE en vivo de la hora de llegada (F7) en la flota. Debe repetirse corrida a corrida: si cambia, es una "
        "regresión.", "h", STAT, {"decimals": 2}, sin_datos="Sin ejecución integrada"))
    p.append(panel("stat", "Lecturas procesadas por el stream", 16, y, 4, 5, [
        prom(f"monitoraqua_d4_stream_filas_total{{{G}}}", "filas", "A")],
        "Filas que entraron en la ventana fija de streaming de la última ejecución integrada.", "none", STAT,
        sin_datos="Sin ejecución integrada"))
    p.append(panel("stat", "Antigüedad de la última ejecución", 20, y, 4, 5, [
        prom(f"time() - monitoraqua_d4_ultima_ejecucion_timestamp{{{G}}}", "antigüedad", "A")],
        "Hace cuánto corrió la ejecución integrada. El Pushgateway conserva el último valor: sin este panel, una "
        "ejecución de hace días parece viva.", "s", SEMAFORO,
        {"decimals": 0, "color": {"mode": "thresholds"},
         "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None},
                                                      {"color": "orange", "value": 86400},
                                                      {"color": "red", "value": 259200}]}},
        sin_datos="Sin ejecución integrada"))
    y += 5

    p.append(fila("Qué se infiere de cada vista", y)); y += 1
    p.append(texto("Lectura del tablero", INFERENCIA.strip(), 0, y, 24, 12))

    for x in p:
        if x.get("title") in ("Nivel del tanque (%) con las llegadas marcadas", "Horas entre llegadas"):
            x["fieldConfig"]["defaults"]["displayName"] = "${__field.labels.sensorId}"

    variables = [
        {"name": "origen", "label": "origenLecturas", "type": "custom",
         "query": "real : monitoraqua, aumentado : monitoraqua_aumentados",
         "current": {"text": "real", "value": "monitoraqua"},
         "options": [{"text": "real", "value": "monitoraqua", "selected": True},
                     {"text": "aumentado", "value": "monitoraqua_aumentados", "selected": False}]},
        {"name": "sensorId", "label": "sensorId", "type": "query", "datasource": INFLUX, "refresh": 2,
         "multi": True, "includeAll": True, "allValue": ".*",
         "current": {"text": ["esp32-tanque-real-01"], "value": ["esp32-tanque-real-01"]},
         "query": 'import "influxdata/influxdb/schema"\nschema.tagValues(bucket: "${origen}", tag: "sensorId", '
                  'predicate: (r) => r._measurement == "nivel_tanque", start: -120d)'},
    ]
    return {"uid": "monitoraqua-d4", "title": "D4 · Llegada del agua (sensor real)",
            "tags": ["monitoraqua", "d4", "bi"], "timezone": "America/Lima", "schemaVersion": 39,
            "version": 10, "editable": False, "graphTooltip": 1, "refresh": "10s",
            "time": {"from": "now-7d", "to": "now+1d"},
            "templating": {"list": variables},
            "annotations": {"list": [anotacion("llegada_agua", "llegada_agua", "blue"), anotacion_prediccion(),
                                     anotacion_mas_probable(), anotacion_pronosticos_pasados(),
                                     anotacion_sin_conexion()]},
            "panels": p}


def main():
    t = tablero_d4()
    (AQUI / "d4_llegadas.json").write_text(json.dumps(t, ensure_ascii=False, indent=1), encoding="utf-8")
    print("d4_llegadas.json", len([x for x in t["panels"] if x["type"] not in ("row", "text")]), "paneles")


if __name__ == "__main__":
    main()