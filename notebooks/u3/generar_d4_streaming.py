"""Genera notebooks/u3/d4_streaming.ipynb (capa de velocidad de D4, rúbrica U2/S12).
Regenerar: python notebooks/u3/generar_d4_streaming.py"""
from pathlib import Path

import nbformat as nbf

AQUI = Path(__file__).resolve().parent
celdas = []


def md(texto):
    celdas.append(nbf.v4.new_markdown_cell(texto.strip()))


def code(texto):
    celdas.append(nbf.v4.new_code_cell(texto.strip()))


md("""
# D4 · Capa de velocidad: Kafka, Spark Structured Streaming, observabilidad e inferencia en vivo

**Dimensión D4 · Próxima llegada del agua** · Wilbert Alex Mayta Arotaype · Equipo Horeb · MonitorAqua

La capa batch (`d4_batch.ipynb`) responde con toda la historia, pero tarde. Esta capa responde **en segundos** con lo
que va llegando. El notebook sigue la secuencia de la evaluación de la Unidad II (S12):

| # | Sección | Criterio de la rúbrica U2 |
|---|---|---|
| 1 | Kafka: tópico, particiones, offsets y clave | 1-2 (ingesta) |
| 2 | Evento **IoT** (ESP32): contrato, validación de esquema y de rango físico | 2 |
| 3 | Evento **empresarial** (`servicio-eventos`): contrato y consumo | 1 |
| 4 | Spark Structured Streaming en vivo: **ventana, watermark y checkpoint** | 3 |
| 5 | Checkpoint verificado: el stream se reanuda donde quedó | 3 |
| 5b | **El modelo d4.1 aplicado en cada micro-lote** (el mismo de producción) | 7 |
| 6 | Observabilidad: lag del consumidor, throughput y disponibilidad (Prometheus) | 4 |
| 7 | Volumen real y costos de escalado | 5 |
| 8 | Inferencia en vivo de D4 y su error en producción (MAE en vivo) | 6-7 |
| 9 | Respuestas a las preguntas de referencia (S12) | 8 |

**Cómo correrlo:** *Kernel → Restart Kernel and Run All Cells* (unos 4 min; la primera vez descarga el conector de
Kafka). **Con el ESP32 encendido** se ven sus lecturas en vivo; si no, se ven las de ThingSpeak.
""")

code("""
# Preparación
import importlib, json, socket, subprocess, sys, tempfile, time, os
for paquete, modulo in [("matplotlib", "matplotlib"), ("pyyaml", "yaml"), ("requests", "requests")]:
    if importlib.util.find_spec(modulo) is None:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", paquete], check=True)
from pathlib import Path
import numpy as np, pandas as pd, matplotlib.pyplot as plt, yaml, requests
from pyspark.sql import SparkSession, functions as F

RAIZ = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "streaming" / "procesar_stream.py").exists())
for d in ("streaming", "batch", "notebooks/u2"):
    sys.path.insert(0, str(RAIZ / d))
import soporte_u2 as su                    # consultas a InfluxDB (token desde .env, sin mostrarlo)
LIMA = "America/Lima"
REAL, TS = "esp32-tanque-real-01", "thingspeak-1233810"
TOPICO, TOPICO_SERVICIO = "aqua-eventos-real", "servicio-eventos"

DURACION_S = 90        # cuánto corre el stream propio (sección 4)
TRIGGER = "10 seconds" # un micro-lote cada 10 s
REPETIR_MIN = 30       # además de lo nuevo, re-procesa lo publicado en los últimos N min (0 = solo lo nuevo)

def alcanzable(candidatos):
    for host, puerto in candidatos:
        try:
            socket.create_connection((host, puerto), timeout=3).close(); return host, puerto
        except OSError:
            pass
    return None

k = alcanzable([("kafka", 9092), ("lambda26-kafka", 9092), ("host.docker.internal", 49092)])
BOOTSTRAP = f"{k[0]}:{k[1]}" if k else None
p = alcanzable([("prometheus", 9090), ("lambda26-prometheus", 9090), ("host.docker.internal", 49090)])
PROMETHEUS = f"http://{p[0]}:{p[1]}" if p else None
print("Kafka:", BOOTSTRAP or "NO ALCANZABLE: levanta Jupyter con notebooks/u2/jupyter.override.yml (red lambda26-kafka-net)")
print("Prometheus:", PROMETHEUS or "no alcanzable")
""")

code("""
spark = (SparkSession.builder.appName("u3-d4-streaming").master("local[*]")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
# El MISMO esquema del evento y el MISMO enriquecimiento (latencia, validación de rango) del job de producción
from procesar_stream import ESQUEMA, enriquecer, WATERMARK_RAPIDO, ORIGENES_RAPIDOS, RANGOS
geom = yaml.safe_load(open(RAIZ / "ingesta" / "tanques.yml", encoding="utf-8"))["tanques"][REAL]
FONDO, ALTO, CAP = geom["sensor_al_fondo_cm"], geom["altura_interior_cm"], geom["capacidad_l"]
EXCLUIDOS = [s["sensorId"] for s in yaml.safe_load(open(RAIZ / "ingesta" / "sensores_prueba.yml", encoding="utf-8")).get("sensores", [])]
ahora_ms = lambda: int(time.time() * 1000)

def leer_kafka_batch(topico, desde_ms):
    \"\"\"Lectura batch (no stream) de un tópico desde una hora: para inspeccionar particiones, offsets y claves.\"\"\"
    return (spark.read.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("subscribe", topico)
            .option("startingTimestamp", str(desde_ms)).option("startingOffsetsByTimestampStrategy", "latest")
            .option("endingOffsets", "latest").load())
print("Spark", spark.version, "· watermark de producción:", WATERMARK_RAPIDO)
""")

md("""
## 1. Kafka: tópico, particiones, offsets y clave

- **Tópico** `aqua-eventos-real`: el log de lecturas. **3 particiones** para repartir la carga.
- **Offset:** posición de cada mensaje dentro de su partición; el consumidor recuerda hasta dónde leyó.
- **Clave = `sensorId`:** Kafka asigna la partición por la clave, así **todas las lecturas de un tanque caen en la
  misma partición y en orden**. Es lo que necesita el detector de llegadas, que compara cada lectura con la anterior.

> **Para explicarlo:** "Cada sensor siempre cae en la misma partición: lo compruebo abajo (una partición por sensor).
> Así el orden de las lecturas de un tanque se conserva."
""")

code("""
ult_hora = leer_kafka_batch(TOPICO, ahora_ms() - 3_600_000).cache()
print(f"Mensajes publicados en la última hora en {TOPICO}: {ult_hora.count():,}")
display(ult_hora.groupBy("partition").agg(F.count("*").alias("mensajes_ultima_hora"), F.min("offset").alias("offset_desde"),
                                          F.max("offset").alias("offset_hasta")).orderBy("partition").toPandas())
claves = (ult_hora.select(F.col("key").cast("string").alias("clave_sensorId"), "partition")
          .groupBy("clave_sensorId").agg(F.count("*").alias("mensajes"), F.countDistinct("partition").alias("particiones_distintas"),
                                         F.first("partition").alias("particion")).orderBy(F.desc("mensajes")).toPandas())
display(claves.head(12))
print("¿Cada sensor usa una sola partición?", bool((claves["particiones_distintas"] == 1).all()))
""")

md("""
## 2. Evento IoT (ESP32): contrato, validación de esquema y de rango físico

El ESP32 publica cada 5 s (alta frecuencia, poco contenido) por HTTPS a Firebase; el **puente** lo pasa a Kafka. El
dispositivo nunca habla Kafka directo: no tiene memoria ni librerías para el protocolo, y exponer el broker a
internet sería un riesgo. Cada evento cumple `contratos/evento_aqua.schema.json`; Spark además valida el **rango
físico** de cada medida (por ejemplo, el `-1` del sensor fallido o una distancia fuera de 2-450 cm).
""")

code("""
ev_hora = enriquecer(ult_hora.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e"), "partition", "offset")
                     .select("e.*", "partition", "offset"))
esp = ev_hora.filter(F.col("sensorId") == REAL)
print("Un evento IoT del ESP32 tal como viaja en Kafka (contrato evento_aqua):")
muestra = ult_hora.filter(F.col("key").cast("string") == REAL).orderBy(F.desc("offset")).select(F.col("value").cast("string")).first()
print(json.dumps(json.loads(muestra[0]), indent=2, ensure_ascii=False) if muestra else "  (el ESP32 no publicó en la última hora)")
print("\\nRangos físicos que valida Spark:", {k: v for k, v in RANGOS.items() if k in ("distanciaCm", "temperaturaC", "humedadPct", "nivelPct")})
display(ev_hora.filter(F.col("origen").isin(ORIGENES_RAPIDOS)).groupBy("sensorId")
        .agg(F.count("*").alias("lecturas_ultima_hora"),
             F.round(F.count("*") / 60, 1).alias("por_minuto"),
             F.sum((~F.col("lectura_valida")).cast("int")).alias("invalidas"),
             F.collect_set("motivo_invalidez").alias("motivos")).orderBy(F.desc("lecturas_ultima_hora")).toPandas())
""")

md("""
## 3. Evento empresarial: `servicio-eventos`

Spark **deriva eventos de negocio** de las lecturas (detector congelado f4.4) y los **publica** en Kafka
`servicio-eventos` con clave `sensorId`: `llegada_agua` (D4), `corte_prolongado`, `alerta_fuga`, `autonomia_baja`…
Son pocos y con mucho significado (al revés que el IoT). Contrato: `contratos/servicio_evento.schema.json`; el
`eventoId = tipo-sensorId-inicioTs` es determinístico, así un consumidor descarta repetidos. Los **consume** el
servicio `aqua-servicio-consumidor` (grupo de consumidores con su lag en la sección 6).

> **Para explicarlo:** "El evento IoT dice 'la distancia es 71 cm'; el evento empresarial dice 'llegó el agua a las
> 13:39 con 61 L/min'. El segundo es el que le importa al negocio y lo produce el streaming."
""")

code("""
contrato = json.load(open(RAIZ / "contratos" / "servicio_evento.schema.json", encoding="utf-8"))
servicio = leer_kafka_batch(TOPICO_SERVICIO, ahora_ms() - 7 * 86_400_000)
filas = [dict(json.loads(r.value), particion=r.partition, offset=r.offset, clave=r.key)
         for r in servicio.select(F.col("key").cast("string").alias("key"), F.col("value").cast("string").alias("value"),
                                  "partition", "offset").collect()]
se = pd.DataFrame(filas)
print(f"Eventos de negocio en {TOPICO_SERVICIO} (últimos 7 días, retención del tópico): {len(se)}")
if len(se):
    faltan = [c for c in contrato["required"] if c not in se.columns]
    print("Campos obligatorios del contrato presentes:", "todos" if not faltan else f"faltan {faltan}")
    print("eventoId == tipo-sensorId-inicioTs en todos:",
          bool((se["eventoId"] == se["tipo"] + "-" + se["sensorId"] + "-" + se["inicioTs"].astype(str)).all()))
    display(se.groupby("tipo").size().rename("eventos").to_frame())
    d4 = se[(se["tipo"] == "llegada_agua") & se["sensorId"].isin([REAL, TS])].copy()
    d4["inicio (Lima)"] = pd.to_datetime(d4["inicioTs"], unit="ms", utc=True).dt.tz_convert(LIMA).dt.strftime("%d/%m %H:%M")
    d4["detectado (Lima)"] = pd.to_datetime(d4["detectadoTs"], unit="ms", utc=True).dt.tz_convert(LIMA).dt.strftime("%d/%m %H:%M:%S")
    print("Llegadas de agua (D4) publicadas como evento de negocio:")
    display(d4[["inicio (Lima)", "detectado (Lima)", "sensorId", "valor", "severidad", "particion", "offset"]].tail(10))
""")

md("""
## 4. Spark Structured Streaming en vivo: ventana, watermark y checkpoint

Un stream propio, aquí mismo, con **el mismo esquema y las mismas reglas** del job de producción
(`streaming/procesar_stream.py`), pero con **checkpoint propio** (no toca producción):

1. `readStream` sobre Kafka: cada micro-lote trae solo lo nuevo (más los últimos `REPETIR_MIN` min, para tener datos).
2. **Watermark de 7 min** sobre el tiempo del evento: hasta cuánto se esperan lecturas atrasadas; lo más viejo se
   descarta (y lo recupera la capa batch).
3. **Deduplicación por `eventId`** dentro del watermark (entrega al-menos-una-vez).
4. **Ventanas fijas de 1 min** por sensor (nivel medio, lecturas, latencia máxima) en modo *update*.
5. `foreachBatch`, igual que producción (allí escribe a InfluxDB; aquí guarda en memoria para mostrarlo).

> **Para explicarlo:** mientras corre, señala cómo cada lote trae filas nuevas y cómo avanza el watermark.
""")

code("""
fuente = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("subscribe", TOPICO)
          .option("failOnDataLoss", "false").option("maxOffsetsPerTrigger", 5000))
fuente = (fuente.option("startingTimestamp", str(ahora_ms() - REPETIR_MIN * 60_000)).option("startingOffsetsByTimestampStrategy", "latest")
          if REPETIR_MIN else fuente.option("startingOffsets", "latest"))
crudo = fuente.load()
ev = enriquecer(crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e"),
                             F.col("partition").alias("kafka_particion"), F.col("offset").alias("kafka_offset"),
                             F.col("timestamp").alias("kafka_ts"))
                .select("e.*", "kafka_particion", "kafka_offset", "kafka_ts"))
tanques = (ev.filter(F.col("origen").isin(ORIGENES_RAPIDOS) & ~F.col("sensorId").isin(EXCLUIDOS))
             .withWatermark("tiempo_evento", WATERMARK_RAPIDO)
             .dropDuplicatesWithinWatermark(["eventId"])
             .withColumn("nivel_pct", F.when(F.col("origen") == "esp32-fisico", (F.lit(FONDO) - F.col("distanciaCm")) / F.lit(ALTO) * 100)
                                       .otherwise(F.col("nivelPct"))))
ventanas = (tanques.filter("lectura_valida")
            .groupBy(F.window("tiempo_evento", "1 minute"), "sensorId")
            .agg(F.count("*").alias("lecturas"), F.round(F.avg("nivel_pct"), 1).alias("nivel_medio_pct"),
                 F.round(F.max("latencia_ingesta_s"), 1).alias("latencia_max_s")))
print("¿Es un stream?", crudo.isStreaming, "· todavía no corre nada: Spark es perezoso hasta start()")
""")

code("""
lotes_lect, lotes_vent = [], []
def guardar(destino):
    def f(df, batch_id):
        p = df.toPandas()
        if len(p):
            destino.append(p.assign(lote=batch_id))
    return f

def como_dict(p):        # PySpark 4 devuelve objetos StreamingQueryProgress; los pasamos a dict
    return json.loads(p.json) if hasattr(p, "json") else p

CHECKPOINT = tempfile.mkdtemp(prefix="u3-d4-stream-")
def iniciar_lecturas():
    return (tanques.select("sensorId", "origen", "eventId", "tiempo_evento", "kafka_ts", "distanciaCm", "nivel_pct",
                           "temperaturaC", "latencia_ingesta_s", "lectura_valida", "motivo_invalidez")
            .writeStream.queryName("d4_lecturas").outputMode("append").foreachBatch(guardar(lotes_lect))
            .option("checkpointLocation", CHECKPOINT + "/lecturas").trigger(processingTime=TRIGGER).start())

def seguir(q, segundos, etiqueta=""):
    t0, vistos = time.time(), set()
    while time.time() - t0 < segundos:
        time.sleep(2)
        for p in map(como_dict, q.recentProgress):
            if p["batchId"] in vistos:
                continue
            vistos.add(p["batchId"])
            print(f"{etiqueta}[{time.time() - t0:5.0f} s] lote {p['batchId']:>3} · filas {p['numInputRows']:>6} · "
                  f"{p.get('processedRowsPerSecond', 0):8.1f} filas/s · watermark {p.get('eventTime', {}).get('watermark', '-')}")
    return [como_dict(p) for p in q.recentProgress]

q1 = iniciar_lecturas()
q2 = (ventanas.writeStream.queryName("d4_ventanas").outputMode("update").foreachBatch(guardar(lotes_vent))
      .option("checkpointLocation", CHECKPOINT + "/ventanas").trigger(processingTime=TRIGGER).start())
progreso = seguir(q1, DURACION_S)
q1.stop(); q2.stop()
print("Stream detenido. Checkpoint:", CHECKPOINT)
""")

code("""
display(pd.DataFrame([{
    "lote": p["batchId"], "filas_entrada": p["numInputRows"], "filas_por_s": round(p.get("processedRowsPerSecond", 0), 1),
    "descartadas_por_watermark": sum(s.get("numRowsDroppedByWatermark", 0) for s in (p.get("stateOperators") or [])),
    "filas_en_estado_dedup": sum(s.get("numRowsTotal", 0) for s in (p.get("stateOperators") or [])),
    "watermark": p.get("eventTime", {}).get("watermark")} for p in progreso]))

lect = pd.concat(lotes_lect, ignore_index=True) if lotes_lect else pd.DataFrame()
if len(lect):
    lect["tiempo_evento"] = pd.to_datetime(lect["tiempo_evento"], utc=True).dt.tz_convert(LIMA)
    display(lect.groupby("sensorId").agg(
        lecturas=("eventId", "count"), validas=("lectura_valida", "sum"),
        latencia_p50_s=("latencia_ingesta_s", lambda s: round(s.quantile(0.5), 1)),
        latencia_p95_s=("latencia_ingesta_s", lambda s: round(s.quantile(0.95), 1))).sort_values("lecturas", ascending=False))
    print("eventId repetidos después de la deduplicación:", int(lect["eventId"].duplicated().sum()))
    e = lect[(lect["sensorId"] == REAL) & lect["lectura_valida"]]
    if len(e):
        u = e.sort_values("tiempo_evento").iloc[-1]
        print(f"ESP32 en vivo: última lectura {u['tiempo_evento']:%H:%M:%S} · distancia {u['distanciaCm']:.1f} cm · "
              f"nivel {u['nivel_pct']:.1f} % · {u['nivel_pct'] / 100 * CAP:.0f} L (comparar con Grafana)")
    else:
        print("El ESP32 no publicó en la ventana: ¿está encendido y con WiFi?")
else:
    print("No entró ninguna lectura de tanque. Sube REPETIR_MIN o enciende el ESP32.")

vent = pd.concat(lotes_vent, ignore_index=True) if lotes_vent else pd.DataFrame()
if len(vent):
    vent["inicio"] = pd.to_datetime(vent["window"].map(lambda w: w["start"]), utc=True).dt.tz_convert(LIMA)
    vent = vent.sort_values("lote").groupby(["sensorId", "inicio"]).tail(1)     # la última versión de cada ventana
    fig, ax = plt.subplots(figsize=(12, 3.3))
    for s, g in vent.sort_values("inicio").groupby("sensorId"):
        ax.plot(g["inicio"], g["nivel_medio_pct"], marker="o", ms=3, label=s)
    ax.set_ylabel("nivel medio %"); ax.set_title("Ventanas de 1 min calculadas por el stream"); ax.legend()
    plt.tight_layout(); plt.show()
""")

md("""
## 5. Checkpoint verificado: el stream se reanuda donde quedó

El checkpoint guarda los **offsets** de Kafka ya procesados y el **estado** (deduplicación, ventanas). Si el job se
cae y vuelve a arrancar con el mismo checkpoint, **continúa en el lote siguiente y desde el offset siguiente**, sin
reprocesar ni perder. Aquí se reinicia la misma consulta 25 s con el mismo checkpoint y se comparan los números.
""")

code("""
print("Contenido del checkpoint:", sorted(os.listdir(CHECKPOINT + "/lecturas")))
ultimo_lote = max(int(f) for f in os.listdir(CHECKPOINT + "/lecturas/commits") if f.isdigit())
offsets_previos = json.loads(open(f"{CHECKPOINT}/lecturas/offsets/{ultimo_lote}").read().splitlines()[-1])
print(f"Último lote confirmado antes de detener: {ultimo_lote} · offsets por partición: {offsets_previos}")
q1b = iniciar_lecturas()
progreso_b = seguir(q1b, 25, "reanudado ")
q1b.stop()
if progreso_b:
    p0 = progreso_b[0]
    print(f"\\nAl reanudar, el primer lote es el {p0['batchId']} (antes terminó en {ultimo_lote}) y empezó en los offsets",
          p0["sources"][0]["startOffset"])
    print("¿Continuó sin reprocesar?", p0["batchId"] == ultimo_lote + 1)
else:
    print("En 25 s no llegaron mensajes nuevos: no hubo lote. Repite la celda con el ESP32 encendido.")
""")

md("""
## 5b. El modelo d4.1 dentro de este stream (el mismo que usa producción)

El modelo de D4 ya está **entrenado en la capa batch** y versionado en `batch/modelos/d4/d4.1-*/modelo.json`
(supervivencia por hora: Weibull o Kaplan-Meier según los intervalos). Aquí se **carga ese mismo archivo** con la
misma clase de producción (`batch/llegadas.py`, `ModeloLlegadas`) y se aplica **dentro de `foreachBatch`, en cada
micro-lote**: con las horas desde la última llegada y la hora de Lima da la hora más probable de la próxima llegada,
la ventana del 80 % y la probabilidad de que llegue en 6/12/24 h. No se reentrena nada: inferencia sin estado propio,
una lectura nueva basta para actualizar el pronóstico.

Después se compara con lo que escribió producción para el mismo tanque: si el modelo y los datos son los mismos, el
pronóstico debe coincidir.

> **Para explicarlo:** "El modelo se entrena en batch y se usa en streaming: es Lambda completo. Lo cargo del mismo
> archivo versionado que usa producción y da el mismo pronóstico. El ESP32 todavía no tiene modelo propio: tiene pocas
> llegadas sin huecos y su geometría es provisional; el sistema lo dice con un motivo en vez de inventar."
""")

code("""
from llegadas import ModeloLlegadas
RUTA_MODELO = sorted((RAIZ / "batch" / "modelos" / "d4").glob("d4.1-*/modelo.json"))[-1]
MODELO_D4 = json.load(open(RUTA_MODELO, encoding="utf-8"))
print("Modelo:", RUTA_MODELO.relative_to(RAIZ), "·", MODELO_D4["modelo"])
tanques_yml = yaml.safe_load(open(RAIZ / "ingesta" / "tanques.yml", encoding="utf-8"))["tanques"]

# Modelo reentrenado en U3 (d4_batch.ipynb, sección 7b): solo para los tanques que d4.1 no cubre (el ESP32)
RUTA_U3 = RAIZ / "data" / "local" / "u3" / "modelo_d4_u3" / "modelo.json"
MODELO_U3 = json.load(open(RUTA_U3, encoding="utf-8")) if RUTA_U3.exists() else {"fuentes_llegadas": {}}
print("Modelo U3 (indicativo):", "cargado, tanques " + str(list(MODELO_U3["fuentes_llegadas"].get("detector_f44", {})))
      if RUTA_U3.exists() else "no existe todavía: corre antes d4_batch.ipynb (sección 7b)")

def entrada_d4(sensor, run=""):
    \"\"\"Igual que streaming/inferencia.py: el modelo del tanque (o de su zona) para esa corrida.\"\"\"
    zona = tanques_yml.get(sensor, {}).get("zona")
    orden = ("subida_sostenida", "detector_f43") if sensor.startswith("thingspeak") else ("detector_f43",)
    for fuente in orden:
        for clave, v in MODELO_D4["fuentes_llegadas"].get(fuente, {}).items():
            partes = clave.split("|")
            if (partes[1] == sensor or partes[1] == f"zona:{zona}") and partes[2] == run:
                return fuente, v
    u3 = MODELO_U3["fuentes_llegadas"].get("detector_f44", {}).get(f"real|{sensor}|")
    return ("d4.1-u3 (indicativo)", u3) if u3 else (None, None)

# Última llegada de cada tanque según producción (horas_desde_llegada de su última estimación)
est_prod = su.predicciones("D4", "estimacion", horas=72, limite=2000)
ULTIMA_LLEGADA = {}
if len(est_prod) and "horas_desde_llegada" in est_prod:
    for sid, g in est_prod.sort_values("_time").groupby("sensorId"):
        u = g.iloc[-1]
        ULTIMA_LLEGADA[sid] = int(u["_time"].timestamp() * 1000 - float(u["horas_desde_llegada"]) * 3_600_000)

# Tanques sin estimación de producción (el ESP32): su última llegada sale de los eventos de negocio del streaming
lleg_prod = su.influx(f\'\'\'from(bucket: "monitoraqua") |> range(start: -30d)
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua" and r._field == "valor")
  |> group(columns: ["sensorId"]) |> last() |> group()\'\'\')
for _, r in lleg_prod.iterrows():
    ULTIMA_LLEGADA.setdefault(r["sensorId"], int(pd.Timestamp(r["_time"]).timestamp() * 1000))

def predecir_d4(sensor, ts):
    fuente, e = entrada_d4(sensor)
    if not e:
        return {"motivo": "sin modelo de llegadas para este tanque (pocas llegadas observadas sin huecos)"}
    if e.get("fuera_de_alcance"):
        return {"motivo": f"fuera del alcance de D4: {e['fuera_de_alcance']}"}
    if sensor not in ULTIMA_LLEGADA:
        return {"motivo": "esperando la primera llegada observada"}
    tipo = "weibull" if e.get("weibull") else "km"
    mod = ModeloLlegadas.desde_dict(e[tipo])
    horas = (ts - ULTIMA_LLEGADA[sensor]) / 3_600_000
    if horas > 72:
        return {"motivo": f"{horas:.0f} h desde la última llegada observada (hueco del sensor): esperando una llegada nueva"}
    hora_lima = pd.Timestamp(ts, unit="ms", tz="UTC").tz_convert(LIMA).hour
    r = mod.predecir(int(horas), hora_lima)
    q = e.get("ventana_q_h") or 0.0
    return {"modelo": tipo, "fuente_llegadas": fuente, "horas_desde_llegada": round(horas, 2), "horas_moda": r["moda_h"],
            "ventana_ini_h": round(max(0.0, r["p10_h"] - q), 2), "ventana_fin_h": round(r["p90_h"] + q, 2),
            "prob_6h": round(r["prob_6h"], 3), "prob_12h": round(r["prob_12h"], 3), "prob_24h": round(r["prob_24h"], 3),
            "proxima_llegada_lima": pd.Timestamp(ts + r["moda_h"] * 3_600_000, unit="ms", tz="UTC").tz_convert(LIMA).strftime("%d/%m %H:%M")}

predicciones_stream = []
def inferir(df, batch_id):          # se ejecuta en CADA micro-lote
    for fila in df.groupBy("sensorId").agg(F.max("timestamp").alias("ts")).collect():
        predicciones_stream.append({"lote": batch_id, "sensorId": fila["sensorId"],
                                    "hora_lectura": pd.Timestamp(fila["ts"], unit="ms", tz="UTC").tz_convert(LIMA).strftime("%H:%M:%S"),
                                    **predecir_d4(fila["sensorId"], fila["ts"])})

q3 = (tanques.filter("lectura_valida").writeStream.queryName("d4_inferencia").foreachBatch(inferir)
      .option("checkpointLocation", CHECKPOINT + "/inferencia").trigger(processingTime=TRIGGER).start())
seguir(q3, 60, "inferencia ")
q3.stop()
pred = pd.DataFrame(predicciones_stream)
if len(pred):
    display(pred.groupby("sensorId").tail(1).reset_index(drop=True))
    if len(est_prod):
        print("Producción (última estimación de D4 por tanque), para comparar:")
        ult = est_prod.sort_values("_time").groupby("sensorId").tail(1)
        display(ult[[c for c in ["_time", "sensorId", "horas_desde_llegada", "horas_moda", "ventana_ini_h", "ventana_fin_h",
                                 "prob_24h", "modelo_tipo"] if c in ult]].reset_index(drop=True))
else:
    print("Ningún micro-lote trajo lecturas de tanque en 60 s.")
""")

md("""
## 6. Observabilidad: disponibilidad, throughput y lag del consumidor (Prometheus)

| Métrica | Qué dice | Fuente |
|---|---|---|
| `up` | Disponibilidad: cada componente responde | Prometheus |
| Mensajes/min en `aqua-eventos-real` | Throughput de entrada | kafka-exporter |
| **Lag del consumidor** | Mensajes que esperan ser procesados: si crece, el pipeline se está atrasando (*backpressure*) | kafka-exporter |
| Filas/s y watermark de cada lote | Rendimiento de Spark | sección 4 / Spark UI |

El **lag** es la primera alarma de retraso: sube antes de que se note en Grafana. En el tablero "Salud del pipeline"
estas métricas están juntas y con umbrales de alerta.
""")

code("""
def prom(q):
    r = requests.get(f"{PROMETHEUS}/api/v1/query", params={"query": q}, timeout=10).json()
    return [(x["metric"], float(x["value"][1])) for x in r.get("data", {}).get("result", [])]

if PROMETHEUS:
    disp = prom("up")
    display(pd.DataFrame([{"job": m.get("job"), "instancia": m.get("instance"), "arriba": int(v)} for m, v in disp]))
    tp = prom('sum by (topic) (rate(kafka_topic_partition_current_offset{topic!~"__.*"}[5m])) * 60')
    display(pd.DataFrame([{"tópico": m.get("topic"), "mensajes_por_min": round(v, 1)} for m, v in tp]).sort_values("mensajes_por_min", ascending=False))
    lag = prom("sum by (consumergroup, topic) (kafka_consumergroup_lag)")
    display(pd.DataFrame([{"grupo": m.get("consumergroup"), "tópico": m.get("topic"), "lag_mensajes": int(v)} for m, v in lag]))
    d4m = prom('{job="monitoraqua_d4"}')
    if d4m:
        print("Métricas de la ejecución integrada D4 (Pushgateway):")
        display(pd.DataFrame([{"métrica": m["__name__"], "valor": v} for m, v in d4m]))
else:
    print("Prometheus no alcanzable desde Jupyter: ver http://localhost:49090 y el tablero 'Salud del pipeline'.")
""")

md("""
## 7. Volumen real y costos de escalado

El costo del streaming depende de la **tasa sostenida** (mensajes por segundo que hay que procesar las 24 h, con
servicios siempre encendidos), no del tamaño total como en el batch de la U1 (que se paga por ejecución). Abajo, el
volumen real de las últimas 24 h; los escenarios (hoy, 100 tanques, toda Juliaca) y sus precios están en
`docs/costos-escalado.md` (cálculo reproducible en `infra/costos_escenarios.py`).
""")

code("""
dia = leer_kafka_batch(TOPICO, ahora_ms() - 86_400_000)
vol = dia.groupBy(F.col("key").cast("string").alias("sensorId")).agg(F.count("*").alias("mensajes_24h"),
                                                                     F.round(F.sum(F.length("value")) / 1e6, 2).alias("MB_24h"))
vol_pd = vol.orderBy(F.desc("mensajes_24h")).toPandas()
total, mb = int(vol_pd["mensajes_24h"].sum()), float(vol_pd["MB_24h"].sum())
print(f"Últimas 24 h en {TOPICO}: {total:,} mensajes · {mb:.1f} MB · {total / 86400:.2f} mensajes/s en promedio")
display(vol_pd.head(10))
esp_dia = int(vol_pd.loc[vol_pd["sensorId"] == REAL, "mensajes_24h"].sum())
print(f"Un ESP32 encendido todo el día publica 17,280 lecturas (una cada 5 s); hoy publicó {esp_dia:,}.")
print(f"Escalado lineal: 100 tanques ≈ {100 * 17280 / 86400:.0f} mensajes/s; 50 000 tanques (Juliaca) ≈ {50000 * 17280 / 86400:,.0f} mensajes/s "
      "→ más particiones y brokers (criterios en docs/costos-escalado.md).")
""")

md("""
## 8. Inferencia en vivo de D4 y su error en producción

La consulta `predicciones` del job de producción carga el modelo **d4.1** (entrenado en batch: supervivencia Weibull,
`batch/modelos/d4/`) y lo aplica a **cada micro-lote**: hora más probable de la próxima llegada, ventana del 80 % y
probabilidad en 6/12/24 h. Cuando el agua llega de verdad, compara la hora pronosticada con la real (**MAE en vivo**).
Todo eso se ve en el tablero D4 **junto a los KPIs** (nivel, llegadas, intervalo, caudal).

> **Para explicarlo:** "El modelo se validó offline contra líneas base con división cronológica (1,6 h frente a
> 2,1 h en la flota). Aquí se valida en producción: el error real es mayor (6,2 h) porque el modelo no anticipa los
> cortes de servicio. Esa es la mejora d4.2."
""")

code("""
lleg = su.influx(f'''from(bucket: "monitoraqua") |> range(start: -7d)
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua")
  |> filter(fn: (r) => r.sensorId == "{REAL}")
  |> filter(fn: (r) => r._field == "valor" or r._field == "caudalLlenadoLpm")
  |> pivot(rowKey: ["_time", "sensorId"], columnKey: ["_field"], valueColumn: "_value") |> group() |> sort(columns: ["_time"])''')
if len(lleg):
    lleg["_time"] = pd.to_datetime(lleg["_time"], format="ISO8601").dt.tz_convert(LIMA)
    print(f"KPI D4 · ESP32, últimos 7 días: {len(lleg)} llegadas · intervalo mediano "
          f"{(lleg['_time'].diff().dt.total_seconds() / 3600).median():.1f} h · caudal mediano {lleg['caudalLlenadoLpm'].median():.1f} L/min")

est = su.predicciones("D4", "estimacion", horas=48, limite=400)
if len(est):
    ult = est.sort_values("_time").groupby("sensorId").tail(1)
    display(ult[[c for c in ["_time", "sensorId", "version_modelo", "horas_moda", "ventana_ini_h", "ventana_fin_h",
                             "prob_6h", "prob_12h", "prob_24h", "advertencia"] if c in ult]].reset_index(drop=True))
mot = su.predicciones("D4", "motivo", horas=48, limite=400)
if len(mot):
    mot = mot.sort_values("_time").groupby("sensorId").tail(1)
    if len(est):
        mot = mot[mot["_time"] > mot["sensorId"].map(est.groupby("sensorId")["_time"].max()).fillna(pd.Timestamp(0, tz=LIMA))]
    print("Tanques reales donde hoy el modelo no aplica (motivo vigente):")
    display(mot[["_time", "sensorId", "motivo"]].reset_index(drop=True))

ver_real = su.predicciones("D4", "verificacion", horas=24 * 30)
ver_flota = su.predicciones("D4", "verificacion", bucket="monitoraqua_aumentados", horas=1440)
for nombre, v in (("real", ver_real), ("flota (datos aumentados)", ver_flota)):
    if len(v) and {"llegada_real_ts", "llegada_pred_ts"} <= set(v.columns):
        v = v.dropna(subset=["llegada_real_ts", "llegada_pred_ts"])
        err = (v["llegada_real_ts"] - v["llegada_pred_ts"]) / 3_600_000
        print(f"{nombre}: {len(v)} pronósticos verificados · MAE en vivo {err.abs().mean():.3f} h · sesgo {(-err).mean():+.3f} h")
        display(v.assign(_e=err.abs()).groupby("sensorId")["_e"].agg(verificados="count", MAE_h="mean").round(3))
    else:
        print(f"{nombre}: todavía sin verificaciones de D4")
print("Tablero con la predicción junto a los KPIs: http://localhost:43000/d/monitoraqua-d4")
""")

md("""
## 9. Respuestas a las preguntas de referencia (S12, Tabla 1)

**Contrato empresarial frente a IoT.** El IoT (`evento_aqua`) es una medición cruda, frecuente (cada 5 s) y con
rangos físicos; el empresarial (`servicio_evento`) es un hecho de negocio, raro y con significado (`llegada_agua`),
con `eventoId` determinístico. Por eso van en tópicos distintos: el IoT necesita particiones por sensor y volumen; el
de negocio, que sus consumidores no se pierdan ninguno.

**Evento tarde y watermark.** Si una lectura llega con más de 7 min de atraso respecto del máximo tiempo de evento
visto, Spark la descarta de las ventanas y de la deduplicación (sección 4, `descartadas_por_watermark`). Sin
watermark, el estado crecería sin límite esperando atrasados y la memoria se agotaría; la capa batch recupera lo
descartado.

**¿Qué métrica avisa primero del retraso (backpressure)?** El **lag del consumidor** (sección 6): sube en cuanto
Spark procesa menos de lo que entra, antes de que el tablero muestre datos viejos.

**Costo del streaming frente al batch de la U1.** El batch se paga por ejecución y depende del tamaño total; el
streaming corre 24/7 y depende de la **tasa sostenida** y de los picos: hay que dimensionar brokers, particiones y
ejecutores para el peor momento (sección 7).

**¿Qué gana el tablero al poner la predicción junto a los KPIs?** Se lee en una sola vista: "llegaron 7 veces esta
semana, cada 4 h en mediana, y la próxima se espera a tal hora (ventana del 80 %)". Separados, el usuario tendría que
cruzarlos y no vería si la predicción es coherente con lo observado.

**Baseline de persistencia.** Es el pronóstico más barato ("se repite el ritmo"); si el modelo no le gana, no aporta.
d4.1 le gana en la evaluación offline de la flota (1,6 h frente a 2,1 h); en el notebook batch la persistencia gana
al MLlib sobre la variable horaria, y se guardó la base.

**KPI de negocio frente a métrica de infraestructura; Pushgateway.** KPI: "horas entre llegadas de agua" (le importa
a la familia). Métrica de infraestructura: lag, CPU, latencia de ingesta (le importa al operador). Pushgateway sirve
para un job **que termina** (la ejecución integrada D4 empuja su resultado al final); para un job que no termina,
lo recomendado es exponer `/metrics` y que Prometheus lo raspe: el Pushgateway guarda el último valor aunque el job
haya muerto, por eso el tablero tiene un panel de antigüedad.
""")

code("""
spark.stop()
print("Listo.")
""")

nb = nbf.v4.new_notebook(cells=celdas, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                                                 "language_info": {"name": "python"}})
nbf.write(nb, AQUI / "d4_streaming.ipynb")
print("notebooks/u3/d4_streaming.ipynb:", len(celdas), "celdas")
