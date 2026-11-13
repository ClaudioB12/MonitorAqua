"""Genera notebooks/u3/d4_batch.ipynb (capa batch de D4, rúbrica U1/S5). Regenerar: python notebooks/u3/generar_d4_batch.py"""
from pathlib import Path

import nbformat as nbf

AQUI = Path(__file__).resolve().parent
celdas = []


def md(texto):
    celdas.append(nbf.v4.new_markdown_cell(texto.strip()))


def code(texto):
    celdas.append(nbf.v4.new_code_cell(texto.strip()))


md("""
# D4 · Capa batch: ETL distribuido, calidad, Parquet particionado y ML con Spark

**Dimensión D4 · Próxima llegada del agua** · Wilbert Alex Mayta Arotaype · Equipo Horeb · MonitorAqua

**Pregunta de la dimensión:** ¿cuándo llegará el agua al tanque? En Juliaca el suministro es intermitente: saber la
hora probable de la próxima llegada permite decidir cuándo llenar y cuánto consumir.

Este notebook es la **capa batch** de la arquitectura Lambda y sigue la secuencia de la evaluación de la Unidad I (S5):

| # | Sección | Qué pide la rúbrica (S5) |
|---|---|---|
| 0 | Arquitectura y dimensión | Arquitectura Big Data del equipo y la dimensión dentro de la pregunta central |
| 1 | Extracción con esquema explícito | Lectura del instrumento con `StructType` |
| 2 | Transformaciones distribuidas | `withColumn`, `filter`, funciones, agrupaciones, **RDD** y **`explain(True)`** (evaluación perezosa) |
| 3 | Calidad: nulos y duplicados | Antes y después; deduplicación determinista con **`Window` + `row_number()`** |
| 4 | Latencia, nivel y huecos | KPI de ingesta con p50/p95 y estado del sensor |
| 5 | Capa oro: llegadas de agua | El indicador de la dimensión |
| 6 | Salida particionada en Parquet | Escritura, **lectura de vuelta** y **`PartitionFilters`** en el plan |
| 7 | ML distribuido con MLlib | `VectorAssembler`, `fit`/`transform`, 3 métricas, comparación, **modelo guardado = ganador** |
| 7b | El modelo de producción (d4.1) reentrenado con todas las llegadas del ESP32 y la API | Modelo de la dimensión, evaluado y guardado para el streaming |
| 8 | Batch frente a streaming | ¿El batch ve lo mismo que el streaming? |
| 9 | Respuestas a las preguntas de referencia | Tabla 2 de S5 |

**Cómo correrlo:** *Kernel → Restart Kernel and Run All Cells* (unos 4-5 min).
**Datos:** lecturas **reales** del ESP32 físico y de ThingSpeak 1233810. Para entrenar el modelo de la sección 7 se
suman los **datos aumentados** de la flota extrapolada (rotulados como tales); la reconstrucción de huecos no entra.
""")

md("""
## 0. Arquitectura y dimensión

```text
ESP32 + JSN-SR04T ─┐
APIs públicas ─────┴─> Firebase ─> puente ─> Kafka aqua-eventos-real ─┬─> SPEED: Spark Structured Streaming ─> InfluxDB ─> Grafana
                                                                      └─> Parquet BRONCE ─> BATCH (este notebook): plata ─> oro ─> modelo
```

**Arquitectura Lambda** (decisión del equipo desde S1): la capa de velocidad responde en segundos pero descarta lo
que llega tarde (watermark); la capa batch reprocesa **todo** lo guardado, corrige y entrena los modelos que luego usa
el streaming. Se eligió Lambda y no Kappa porque los modelos necesitan reentrenarse sobre la historia completa y el
ESP32 entrega lecturas atrasadas de horas o días cuando se reconecta.

**Evolución de la dimensión:** U1 → ETL batch y primer modelo; U2 → detector de llegadas en streaming, modelo de
supervivencia d4.1 e inferencia en vivo; U3 → ejecución integrada con compuertas, este notebook y su gemelo
`d4_streaming.ipynb`.
""")

code("""
# Preparación: librerías, rutas y Spark
import importlib, subprocess, sys
for paquete, modulo in [("matplotlib", "matplotlib"), ("pyyaml", "yaml"), ("pyarrow", "pyarrow")]:
    if importlib.util.find_spec(modulo) is None:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", paquete], check=True)

import glob, json, shutil, time, urllib.request
from pathlib import Path
import numpy as np, pandas as pd, matplotlib.pyplot as plt, yaml
from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import (StructType, StructField, StringType, LongType, DoubleType, BooleanType, DateType)

RAIZ = next(p for p in [Path.cwd(), *Path.cwd().parents] if (p / "streaming" / "detector_servicio.py").exists())
sys.path.insert(0, str(RAIZ / "streaming"))
BRONCE = RAIZ / "data" / "local" / "bronce"
SALIDA = RAIZ / "data" / "local" / "u3"                 # salidas de este notebook (no versionadas)
LIMA = "America/Lima"
REAL, TS = "esp32-tanque-real-01", "thingspeak-1233810"
SENSORES = [REAL, TS]
DESDE = pd.Timestamp("2026-10-29T09:22:23-05:00")      # instalación del JSN-SR04T (ingesta/tanques.yml -> sensor_desde)

spark = (SparkSession.builder.appName("u3-d4-batch").master("local[*]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.files.openCostInBytes", "8192")   # muchos Parquet pequeños: agruparlos al leer
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
pd.set_option("display.width", 160)
print("Spark", spark.version, "· núcleos", spark.sparkContext.defaultParallelism, "· repo:", RAIZ)
""")

md("""
## 1. Extracción con esquema explícito

El bronce es la copia cruda de todo lo que pasó por Kafka, en Parquet particionado por `origen` y `fecha`. Se lee con
un **esquema explícito** (`StructType`): los tipos los fija el contrato del evento (`contratos/evento_aqua.schema.json`),
no la inferencia de Spark, así un archivo raro no cambia el tipo de una columna sin que nos enteremos.

> **Para explicarlo:** "Defino el esquema yo, como en S2: el `timestamp` es epoch en ms (long), las medidas son double.
> El batch vuelve a leer todo lo que guardó el streaming; no depende de que el stream haya funcionado bien."
""")

code("""
ESQUEMA_BRONCE = StructType([
    StructField("tipoEvento", StringType()), StructField("eventId", StringType()),
    StructField("sensorId", StringType()), StructField("fuenteTipo", StringType()),
    StructField("timestamp", LongType()), StructField("ingestaTs", LongType()),
    StructField("distanciaCm", DoubleType()), StructField("nivelPct", DoubleType()), StructField("nivelM", DoubleType()),
    StructField("temperaturaC", DoubleType()), StructField("humedadPct", DoubleType()),
    StructField("latencia_ingesta_s", DoubleType()), StructField("motivo_invalidez", StringType()),
    StructField("lectura_valida", BooleanType()), StructField("kafka_offset", LongType()),
    StructField("origen", StringType()), StructField("fecha", DateType()),          # columnas de partición (carpetas)
])
dia_min = (DESDE - pd.Timedelta(days=1)).date().isoformat()
rutas = sorted(p for o in ["esp32-fisico", "thingspeak"]
               for p in glob.glob(str(BRONCE / f"origen={o}" / "fecha=*")) if p.rsplit("fecha=", 1)[1] >= dia_min)
t0 = time.time()
bronce = (spark.read.schema(ESQUEMA_BRONCE).option("basePath", str(BRONCE)).parquet(*rutas)
          .filter(F.col("timestamp") >= int(DESDE.timestamp() * 1000))
          .cache())
n_bronce = bronce.count()
print(f"{len(rutas)} carpetas origen/fecha · {n_bronce:,} lecturas · {bronce.rdd.getNumPartitions()} particiones de Spark · {time.time() - t0:.0f} s")
bronce.printSchema()
""")

md("""
## 2. Transformaciones distribuidas, RDD y evaluación perezosa

- **Transformaciones** (`withColumn`, `filter`, `groupBy`) solo construyen un plan: no ejecutan nada.
- **Acciones** (`count`, `show`, `collect`, `write`) disparan la ejecución.
- `explain(True)` muestra el plan lógico, el optimizado por **Catalyst** (que empuja los filtros hacia la lectura) y
  el físico, **antes** de ejecutar.

> **Para explicarlo:** "Hasta aquí Spark no calculó nada: solo armó el plan. Fíjense en el plan físico: el filtro por
> sensor baja hasta el `FileScan` (PushedFilters). Recién el `show()` lo ejecuta."
""")

code("""
geom = yaml.safe_load(open(RAIZ / "ingesta" / "tanques.yml", encoding="utf-8"))["tanques"][REAL]
FONDO, ALTO, CAP = geom["sensor_al_fondo_cm"], geom["altura_interior_cm"], geom["capacidad_l"]

# Transformaciones: columnas nuevas con funciones de Spark (sin UDF), filtro y agregación
lecturas = (bronce
    .filter(F.col("sensorId").isin(SENSORES))
    .withColumn("hora_lima", F.from_utc_timestamp(F.timestamp_millis("timestamp"), LIMA))
    .withColumn("dia", F.to_date("hora_lima"))
    .withColumn("nivel_pct", F.when(F.col("sensorId") == REAL, (F.lit(FONDO) - F.col("distanciaCm")) / F.lit(ALTO) * 100)
                              .otherwise(F.col("nivelPct"))))
resumen_dia = (lecturas.filter(F.col("lectura_valida") & F.col("nivel_pct").between(0, 110))
               .groupBy("sensorId", "dia")
               .agg(F.count("*").alias("lecturas"), F.round(F.avg("nivel_pct"), 1).alias("nivel_medio_pct"),
                    F.round(F.min("nivel_pct"), 1).alias("nivel_min_pct"), F.round(F.max("nivel_pct"), 1).alias("nivel_max_pct"))
               .orderBy("sensorId", "dia"))
print("Todavía no se ejecutó nada (evaluación perezosa). Plan de ejecución:")
resumen_dia.explain(True)
""")

code("""
resumen_dia.show(40, truncate=False)        # ACCIÓN: aquí recién se ejecuta el plan
""")

code("""
# Lo mismo con la API de bajo nivel (RDD): map + reduceByKey, como en S2
rdd = (lecturas.select("sensorId", "dia", "lectura_valida").rdd
       .map(lambda r: ((r.sensorId, str(r.dia)), (1, int(bool(r.lectura_valida)))))
       .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
print("Particiones del RDD:", rdd.getNumPartitions())
for (sensor, dia), (total, validas) in sorted(rdd.collect())[:12]:
    print(f"{sensor:22s} {dia}  total {total:6d}  válidas {validas:6d}")
""")

md("""
## 3. Calidad: nulos y duplicados (antes y después)

**Nulos.** El ESP32 manda `-1` cuando el sensor falla: no es un valor, es un nulo disfrazado. Se convierte a `null` y
se cuentan los nulos por columna **antes y después**. Las lecturas sin distancia ni nivel se marcan inválidas (no se
borran del bronce: se excluyen de los cálculos de nivel).

**Duplicados.** Kafka entrega *al-menos-una-vez*: la misma lectura (`eventId`) puede llegar dos veces. Se deduplica de
forma **determinista** con `Window.partitionBy("eventId").orderBy(ingestaTs, kafka_offset)` + `row_number() == 1`:
siempre se queda la primera copia que entró, en cualquier corrida.

> **Para explicarlo:** "Si no quito duplicados antes de calcular, una llegada de agua se contaría dos veces y el
> intervalo entre llegadas saldría más corto. No se nota a simple vista: por eso lo mido antes y después."
""")

code("""
prueba = yaml.safe_load(open(RAIZ / "ingesta" / "sensores_prueba.yml", encoding="utf-8"))
EXCLUIDOS = [s["sensorId"] for s in prueba.get("sensores", [])]
base = lecturas.filter(~F.col("sensorId").isin(EXCLUIDOS))

def nulos(df, cols):
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols])

COLS = ["eventId", "timestamp", "ingestaTs", "distanciaCm", "nivelPct", "temperaturaC", "humedadPct"]
antes_nulos = nulos(base.filter(F.col("sensorId") == REAL), COLS).toPandas().assign(etapa="antes")

# Tratamiento: el -1 del sensor es un nulo; lecturas sin medida de nivel quedan marcadas como inválidas
tratada = (base
    .withColumn("distanciaCm", F.when(F.col("distanciaCm") == -1, None).otherwise(F.col("distanciaCm")))
    .withColumn("temperaturaC", F.when(F.col("temperaturaC") == -1, None).otherwise(F.col("temperaturaC")))
    .withColumn("humedadPct", F.when(F.col("humedadPct") == -1, None).otherwise(F.col("humedadPct")))
    .withColumn("nivel_pct", F.when(F.col("sensorId") == REAL, (F.lit(FONDO) - F.col("distanciaCm")) / F.lit(ALTO) * 100)
                              .otherwise(F.col("nivelPct"))))
despues_nulos = nulos(tratada.filter(F.col("sensorId") == REAL), COLS).toPandas().assign(etapa="después (-1 → null)")
print("Nulos por columna en el ESP32:")
display(pd.concat([antes_nulos, despues_nulos]).set_index("etapa"))
print("El tratamiento revela los fallos del sensor que el -1 escondía: pasan a null y no entran a promedios ni al detector de nivel.")
""")

code("""
# Duplicados: determinista con Window + row_number()
w_dup = Window.partitionBy("eventId").orderBy(F.col("ingestaTs").asc_nulls_last(), F.col("kafka_offset").asc_nulls_last())
con_orden = tratada.withColumn("copia", F.row_number().over(w_dup))
plata = con_orden.filter(F.col("copia") == 1).drop("copia")

esp32_imposible = (F.col("sensorId") == REAL) & (F.col("distanciaCm").isNull() | ~F.col("distanciaCm").between(2, 450)
                                                 | (F.col("distanciaCm") > FONDO + 10))
plata = (plata.withColumn("valida_plata", F.col("lectura_valida") & ~esp32_imposible & F.col("nivel_pct").isNotNull())
              .withColumn("zona_ciega", (F.col("sensorId") == REAL) & F.col("distanciaCm").between(2, 25))
              .cache())

n_antes, n_desp = base.count(), plata.count()
d_antes, d_desp = base.select("eventId").distinct().count(), plata.select("eventId").distinct().count()
display(pd.DataFrame({"lecturas": [n_antes, n_desp], "eventId distintos": [d_antes, d_desp],
                      "copias repetidas": [n_antes - d_antes, n_desp - d_desp]}, index=["antes", "después (row_number = 1)"]))
display(plata.groupBy("sensorId").agg(
    F.count("*").alias("lecturas"),
    F.round(F.avg(F.col("valida_plata").cast("double")), 4).alias("ratio_validas"),
    F.round(F.avg(F.col("zona_ciega").cast("double")), 4).alias("ratio_zona_ciega")).toPandas())
print("Zona ciega del JSN-SR04T (< 25 cm = tanque casi lleno): se marca y se informa, no se borra.")
""")

md("""
## 4. Latencia de ingesta (p50/p95), nivel del tanque y huecos del sensor

- **Latencia** = `ingestaTs − timestamp`. Se informan p50 y p95, no solo el promedio: las lecturas que el puente
  recupera tras apagar la PC llegan con días de atraso y desfiguran el promedio.
- **Nivel** = `(sensor_al_fondo − distancia) / altura × 100` con la geometría provisional de `ingesta/tanques.yml`.
- **Huecos:** con `lag()` sobre una ventana por sensor, los saltos de más de 30 min entre lecturas seguidas.

> **Para explicarlo:** "El pipeline entrega en segundos (p50 en vivo); los atrasos de horas son lecturas recuperadas.
> Los huecos son las franjas grises del tablero."
""")

code("""
lat = (plata.filter(F.col("latencia_ingesta_s").isNotNull()).groupBy("sensorId").agg(
        F.count("*").alias("n"), F.round(F.avg("latencia_ingesta_s"), 1).alias("promedio_s"),
        F.round(F.percentile_approx("latencia_ingesta_s", 0.5, 10000), 1).alias("p50_s"),
        F.round(F.percentile_approx("latencia_ingesta_s", 0.95, 10000), 1).alias("p95_s"),
        F.round(F.avg((F.col("latencia_ingesta_s") <= 60).cast("double")), 3).alias("frac_en_vivo")).toPandas())
display(lat)

nivel = plata.filter((F.col("sensorId") == REAL) & F.col("valida_plata"))
por_minuto = (nivel.groupBy(F.window(F.timestamp_millis("timestamp"), "1 minute").alias("w"))
              .agg(F.percentile_approx("nivel_pct", 0.5).alias("nivel_pct"))
              .select(F.col("w.start").alias("minuto"), "nivel_pct").orderBy("minuto").toPandas())
por_minuto["minuto"] = pd.to_datetime(por_minuto["minuto"], utc=True).dt.tz_convert(LIMA)
serie = por_minuto.set_index("minuto")["nivel_pct"].resample("1min").mean()
import matplotlib.dates as mdates
fig, ax = plt.subplots(figsize=(13, 3.6))
ax.plot(serie.index, serie.values, lw=0.8, color="tab:green")
ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m", tz=serie.index.tz))
ax.set_ylim(0, 100); ax.set_ylabel("nivel %"); ax.set_title(f"{REAL}: nivel por minuto (los cortes son huecos del sensor)")
plt.tight_layout(); plt.show()
u = nivel.orderBy(F.desc("timestamp")).first()
print(f"Última lectura válida: {pd.Timestamp(u['timestamp'], unit='ms', tz='UTC').tz_convert(LIMA):%d/%m %H:%M:%S} · "
      f"distancia {u['distanciaCm']:.1f} cm · nivel {u['nivel_pct']:.1f} % · {u['nivel_pct'] / 100 * CAP:.0f} L  (comparar con Grafana)")

w_s = Window.partitionBy("sensorId").orderBy("timestamp")
huecos = (plata.filter(F.col("sensorId") == REAL).select("timestamp")
          .withColumn("previo", F.lag("timestamp").over(Window.orderBy("timestamp")))
          .withColumn("horas", (F.col("timestamp") - F.col("previo")) / 3_600_000)
          .filter(F.col("horas") > 0.5).withColumn("horas", F.round("horas", 1)).orderBy("previo").toPandas())
huecos["desde"] = pd.to_datetime(huecos["previo"], unit="ms", utc=True).dt.tz_convert(LIMA).dt.strftime("%d/%m %H:%M")
huecos["hasta"] = pd.to_datetime(huecos["timestamp"], unit="ms", utc=True).dt.tz_convert(LIMA).dt.strftime("%d/%m %H:%M")
total_h = (pd.Timestamp.now(tz=LIMA) - DESDE).total_seconds() / 3600
print(f"{len(huecos)} huecos de más de 30 min · {huecos['horas'].sum():.0f} h sin datos de {total_h:.0f} h")
huecos[["desde", "hasta", "horas"]]
""")

md("""
## 5. Capa oro: las llegadas de agua (el indicador de D4)

Las llegadas se detectan con el **mismo código** del streaming (`streaming/detector_servicio.py`, reglas congeladas
f4.4): el nivel sube más del 6 % de la capacidad en 15 min (o el criterio lento/sostenido), confirmado en 3
medianas seguidas. Así batch y streaming dan la misma respuesta. Después, con Spark: horas entre llegadas
(p50/p95), hora del día y caudal de llenado.

> **Para explicarlo:** "La capa oro es la respuesta del negocio: cuándo llegó el agua, cada cuánto y con qué caudal.
> Lo compruebo con Grafana: en los últimos 7 días debe dar las mismas llegadas, intervalo y caudal."
""")

code("""
from detector_servicio import Configuracion, Detector

def detectar(pdf):
    \"\"\"Corre el detector congelado por sensor, en lotes de 10 min de tiempo de evento (como micro-lotes).\"\"\"
    det = Detector(Configuracion(str(RAIZ / "streaming" / "reglas_servicio.yml"), str(RAIZ / "ingesta" / "tanques.yml")))
    eventos = []
    for sensor, df in pdf.sort_values("timestamp").groupby("sensorId"):
        lec = [{k: (None if pd.isna(v) else v) for k, v in r.items()} for r in df.to_dict("records")]
        estado, j = None, 0
        while j < len(lec):
            k = j
            while k < len(lec) and lec[k]["timestamp"] - lec[j]["timestamp"] < 600_000:
                k += 1
            evs, estado = det.procesar(sensor, lec[j:k], estado, "batch", int(time.time() * 1000))
            eventos += evs or []
            j = k
    return pd.DataFrame([{"eventoId": e["eventoId"], "sensorId": e["sensorId"], "inicioTs": e["inicioTs"],
                          "subida_pct": e["valor"], "criterio": e["detalle"].get("criterio"),
                          "caudal_L_min": e["detalle"].get("caudalLlenadoLpm")}
                         for e in eventos if e["tipo"] == "llegada_agua"])

COLS_DET = ["sensorId", "timestamp", "distanciaCm", "nivelPct", "nivelM", "temperaturaC", "lectura_valida"]
llegadas_pd = detectar(plata.select(*COLS_DET).toPandas())
w = Window.partitionBy("sensorId").orderBy("inicioTs")
oro = (spark.createDataFrame(llegadas_pd)
       .withColumn("hora_lima", F.from_utc_timestamp(F.timestamp_millis("inicioTs"), LIMA))
       .withColumn("fecha", F.to_date("hora_lima"))
       .withColumn("horas_desde_anterior", F.round((F.col("inicioTs") - F.lag("inicioTs").over(w)) / 3_600_000, 2))
       .cache())
kpi = (oro.groupBy("sensorId").agg(
        F.count("*").alias("llegadas"),
        F.round(F.percentile_approx("horas_desde_anterior", 0.5), 2).alias("intervalo_p50_h"),
        F.round(F.percentile_approx("horas_desde_anterior", 0.95), 2).alias("intervalo_p95_h"),
        F.round(F.percentile_approx("caudal_L_min", 0.5), 1).alias("caudal_mediano_L_min"),
        F.date_format(F.max("hora_lima"), "dd/MM HH:mm").alias("ultima_llegada")).toPandas())
display(kpi)
display(oro.filter(F.col("sensorId") == REAL).orderBy("inicioTs")
          .select(F.date_format("hora_lima", "dd/MM HH:mm").alias("llegada (Lima)"), "subida_pct", "caudal_L_min",
                  "criterio", "horas_desde_anterior").toPandas())
hace7 = int((pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=7)).timestamp() * 1000)
k7 = oro.filter((F.col("sensorId") == REAL) & (F.col("inicioTs") >= hace7)).toPandas().sort_values("inicioTs")
print(f"ESP32, últimos 7 días (comparar con Grafana): {len(k7)} llegadas · intervalo mediano "
      f"{(k7['inicioTs'].diff().dropna() / 3_600_000).median():.1f} h · caudal mediano {k7['caudal_L_min'].median():.1f} L/min")
""")

code("""
horas = oro.filter(F.col("sensorId") == REAL).groupBy(F.hour("hora_lima").alias("hora")).count().orderBy("hora").toPandas()
iv = oro.filter((F.col("sensorId") == REAL) & F.col("horas_desde_anterior").isNotNull()).select("horas_desde_anterior").toPandas()
fig, axs = plt.subplots(1, 2, figsize=(13, 3.4))
axs[0].bar(horas["hora"], horas["count"], color="tab:blue"); axs[0].set_xticks(range(0, 24, 2))
axs[0].set_title("ESP32: a qué hora del día llega el agua (Lima)")
axs[1].hist(iv["horas_desde_anterior"], bins=20, color="tab:orange"); axs[1].set_title("Horas entre llegadas")
for q, ls in [(0.5, "-"), (0.95, "--")]:
    if len(iv):
        axs[1].axvline(iv["horas_desde_anterior"].quantile(q), color="k", ls=ls, label=f"p{int(q * 100)}")
axs[1].legend(); plt.tight_layout(); plt.show()
""")

md("""
## 6. Salida particionada en Parquet, lectura de vuelta y `PartitionFilters`

Se particiona por **`fecha`** (baja cardinalidad: un directorio por día) y no por `eventId` (un directorio por
lectura: millones de carpetas diminutas, lectura lentísima). Al filtrar por fecha, Spark **solo abre la carpeta de ese
día**: el plan físico lo muestra como `PartitionFilters`.

> **Para explicarlo:** "Escribo la plata y el oro particionados por fecha, los vuelvo a leer y compruebo que el conteo
> coincide. En el plan se ve `PartitionFilters: [... fecha = ...]`: Spark no lee los otros días."
""")

code("""
d_plata, d_oro = SALIDA / "plata_d4", SALIDA / "oro_d4_llegadas"
(plata.withColumn("fecha", F.to_date(F.from_utc_timestamp(F.timestamp_millis("timestamp"), LIMA)))
      .select("eventId", "sensorId", "origen", "timestamp", "distanciaCm", "nivel_pct", "temperaturaC", "humedadPct",
              "latencia_ingesta_s", "valida_plata", "zona_ciega", "fecha")
      .write.mode("overwrite").partitionBy("fecha").parquet(str(d_plata)))
oro.drop("hora_lima").write.mode("overwrite").partitionBy("fecha").parquet(str(d_oro))

releida = spark.read.parquet(str(d_plata))
print(f"Plata escrita {plata.count():,} · releída {releida.count():,} · carpetas fecha= {len(list(d_plata.glob('fecha=*')))}")
print(f"Oro escrito {oro.count()} · releído {spark.read.parquet(str(d_oro)).count()}")
un_dia = sorted(p.name.split("=")[1] for p in d_plata.glob("fecha=*"))[-2]
consulta = releida.filter(F.col("fecha") == un_dia)
print(f"\\nLectura de un solo día ({un_dia}): {consulta.count():,} filas. Plan físico:")
plan = consulta._jdf.queryExecution().executedPlan().toString()
print(plan)
print("¿PartitionFilters en el plan?", "PartitionFilters: [" in plan and "fecha" in plan.split("PartitionFilters: [")[1].split("]")[0])
""")

md("""
## 7. ML distribuido con Spark MLlib: horas hasta la próxima llegada

**Variable objetivo (el indicador de D4):** cada 15 min, ¿cuántas horas faltan para la próxima llegada de agua?

**Datos:** se construyen con Spark a partir de la capa oro (llegadas) y la plata (nivel):

- Se toman los intervalos entre dos llegadas seguidas **observadas sin huecos** (si el sensor se apagó en medio, no
  se sabe si hubo otra llegada: ese intervalo no sirve).
- **Volumen:** las llegadas reales son pocas (el ESP32 está instalado desde el 29-oct), así que el entrenamiento usa
  la **flota extrapolada** (datos aumentados, 4 tanques × 30 días, `batch/datos/aumentados/runId=r2026-45facae2`), y
  las llegadas reales del ESP32 y 1233810 se evalúan aparte, como **evidencia indicativa**.
- **División cronológica**, nunca aleatoria: se entrena con el primer 75 % del tiempo y se prueba con el último 25 %.

**Variables:** hora del día (seno y coseno), día de la semana, horas desde la última llegada, intervalo anterior,
nivel actual y cambio del nivel en la última hora.

**Modelos comparados:** línea base de **persistencia del ritmo** (intervalo mediano − horas transcurridas, armada
como `Pipeline` de MLlib con `SQLTransformer`), regresión lineal con regularización (elastic net) y Random Forest.
Métricas: **MAE, RMSE y R²**. Gana el de menor MAE en la prueba, **aunque sea la línea base**: un modelo que no le
gana a la persistencia no aporta, y eso también es un resultado.

> **Para explicarlo:** "`VectorAssembler` junta las columnas en un solo vector porque MLlib trabaja sobre una
> columna `features` distribuida, no sobre una matriz en memoria como scikit-learn. `fit()` entrena y `transform()`
> agrega la predicción. Guardo el modelo que gana la comparación, y lo compruebo releyéndolo."
""")

code("""
# 7.1 Llegadas de la flota extrapolada (datos aumentados) con el mismo detector
FLOTA = RAIZ / "batch" / "datos" / "aumentados" / "runId=r2026-45facae2"
flota = (spark.read.parquet(str(FLOTA))
         .withColumn("nivelM", F.lit(None).cast("double"))
         .withColumn("lectura_valida", F.col("distanciaCm").between(2, 450) | F.col("nivelPct").isNotNull()))
t0 = time.time()
llegadas_flota = detectar(flota.select(*COLS_DET).toPandas())
print(f"Flota: {flota.count():,} lecturas · {len(llegadas_flota)} llegadas detectadas · {time.time() - t0:.0f} s")
lecturas_ml = (flota.select("sensorId", "timestamp", F.col("nivelPct").alias("nivel_pct"), F.lit("flota (aumentado)").alias("conjunto"))
               .unionByName(plata.filter("valida_plata").select("sensorId", "timestamp", "nivel_pct", F.lit("real").alias("conjunto"))))
todas = spark.createDataFrame(pd.concat([llegadas_flota.assign(conjunto="flota (aumentado)"), llegadas_pd.assign(conjunto="real")]))
""")

code("""
# 7.2 Dataset: un punto cada 15 min entre dos llegadas seguidas observadas sin huecos
PASO = 15 * 60_000
w = Window.partitionBy("sensorId").orderBy("inicioTs")
pares = (todas.select("sensorId", "conjunto", "inicioTs")
         .withColumn("siguiente", F.lead("inicioTs").over(w))
         .withColumn("intervalo_previo_h", (F.col("inicioTs") - F.lag("inicioTs").over(w)) / 3_600_000)
         .filter(F.col("siguiente").isNotNull()))
puntos = (pares.withColumn("t", F.explode(F.sequence(F.col("inicioTs"), F.col("siguiente") - 1, F.lit(PASO))))
               .withColumn("y_horas", (F.col("siguiente") - F.col("t")) / 3_600_000)
               .withColumn("horas_desde_ultima", (F.col("t") - F.col("inicioTs")) / 3_600_000))
# nivel por bloque de 15 min (mediana) y el de una hora antes
nivel15 = (lecturas_ml.withColumn("t", F.col("timestamp") - F.col("timestamp") % PASO)
           .groupBy("sensorId", "t").agg(F.percentile_approx("nivel_pct", 0.5).alias("nivel")))
puntos = (puntos.withColumn("t", F.col("t") - F.col("t") % PASO)
          .join(nivel15, ["sensorId", "t"], "left")
          .join(nivel15.select("sensorId", (F.col("t") + 4 * PASO).alias("t"), F.col("nivel").alias("nivel_1h_antes")),
                ["sensorId", "t"], "left"))
# intervalo observado sin huecos: como mucho 4 bloques de 15 min sin lecturas (1 h)
sin_huecos = (puntos.groupBy("sensorId", "inicioTs").agg(F.sum(F.col("nivel").isNull().cast("int")).alias("bloques_sin_dato"))
              .filter(F.col("bloques_sin_dato") <= 4))
hora = F.from_utc_timestamp(F.timestamp_millis("t"), LIMA)
ds = (puntos.join(sin_huecos.select("sensorId", "inicioTs"), ["sensorId", "inicioTs"])
      .filter(F.col("nivel").isNotNull())
      .withColumn("hora_dia", F.hour(hora) + F.minute(hora) / 60)
      .withColumn("hora_sin", F.sin(2 * np.pi * F.col("hora_dia") / 24))
      .withColumn("hora_cos", F.cos(2 * np.pi * F.col("hora_dia") / 24))
      .withColumn("dia_semana", F.dayofweek(hora).cast("double"))
      .withColumn("cambio_nivel_1h", F.coalesce(F.col("nivel") - F.col("nivel_1h_antes"), F.lit(0.0)))
      .cache())
med_prev = ds.agg(F.percentile_approx("intervalo_previo_h", 0.5)).first()[0]
ds = ds.fillna({"intervalo_previo_h": med_prev})
display(ds.groupBy("conjunto").agg(F.count("*").alias("puntos_15min"), F.countDistinct("sensorId", "inicioTs").alias("intervalos"),
                                   F.round(F.avg("y_horas"), 1).alias("y_media_h")).toPandas())
""")

code("""
# 7.3 División cronológica (flota): primer 75 % del tiempo = entrenamiento, último 25 % = prueba
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator

fl = ds.filter(F.col("conjunto") == "flota (aumentado)")
corte = fl.agg(F.percentile_approx("t", 0.75)).first()[0]
train, test = fl.filter(F.col("t") < corte), fl.filter(F.col("t") >= corte)
real = ds.filter(F.col("conjunto") == "real")
print(f"Corte cronológico: {pd.Timestamp(corte, unit='ms', tz='UTC').tz_convert(LIMA):%d/%m %H:%M} · "
      f"entrenamiento {train.count():,} · prueba {test.count():,} · real (indicativo) {real.count():,}")

VARIABLES = ["hora_sin", "hora_cos", "dia_semana", "horas_desde_ultima", "intervalo_previo_h", "nivel", "cambio_nivel_1h"]
ensamblador = VectorAssembler(inputCols=VARIABLES, outputCol="features")
candidatos = {
    "Regresión lineal (elastic net)": LinearRegression(featuresCol="features", labelCol="y_horas", regParam=0.1, elasticNetParam=0.5),
    "Random Forest": RandomForestRegressor(featuresCol="features", labelCol="y_horas", numTrees=60, maxDepth=8, seed=2026),
}
modelos = {n: Pipeline(stages=[ensamblador, est]).fit(train) for n, est in candidatos.items()}   # fit()

# Línea base de persistencia del ritmo, también como Pipeline de MLlib (así se puede guardar si gana):
# "la próxima llegada será un intervalo mediano después de la última"; el intervalo se aprende SOLO del entrenamiento.
intervalo_mediano = train.agg(F.percentile_approx("intervalo_previo_h", 0.5)).first()[0]
BASE = f"Base: persistencia del ritmo ({intervalo_mediano:.1f} h − transcurridas)"
modelos[BASE] = Pipeline(stages=[SQLTransformer(statement=
    f"SELECT *, GREATEST({intervalo_mediano} - horas_desde_ultima, 0.0) AS prediction FROM __THIS__")]).fit(train)

def metricas(pred):
    ev = lambda m: RegressionEvaluator(labelCol="y_horas", predictionCol="prediction", metricName=m).evaluate(pred)
    return {"MAE_h": round(ev("mae"), 3), "RMSE_h": round(ev("rmse"), 3), "R2": round(ev("r2"), 3)}

filas = []
for conjunto, df in (("prueba flota", test), ("real (indicativo)", real)):
    if df.count() == 0:
        continue
    for n, m in modelos.items():
        filas.append({"modelo": n, "conjunto": conjunto, **metricas(m.transform(df))})   # transform()
comparacion = pd.DataFrame(filas)
display(comparacion)
""")

code("""
# 7.4 Ganador (menor MAE en la prueba cronológica de la flota, línea base incluida), importancia de variables y guardado
prueba_fl = comparacion[comparacion["conjunto"] == "prueba flota"]
ganador = prueba_fl.sort_values("MAE_h").iloc[0]["modelo"]
print("Ganador:", ganador)
if ganador == BASE:
    print("Ningún modelo de MLlib le gana a la persistencia del ritmo: el ML no aporta sobre esta variable con estos datos. "
          "Se guarda el ganador real (la base, como Pipeline de MLlib), no el modelo más complejo.")
rf = modelos["Random Forest"].stages[-1]
display(pd.Series(rf.featureImportances.toArray(), index=VARIABLES).sort_values(ascending=False).round(3).to_frame("importancia (Random Forest)"))
lr = modelos["Regresión lineal (elastic net)"].stages[-1]
display(pd.Series(lr.coefficients.toArray(), index=VARIABLES).round(3).to_frame("coeficiente (lineal)"))

d_modelo = SALIDA / "modelo_d4_mllib"
modelos[ganador].write().overwrite().save(str(d_modelo))
json.dump({"ganador": ganador, "metricas_prueba": prueba_fl.set_index("modelo").loc[ganador].to_dict(),
           "variables": VARIABLES, "corte_cronologico_ms": int(corte)},
          open(SALIDA / "modelo_d4_mllib.json", "w", encoding="utf-8"), ensure_ascii=False, indent=2, default=str)

recargado = PipelineModel.load(str(d_modelo))
m_rec = metricas(recargado.transform(test))
print("Métricas del modelo releído del disco:", m_rec)
print("¿El modelo guardado es el ganador?", m_rec == {k: prueba_fl.set_index("modelo").loc[ganador][k] for k in ("MAE_h", "RMSE_h", "R2")})
""")

code("""
p = recargado.transform(test).select("t", "sensorId", "y_horas", "prediction").toPandas()
s = p["sensorId"].value_counts().index[0]
g = p[p["sensorId"] == s].sort_values("t")
fig, ax = plt.subplots(figsize=(13, 3.4))
ax.plot(pd.to_datetime(g["t"], unit="ms"), g["y_horas"], "k-", lw=1, label="real: horas hasta la próxima llegada")
ax.plot(pd.to_datetime(g["t"], unit="ms"), g["prediction"], "--", color="tab:red", lw=1.2, label=f"predicción ({ganador})")
ax.set_ylabel("horas"); ax.set_title(f"Prueba cronológica · {s} (datos aumentados)"); ax.legend()
plt.tight_layout(); plt.show()
print("El modelo de producción de D4 es d4.1 (supervivencia Weibull, notebooks/u2/d4_llegadas.ipynb): da además la "
      "ventana del 80 % y la probabilidad de llegada. Este modelo MLlib es el componente distribuido de la U1 sobre la misma variable.")
""")

md("""
## 7b. El modelo de producción de D4 (supervivencia) reentrenado con todas las llegadas del ESP32 y de la API

El modelo que usa el streaming es **d4.1** (U2): supervivencia en tiempo discreto por hora. En cada hora se estima la
probabilidad de que llegue agua según **las horas desde la última llegada** y **la hora del día**; de ahí salen la
hora más probable, la ventana P10-P90 y la probabilidad en 6/12/24 h. d4.1 se entrenó el 29-oct: entonces el ESP32
tenía muy pocas llegadas y quedó **sin modelo** (en producción aparece con un motivo).

Aquí se reentrena **con el mismo código de la U2** (`batch/modelo_d4.py` → `persona_periodo`, `intervalos`;
`batch/llegadas.py` → `ModeloLlegadas.ajustar_km` / `ajustar_weibull`), un modelo por tanque, con **todas** las
llegadas guardadas en el bronce:

- **ESP32** (sensor propio, IoT): todo su historial, también el del HC-SR04 anterior al 29-oct (mismo tanque).
- **ThingSpeak 1233810** (API pública): todo lo que trajo el colector.

**Evaluación cronológica** por tanque (75 % inicial para entrenar, 25 % final para probar) contra dos líneas base de la
U2: *mediana del intervalo* y *misma hora que la última llegada*. Con tan pocas llegadas reales, el resultado es
**indicativo**. El modelo final (con todos los datos) se guarda en `data/local/u3/modelo_d4_u3/modelo.json`, con el
**mismo formato que d4.1**, para que el notebook de streaming lo aplique al ESP32. **No reemplaza a d4.1 en
producción**: eso se decide en equipo (sería d4.2).

> **Para explicarlo:** "Es el mismo modelo de la U2, con el mismo código; solo lo reentreno con las llegadas que
> el ESP32 y la API acumularon desde entonces. Lo evalúo con división cronológica contra las líneas base, y lo guardo
> para usarlo en el streaming."
""")

code("""
sys.path.insert(0, str(RAIZ / "batch"))
import modelo_d4 as m4                                   # el script de la U2: persona_periodo, intervalos, CFG
from llegadas import ModeloLlegadas
H = 3_600_000
C = m4.CFG["modelo"]

# Todo el bronce de los dos tanques (sin filtro de fecha), con la misma deduplicación determinista
rutas_todas = sorted(p for o in ["esp32-fisico", "thingspeak"] for p in glob.glob(str(BRONCE / f"origen={o}" / "fecha=*")))
hist = (spark.read.schema(ESQUEMA_BRONCE).option("basePath", str(BRONCE)).parquet(*rutas_todas)
        .filter(F.col("sensorId").isin(SENSORES))
        .withColumn("copia", F.row_number().over(Window.partitionBy("eventId").orderBy(F.col("ingestaTs").asc_nulls_last())))
        .filter("copia = 1").drop("copia"))
hist_pd = hist.select(*COLS_DET).toPandas()
ll_hist = detectar(hist_pd)
ll_hist = ll_hist.assign(fuente="real", runId="", ts=ll_hist["inicioTs"])
desde_tanque = hist_pd.groupby("sensorId")["timestamp"].min()
print("Historial usado:", {s: pd.Timestamp(t, unit="ms", tz="UTC").tz_convert(LIMA).strftime("%d/%m/%Y") for s, t in desde_tanque.items()})
print("Llegadas por tanque:", ll_hist.groupby("sensorId").size().to_dict())

# Horas observadas de cada tanque (con al menos una lectura válida) y corte cronológico 75/25 por tanque
horas = (hist_pd[hist_pd["lectura_valida"].astype("boolean").fillna(False)].assign(slot=lambda d: d["timestamp"] // H)
         [["sensorId", "slot"]].drop_duplicates().sort_values(["sensorId", "slot"]).reset_index(drop=True))
horas["fuente"], horas["runId"] = "real", ""
corte_h = horas.groupby("sensorId")["slot"].quantile(0.75).rename("corte")
horas = horas.join(corte_h, on="sensorId")
horas["particion"] = np.where(horas["slot"] < horas["corte"], "entrenamiento", "prueba")
horas["conj"] = horas["particion"]
pp = m4.persona_periodo(horas.drop(columns="corte"), ll_hist[["fuente", "sensorId", "runId", "ts"]])
display(pp.groupby(["sensorId", "particion"]).agg(horas_en_riesgo=("e", "size"), llegadas=("evento_hora", "sum"),
                                                  episodios=("episodio", "nunique")))
""")

code("""
def ajustar_tanque(g):
    \"\"\"Como modelo_d4.ajustar: Kaplan-Meier siempre; Weibull si hay al menos weibull_min_intervalos intervalos.\"\"\"
    if g["evento_hora"].sum() < C["min_llegadas_entrenamiento"]:
        return None
    t, d = m4.intervalos(g)
    km = ModeloLlegadas.ajustar_km(g["e"], g["hora"], g["evento_hora"], C["e_max_h"], C["iteraciones_ipf"], C["horizonte_h"])
    wb = (ModeloLlegadas.ajustar_weibull(g["e"], g["hora"], g["evento_hora"], t, d, C["e_max_h"], horizonte_h=C["horizonte_h"])
          if len(t) >= C["weibull_min_intervalos"] else None)
    return {"km": km, "weibull": wb, "mediana_intervalo_h": float(np.median(t[d])) if d.any() else None,
            "intervalos": int(len(t))}

filas_ev = []
for sid, g in pp.groupby("sensorId"):
    mod = ajustar_tanque(g[g["particion"] == "entrenamiento"])
    prueba = g[(g["particion"] == "prueba") & g["observado"]]
    if mod is None or prueba.empty:
        filas_ev.append({"sensorId": sid, "modelo": "—", "nota": "pocas llegadas en el entrenamiento o ninguna en la prueba"})
        continue
    m = mod["weibull"] or mod["km"]
    pred = np.array([m.predecir(e, h)["moda_h"] for e, h in zip(prueba["e"], prueba["hora"])])
    p10p90 = np.array([[m.predecir(e, h)["p10_h"], m.predecir(e, h)["p90_h"]] for e, h in zip(prueba["e"], prueba["hora"])])
    y = prueba["t_obs_h"].values
    hora_ult = ((prueba["ultima"].values - 5 * H) // H) % 24
    bases = {"base: mediana del intervalo": np.maximum(mod["mediana_intervalo_h"] - prueba["e"].values, 0.5),
             "base: misma hora que la última llegada": ((hora_ult - prueba["hora"].values) % 24) + 0.5}
    for nombre, p in [(f"d4 supervivencia ({m.tipo})", pred), *bases.items()]:
        filas_ev.append({"sensorId": sid, "modelo": nombre, "horas_prueba": len(y), "episodios_prueba": prueba["episodio"].nunique(),
                         "MAE_h": round(float(np.mean(np.abs(p - y))), 2), "RMSE_h": round(float(np.sqrt(np.mean((p - y) ** 2))), 2),
                         "cobertura_P10_P90": round(float(np.mean((y >= p10p90[:, 0]) & (y <= p10p90[:, 1]))), 2) if nombre.startswith("d4") else None})
display(pd.DataFrame(filas_ev))
print("Indicativo: pocas llegadas reales en la prueba (ver episodios_prueba). Si una línea base gana, se dice así: con pocas "
      "llegadas el modelo de supervivencia todavía no supera a las reglas simples. La conclusión sólida de D4 sigue siendo "
      "la de la U2 (flota: Weibull 1,6 h frente a 2,1 h de las bases).")
""")

code("""
# Modelo final con TODAS las horas (entrenamiento + prueba), en el formato de d4.1, para el streaming
final = {"version": "d4.1-u3", "fecha": pd.Timestamp.now(tz=LIMA).strftime("%Y%m%d"),
         "modelo": "supervivencia por hora (batch/llegadas.py), reentrenado en U3 con todas las llegadas del bronce",
         "uso_f7": "igual que d4.1; NO está en batch/modelos (producción sigue con d4.1)", "fuentes_llegadas": {"detector_f44": {}}}
for sid, g in pp.groupby("sensorId"):
    mod = ajustar_tanque(g)
    if mod is None:
        print(f"{sid}: sin modelo (menos de {C['min_llegadas_entrenamiento']} llegadas)"); continue
    final["fuentes_llegadas"]["detector_f44"][f"real|{sid}|"] = {
        "fuera_de_alcance": None, "km": mod["km"].a_dict(), "weibull": mod["weibull"].a_dict() if mod["weibull"] else None,
        "mediana_intervalo_h": mod["mediana_intervalo_h"], "ventana_q_h": None}
    m = mod["weibull"] or mod["km"]
    print(f"{sid}: {m.tipo} · {mod['intervalos']} intervalos · mediana del intervalo {mod['mediana_intervalo_h']:.1f} h")
d_mod = SALIDA / "modelo_d4_u3"
d_mod.mkdir(parents=True, exist_ok=True)
(d_mod / "modelo.json").write_text(json.dumps(final, ensure_ascii=False, indent=1), encoding="utf-8")
releido = json.loads((d_mod / "modelo.json").read_text(encoding="utf-8"))
print("Guardado y releído:", (d_mod / "modelo.json").relative_to(RAIZ), "·", list(releido["fuentes_llegadas"]["detector_f44"]))

# Ejemplo: qué diría ahora para el ESP32
clave = f"real|{REAL}|"
if clave in releido["fuentes_llegadas"]["detector_f44"]:
    e = releido["fuentes_llegadas"]["detector_f44"][clave]
    m = ModeloLlegadas.desde_dict(e["weibull"] or e["km"])
    ult = ll_hist[ll_hist["sensorId"] == REAL]["ts"].max()
    ahora = pd.Timestamp.now(tz="UTC")
    horas_desde = (ahora.value // 10**6 - ult) / H
    lect_desde = np.sort(hist_pd.loc[(hist_pd["sensorId"] == REAL) & (hist_pd["timestamp"] >= ult), "timestamp"].values)
    hueco_max = float(np.max(np.diff(np.append(lect_desde, ahora.value // 10**6))) / H) if len(lect_desde) else horas_desde
    print(f"ESP32: última llegada observada {pd.Timestamp(ult, unit='ms', tz='UTC').tz_convert(LIMA):%d/%m %H:%M} (hace {horas_desde:.0f} h)")
    if hueco_max > 1:
        print(f"  Sin pronóstico: hubo un hueco de {hueco_max:.0f} h del sensor después de esa llegada; pudo llegar agua sin "
              "que se viera. El modelo espera la próxima llegada observada (como producción: 'reanudación').")
    else:
        r = m.predecir(int(horas_desde), ahora.tz_convert(LIMA).hour)
        print(f"  Próxima llegada más probable en {r['moda_h']:.1f} h (P10-P90 {r['p10_h']:.1f}-{r['p90_h']:.1f} h) · prob. 24 h {r['prob_24h']:.0%}")
""")

md("""
## 8. Batch frente a streaming: ¿ven las mismas llegadas?

El streaming publicó sus llegadas en InfluxDB (`eventos_servicio`). Como el `eventoId` es determinístico
(`tipo-sensorId-inicioTs`), se comparan uno a uno.

- **Solo en batch:** lecturas que llegaron atrasadas y el watermark de 7 min del streaming descartó; el batch, que
  lee todo el bronce, sí las procesa. Es la razón de ser de la capa batch en Lambda.
- **Solo en streaming:** llegadas detectadas con un estado previo que el batch no tiene.

> **Para explicarlo:** "El batch revisa al streaming: lo que el stream no pudo ver por llegar tarde, el batch lo recupera."
""")

code("""
def leer_env():
    env = {}
    for linea in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in linea and not linea.lstrip().startswith("#"):
            k, v = linea.split("=", 1); env[k.strip()] = v.strip()
    return env

def flux(url, org, token, q):
    from io import StringIO
    req = urllib.request.Request(f"{url}/api/v2/query?org={org}", data=json.dumps({"query": q, "type": "flux"}).encode(),
                                 headers={"Authorization": f"Token {token}", "Content-Type": "application/json", "Accept": "application/csv"})
    texto = urllib.request.urlopen(req, timeout=20).read().decode()
    return pd.read_csv(StringIO(texto), comment="#") if texto.strip() else pd.DataFrame()

streaming_ids, error = None, None
try:
    env = leer_env()
    q = f'''from(bucket: "{env.get("INFLUX_BUCKET", "monitoraqua")}") |> range(start: {DESDE.tz_convert("UTC"):%Y-%m-%dT%H:%M:%SZ})
  |> filter(fn: (r) => r._measurement == "eventos_servicio" and r.tipo == "llegada_agua" and r._field == "eventoId")
  |> filter(fn: (r) => r.sensorId == "{REAL}" or r.sensorId == "{TS}") |> keep(columns: ["_value", "sensorId"])'''
    for url in ["http://influxdb:8086", "http://lambda26-influxdb:8086", "http://host.docker.internal:48086", "http://localhost:48086"]:
        try:
            r = flux(url, env.get("INFLUX_ORG", "horeb"), env["INFLUX_TOKEN"], q)
            streaming_ids = set(r["_value"]) if len(r) else set(); break
        except Exception as e:
            error = e
except Exception as e:
    error = e
if streaming_ids is None:
    print("No se pudo leer InfluxDB desde Jupyter:", error)
else:
    batch_ids = set(llegadas_pd["eventoId"])
    display(pd.DataFrame([{"sensorId": s, "batch": len(b := {i for i in batch_ids if f"-{s}-" in i}),
                           "streaming": len(st := {i for i in streaming_ids if f"-{s}-" in i}),
                           "coinciden": len(b & st), "solo_batch": len(b - st), "solo_streaming": len(st - b)} for s in SENSORES]))
""")

md("""
## 9. Respuestas a las preguntas de referencia (S5, Tabla 2)

**¿Por qué Kappa no necesita una capa batch separada, y qué se pierde frente a Lambda?** En Kappa todo es stream: para
reprocesar se re-lee el log de Kafka desde el inicio. Se gana un solo código; se pierde poder reprocesar más allá de
la retención de Kafka (aquí 7 días y sin volumen) y entrenar sobre la historia completa. Por eso MonitorAqua usa
Lambda: el bronce en Parquet guarda toda la historia (sección 1).

**Transformación frente a acción; por qué `explain(True)` muestra un plan sin ejecutar nada.** Las transformaciones
(`filter`, `withColumn`, `groupBy`) solo describen el cálculo; Catalyst las optimiza como plan. Solo una acción
(`show`, `count`, `write`) lo ejecuta. `explain` imprime ese plan, que todavía no corrió (sección 2).

**¿Qué pasa si no se resuelven los duplicados antes de integrar?** Una lectura repetida duplica su peso: una llegada
podría contarse dos veces, el intervalo entre llegadas saldría más corto y el modelo aprendería un ritmo falso. No se
nota a simple vista porque los valores son plausibles (sección 3).

**¿Por qué no particionar por un ID único?** Crearía una carpeta por lectura (cientos de miles de archivos diminutos)
y ningún filtro útil podría podar particiones. Conviene una columna de baja cardinalidad que se use al filtrar:
`fecha` (sección 6).

**¿Por qué `VectorAssembler` en MLlib y no en scikit-learn? ¿Y si guardo el primer modelo y no el ganador?** MLlib
recibe las variables como una sola columna vector distribuida en el clúster; scikit-learn recibe una matriz en
memoria. Guardar el primero en vez del ganador publicaría un modelo peor que el reportado: por eso la sección 7.4
relee el guardado y comprueba que sus métricas son las del ganador.

**¿Por qué comparar primero contra la persistencia, y qué significa no ganarle?** (también es pregunta de S10) La
persistencia ("el agua vuelve a llegar un intervalo típico después") es el pronóstico más barato posible. Si un
modelo no la supera, su complejidad no aporta nada y no debe publicarse como mejora: se reporta y se guarda la base.
En la flota el ritmo es casi regular y la base gana; en los datos reales nadie baja de unas 8 h de error, porque los
cortes de servicio no siguen el ritmo: es la limitación que d4.2 debe atacar (probabilidad de corte).
""")

code("""
resumen = {"lecturas_bronce": n_bronce, "lecturas_plata": plata.count(),
           "validas_plata": plata.filter("valida_plata").count(), "huecos_esp32": len(huecos),
           "llegadas_oro": {r["sensorId"]: int(r["llegadas"]) for _, r in kpi.iterrows()},
           "modelo_ganador": ganador, "metricas_modelo_guardado": m_rec}
print(json.dumps(resumen, indent=2, ensure_ascii=False))
spark.stop()
""")

nb = nbf.v4.new_notebook(cells=celdas, metadata={"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                                                 "language_info": {"name": "python"}})
nbf.write(nb, AQUI / "d4_batch.ipynb")
print("notebooks/u3/d4_batch.ipynb:", len(celdas), "celdas")
