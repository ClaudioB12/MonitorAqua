"""
Spark Structured Streaming sobre aqua-eventos-real (MonitorAqua, F3).

Lee el tópico, agrega procedencia y calidad a cada lectura y arranca cuatro consultas, cada una con su
propio checkpoint:

  rapidas_agg   esp32-fisico + thingspeak        watermark 7 min,  ventanas de 1 min cada 30 s  -> InfluxDB lecturas_agg
  lentas_agg    ea-uk + usgs + open-meteo        watermark 90 min, ventanas de 15 min           -> InfluxDB lecturas_agg
  esp32_detalle cada lectura válida del ESP32     watermark 7 min                                -> InfluxDB lecturas_esp32
  bronce        todas las lecturas, válidas e inválidas, sin deduplicar                         -> Parquet origen/fecha

Las agregaciones y el detalle deduplican por eventId dentro del watermark (dropDuplicatesWithinWatermark).
Las lecturas inválidas se cuentan (n_invalidas) pero no entran a los promedios, mínimos ni máximos.
El progreso de cada consulta (filas, descartes por watermark, duplicados) se registra en el log y en
InfluxDB, measurement spark_progreso.

Los watermarks salen del atraso medido en Kafka el 2026-10-28 (docs/streaming.md): con el PC despierto,
ThingSpeak llega hasta ~5 min tarde (el colector consulta cada 5 min) y USGS hasta ~54 min.

Se lanza con spark-submit y el conector Kafka por --packages (ver infra/compose.yml).
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

import influx

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aqua-eventos-real")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/checkpoints")
BRONCE_DIR = os.getenv("BRONCE_DIR", "/datos/bronce")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")  # solo aplica a un checkpoint nuevo
SENSORES_PRUEBA_PATH = os.getenv("SENSORES_PRUEBA_PATH", "/app/config/sensores_prueba.yml")

ORIGENES_RAPIDOS = ["esp32-fisico", "thingspeak"]
ORIGENES_LENTOS = ["ea-uk", "usgs", "open-meteo"]
WATERMARK_RAPIDO = os.getenv("WATERMARK_RAPIDO", "7 minutes")
WATERMARK_LENTO = os.getenv("WATERMARK_LENTO", "90 minutes")

MEDIDAS = ["distanciaCm", "nivelPct", "nivelM", "caudalM3s", "temperaturaC", "humedadPct",
           "tempAguaC", "ph", "oxigenoMgL"]
# Rango físico aceptado por medida (inclusive). distanciaCm = -1 es el código de fallo del HC-SR04.
RANGOS = {
    "distanciaCm": (2, 450),     # HC-SR04 / JSN-SR04T
    "nivelPct": (0, 100),
    "nivelM": (-10, 100),        # stage de río (EA): puede ser negativo respecto del cero de la estación
    "caudalM3s": (0, 10000),
    "temperaturaC": (-40, 60),   # aire
    "humedadPct": (0, 100),
    "tempAguaC": (-5, 45),
    "ph": (0, 14),
    "oxigenoMgL": (0, 25),
}

ESQUEMA = StructType([
    StructField("tipoEvento", StringType()), StructField("eventId", StringType()),
    StructField("sensorId", StringType()), StructField("origen", StringType()),
    StructField("fuenteTipo", StringType()), StructField("timestamp", LongType()),
    StructField("ingestaTs", LongType()),
    *[StructField(m, DoubleType()) for m in MEDIDAS],
    StructField("lat", DoubleType()), StructField("lon", DoubleType()),
])


def sensores_prueba():
    """sensorId de prueba excluidos de los agregados (lista única: ingesta/sensores_prueba.yml)."""
    import yaml
    try:
        with open(SENSORES_PRUEBA_PATH, encoding="utf-8") as f:
            return [s["sensorId"] for s in (yaml.safe_load(f) or {}).get("sensores", [])]
    except FileNotFoundError:
        log(status="aviso", motivo=f"sin lista de sensores de prueba en {SENSORES_PRUEBA_PATH}")
        return []


def log(**campos):
    print(json.dumps({"service": "aqua-streaming", "ts": int(time.time() * 1000), **campos},
                     ensure_ascii=False, default=str), flush=True)


# --- Lectura, procedencia y calidad -------------------------------------------------------

def leer_eventos(spark, topico=KAFKA_TOPIC, starting_offsets=STARTING_OFFSETS):
    crudo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", topico)
             .option("startingOffsets", starting_offsets)
             .option("maxOffsetsPerTrigger", 20000)
             .option("failOnDataLoss", "false")  # la retención de 7 días puede borrar offsets viejos
             .load())
    ev = (crudo.select(
            F.from_json(F.col("value").cast("string"), ESQUEMA).alias("e"),
            F.col("partition").alias("kafka_particion"), F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"))
          .select("e.*", "kafka_particion", "kafka_offset", "kafka_ts"))
    return enriquecer(ev)


def enriquecer(ev):
    # Catálogo de procedencia
    ev = (ev.withColumn("tiempo_evento", F.timestamp_millis("timestamp"))
            .withColumn("es_dato_real", F.col("fuenteTipo").isin("iot-propio", "api-publica"))
            .withColumn("latencia_ingesta_s", (F.col("ingestaTs") - F.col("timestamp")) / 1000.0))
    # Calidad: primer motivo que invalida la lectura (null = válida)
    motivo = F.when(F.col("eventId").isNull() | F.col("timestamp").isNull(), F.lit("sin_eventId_o_timestamp"))
    motivo = motivo.when(F.col("distanciaCm") == -1, F.lit("distanciaCm_-1"))
    for medida, (lo, hi) in RANGOS.items():
        motivo = motivo.when(F.col(medida).isNotNull() & ~F.col(medida).between(lo, hi),
                             F.lit(f"{medida}_fuera_de_rango"))
    motivo = motivo.when(F.coalesce(*[F.col(m) for m in MEDIDAS]).isNull(), F.lit("sin_medidas"))
    return (ev.withColumn("motivo_invalidez", motivo)
              .withColumn("lectura_valida", F.col("motivo_invalidez").isNull()))


def agregar(ev, ventana, desliz):
    valida = F.col("lectura_valida")
    aggs = [
        F.count(F.lit(1)).alias("n_total"),
        F.sum(valida.cast("int")).alias("n_validas"),
        F.sum((~valida).cast("int")).alias("n_invalidas"),
        F.max(F.col("es_dato_real").cast("int")).alias("es_dato_real"),
        F.avg("latencia_ingesta_s").alias("latencia_ingesta_s_media"),
        F.max("latencia_ingesta_s").alias("latencia_ingesta_s_max"),
    ]
    for m in MEDIDAS:  # solo lecturas válidas entran a los estadísticos
        aggs.append(F.avg(F.when(valida, F.col(m))).alias(f"{m}_media"))
    for m in ("distanciaCm", "nivelPct", "nivelM"):
        aggs += [F.min(F.when(valida, F.col(m))).alias(f"{m}_min"),
                 F.max(F.when(valida, F.col(m))).alias(f"{m}_max")]
    return (ev.groupBy(F.window("tiempo_evento", ventana, desliz).alias("ventana"),
                       "sensorId", "origen", "fuenteTipo")
              .agg(*aggs))


# --- Sumideros InfluxDB --------------------------------------------------------------------

def a_influx_agg(etiqueta_ventana):
    def escribir_lote(df, batch_id):
        filas = df.collect()
        lineas = []
        for r in filas:
            d = r.asDict()
            ventana = d.pop("ventana")
            tags = {"sensorId": d.pop("sensorId"), "origen": d.pop("origen"),
                    "fuenteTipo": d.pop("fuenteTipo"), "ventana": etiqueta_ventana}
            d["es_dato_real"] = bool(d["es_dato_real"])
            d["ventana_fin_ms"] = int(ventana["end"].timestamp() * 1000)
            lineas.append(influx.linea("lecturas_agg", tags, d, ventana["start"].timestamp() * 1000))
        n = influx.escribir(lineas) if lineas else 0
        log(status="influx_write", measurement="lecturas_agg", ventana=etiqueta_ventana, batchId=batch_id, puntos=n)
    return escribir_lote


_DETECTOR_NIVEL = None


def _detector_nivel():
    """Detector congelado de F4, solo para Detector.nivel() (geometría + compensación); no se modifica."""
    global _DETECTOR_NIVEL
    if _DETECTOR_NIVEL is None:
        from servicio_spark import REGLAS_PATH, TANQUES_PATH
        from detector_servicio import Configuracion, Detector
        _DETECTOR_NIVEL = Detector(Configuracion(REGLAS_PATH, TANQUES_PATH))
    return _DETECTOR_NIVEL


def a_influx_esp32(df, batch_id):
    import tablero
    filas = df.select("sensorId", "timestamp", "eventId", "distanciaCm", "temperaturaC", "humedadPct",
                      "latencia_ingesta_s").collect()
    lineas = [influx.linea("lecturas_esp32", {"sensorId": r.sensorId},
                           {"distanciaCm": r.distanciaCm, "temperaturaC": r.temperaturaC,
                            "humedadPct": r.humedadPct, "latencia_ingesta_s": r.latencia_ingesta_s,
                            "eventId": r.eventId}, r.timestamp) for r in filas]
    # F5: nivel en % y litros, distancia cruda vs compensada (tablero BI, D2 y D6)
    det = _detector_nivel()
    lineas += [tablero.linea_nivel(det, r.asDict(), r.sensorId, "real") for r in filas]
    n = influx.escribir(lineas) if lineas else 0
    log(status="influx_write", measurement="lecturas_esp32+nivel_tanque", batchId=batch_id, puntos=n)


def a_influx_latencias(df, batch_id):
    """F5: latencia de ingesta por lectura y origen (p50/p95 en el tablero del pipeline). Sin estado."""
    filas = df.select("origen", "sensorId", "timestamp", "latencia_ingesta_s", "eventId").collect()
    lineas = [influx.linea("latencia_ingesta", {"origen": r.origen, "sensorId": r.sensorId},
                           {"s": r.latencia_ingesta_s, "eventId": r.eventId}, r.timestamp)
              for r in filas if r.latencia_ingesta_s is not None]
    n = influx.escribir(lineas) if lineas else 0
    log(status="influx_write", measurement="latencia_ingesta", batchId=batch_id, puntos=n)


class Progreso(StreamingQueryListener):
    """Progreso de cada micro-lote: al log y a InfluxDB (measurement spark_progreso) para la observabilidad."""

    def onQueryStarted(self, event):
        log(status="query_started", consulta=event.name, id=str(event.id), runId=str(event.runId))

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        ops = p.get("stateOperators", [])
        resumen = {
            "batchId": p["batchId"],
            "numInputRows": p.get("numInputRows", 0),
            "inputRowsPerSecond": float(p.get("inputRowsPerSecond") or 0.0),
            "processedRowsPerSecond": float(p.get("processedRowsPerSecond") or 0.0),
            "batchDurationMs": p.get("batchDuration", 0),
            "numRowsDroppedByWatermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
            "numDroppedDuplicateRows": sum(o.get("customMetrics", {}).get("numDroppedDuplicateRows", 0) for o in ops),
            "numRowsTotalEstado": sum(o.get("numRowsTotal", 0) for o in ops),
        }
        watermark = p.get("eventTime", {}).get("watermark")
        log(status="progress", consulta=p.get("name"), watermark=watermark,
            fuente=[{"start": s.get("startOffset"), "end": s.get("endOffset")} for s in p.get("sources", [])],
            **resumen)
        try:
            influx.escribir([influx.linea("spark_progreso", {"consulta": p.get("name")}, resumen,
                                          time.time() * 1000)])
        except Exception as ex:  # noqa: BLE001 - la métrica no debe tumbar la consulta
            log(status="influx_error", measurement="spark_progreso", error=str(ex))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log(status="query_terminated", id=str(event.id), excepcion=event.exception)


# --- Arranque ------------------------------------------------------------------------------

def main():
    spark = (SparkSession.builder.appName("monitoraqua-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")  # queda fijado en el checkpoint
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(Progreso())

    ev = leer_eventos(spark)
    cp = lambda nombre: f"{CHECKPOINT_DIR}/{nombre}"  # noqa: E731
    excluidos = sensores_prueba()
    # Bronce guarda todo lo que llega; los agregados y el detalle no cuentan los sensores de prueba
    reales = ev.filter(~F.col("sensorId").isin(excluidos)) if excluidos else ev

    rapidas = (reales.filter(F.col("origen").isin(ORIGENES_RAPIDOS))
                 .withWatermark("tiempo_evento", WATERMARK_RAPIDO)
                 .dropDuplicatesWithinWatermark(["eventId"]))
    lentas = (reales.filter(F.col("origen").isin(ORIGENES_LENTOS))
                .withWatermark("tiempo_evento", WATERMARK_LENTO)
                .dropDuplicatesWithinWatermark(["eventId"]))

    (agregar(rapidas, "1 minute", "30 seconds").writeStream.queryName("rapidas_agg")
        .outputMode("update").foreachBatch(a_influx_agg("1m"))
        .option("checkpointLocation", cp("rapidas_agg")).trigger(processingTime="30 seconds").start())

    (agregar(lentas, "15 minutes", "15 minutes").writeStream.queryName("lentas_agg")
        .outputMode("update").foreachBatch(a_influx_agg("15m"))
        .option("checkpointLocation", cp("lentas_agg")).trigger(processingTime="1 minute").start())

    (rapidas.filter((F.col("origen") == "esp32-fisico") & F.col("lectura_valida"))
        .writeStream.queryName("esp32_detalle").outputMode("append").foreachBatch(a_influx_esp32)
        .option("checkpointLocation", cp("esp32_detalle")).trigger(processingTime="10 seconds").start())

    (ev.withColumn("fecha", F.to_date("tiempo_evento"))
        .writeStream.queryName("bronce").format("parquet").outputMode("append")
        .partitionBy("origen", "fecha")
        .option("path", BRONCE_DIR).option("checkpointLocation", cp("bronce"))
        .trigger(processingTime="1 minute").start())

    # F5: latencia de ingesta por lectura (todas las fuentes reales). Consulta sin estado, checkpoint propio.
    (reales.writeStream.queryName("latencias").outputMode("append").foreachBatch(a_influx_latencias)
        .option("checkpointLocation", cp("latencias")).trigger(processingTime="30 seconds").start())

    # F4: eventos de servicio. Mismo watermark y deduplicación que las rápidas; estado por sensor en el detector.
    from servicio_spark import a_servicio, eventos_servicio
    tanques_reales = (reales.filter(F.col("origen").isin(ORIGENES_RAPIDOS))
                      .withWatermark("tiempo_evento", WATERMARK_RAPIDO)
                      .dropDuplicatesWithinWatermark(["eventId"]))
    (eventos_servicio(tanques_reales, "real").writeStream.queryName("eventos_servicio").outputMode("append")
        .foreachBatch(a_servicio(log=log)).option("checkpointLocation", cp("eventos_servicio"))
        .trigger(processingTime="10 seconds").start())

    log(status="started", consultas=[q.name for q in spark.streams.active], kafkaTopic=KAFKA_TOPIC,
        sensoresPruebaExcluidos=excluidos,
        watermarkRapido=WATERMARK_RAPIDO, watermarkLento=WATERMARK_LENTO, checkpoints=CHECKPOINT_DIR,
        bronce=BRONCE_DIR)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
