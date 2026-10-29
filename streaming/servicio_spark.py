"""
Eventos de servicio en Spark (F4): el detector de streaming/detector_servicio.py dentro de
applyInPandasWithState, con un estado por sensorId, y el sumidero a Kafka (servicio-eventos) e InfluxDB
(eventos_servicio).

Por qué estado y no ventanas: las reglas son máquinas de estado por sensor con horizontes largos y desiguales
("horas desde la última llegada", "N inválidas seguidas", "la noche de hoy contra la tasa de las noches
anteriores"). Con ventanas habría que elegir una ventana enorme para el corte y otra distinta para cada regla, y aun
así no se puede expresar "desde la última llegada" ni "seguidas". applyInPandasWithState guarda por sensor solo lo
necesario (último llenado, rachas, una noche de medianas por minuto) y procesa cada lectura una vez.
"""
import json
import os
import time

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

import influx

REGLAS_PATH = os.getenv("REGLAS_SERVICIO_PATH", "/app/config/reglas_servicio.yml")
TANQUES_PATH = os.getenv("TANQUES_PATH", "/app/config/tanques.yml")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPICO_SERVICIO = os.getenv("KAFKA_TOPIC_SERVICIO", "servicio-eventos")
ORIGENES_TANQUE = ["esp32-fisico", "thingspeak", "flota-extrapolada"]
COLUMNAS_ENTRADA = ["sensorId", "timestamp", "distanciaCm", "nivelPct", "nivelM", "temperaturaC", "lectura_valida"]

SALIDA = StructType([
    StructField("eventoId", StringType()), StructField("tipo", StringType()), StructField("sensorId", StringType()),
    StructField("zona", StringType()), StructField("severidad", StringType()), StructField("inicioTs", LongType()),
    StructField("finTs", LongType()), StructField("valor", DoubleType()), StructField("umbral", DoubleType()),
    StructField("unidad", StringType()), StructField("origenLecturas", StringType()),
    StructField("detectadoTs", LongType()), StructField("versionRegla", StringType()),
    StructField("detalle", StringType()),  # JSON; en Kafka viaja como objeto
])
ESTADO = StructType([StructField("json", StringType())])
_DETECTOR = None


def _detector():
    """Un detector por proceso Python de Spark; relee la configuración cuando cambia el archivo."""
    global _DETECTOR
    if _DETECTOR is None:
        from detector_servicio import Configuracion, Detector
        _DETECTOR = Detector(Configuracion(REGLAS_PATH, TANQUES_PATH))
    return _DETECTOR


def detectar(origen_lecturas):
    def por_sensor(clave, lotes, estado):
        df = pd.concat(list(lotes)).sort_values("timestamp")
        lecturas = [{k: (None if pd.isna(v) else v) for k, v in fila.items()}
                    for fila in df.to_dict("records")]
        previo = json.loads(estado.get[0]) if estado.exists else None
        eventos, nuevo = _detector().procesar(clave[0], lecturas, previo, origen_lecturas, int(time.time() * 1000))
        estado.update((json.dumps(nuevo),))
        filas = [{**ev, "detalle": json.dumps(ev["detalle"], ensure_ascii=False)} for ev in eventos]
        # F5: una fila "_estado" por sensor y lote con la foto del estado (temperatura del agua estimada,
        # residencia). No es un evento: el sumidero la manda a InfluxDB estado_tanque, nunca a Kafka.
        if nuevo.get("ult_ts"):
            filas.append({"eventoId": f"_estado-{clave[0]}-{nuevo['ult_ts']}", "tipo": "_estado", "sensorId": clave[0],
                          "zona": "", "severidad": "info", "inicioTs": nuevo["ult_ts"], "finTs": nuevo["ult_ts"],
                          "valor": None, "umbral": None, "unidad": "", "origenLecturas": origen_lecturas,
                          "detectadoTs": int(time.time() * 1000), "versionRegla": "",
                          "detalle": json.dumps(nuevo, ensure_ascii=False)})
        yield pd.DataFrame(filas, columns=[c.name for c in SALIDA.fields])
    return por_sensor


def eventos_servicio(lecturas, origen_lecturas):
    """lecturas: DataFrame en streaming ya enriquecido (procesar_stream.enriquecer)."""
    return (lecturas.filter(F.col("origen").isin(ORIGENES_TANQUE))
            .select(*COLUMNAS_ENTRADA)
            .groupBy("sensorId")
            .applyInPandasWithState(detectar(origen_lecturas), SALIDA, ESTADO, "append", GroupStateTimeout.NoTimeout))


def a_servicio(topico=TOPICO_SERVICIO, a_influx=True, jsonl=None, log=print):
    """foreachBatch: publica en Kafka (clave sensorId) y en InfluxDB eventos_servicio (tiempo = inicioTs)."""
    def escribir(df, batch_id):
        import tablero
        filas = [{**r.asDict(), "detalle": json.loads(r.detalle) if r.detalle else None} for r in df.collect()]
        estados = [f for f in filas if f["tipo"] == "_estado"]
        eventos = [f for f in filas if f["tipo"] != "_estado"]
        if estados and a_influx:
            influx.escribir([tablero.linea_estado(f["sensorId"], f["detalle"], f["origenLecturas"]) for f in estados])
        if not eventos:
            return
        for ev in eventos:
            if ev["detalle"] is None:
                del ev["detalle"]
        spark = df.sparkSession
        (spark.createDataFrame([(e["sensorId"], json.dumps(e, ensure_ascii=False)) for e in eventos],
                               "key string, value string")
         .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", topico).save())
        if a_influx:
            influx.escribir([tablero.linea_evento(e) for e in eventos])  # F5: con los numéricos de detalle aplanados
        if jsonl:
            with open(jsonl, "a", encoding="utf-8") as f:
                for e in eventos:
                    f.write(json.dumps(e, ensure_ascii=False) + "\n")
        log(status="eventos_servicio", batchId=batch_id, topico=topico, eventos=len(eventos),
            tipos=sorted({e["tipo"] for e in eventos}))
    return escribir
