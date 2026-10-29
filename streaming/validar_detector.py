"""
Validación de las reglas de servicio con datos aumentados (F4).

Reproduce aqua-eventos-aumentados (trigger availableNow) por el MISMO detector que corre en vivo, con
origenLecturas = "aumentado", y publica lo detectado en un tópico de prueba (no en servicio-eventos) y en un JSONL
para evaluarlo contra las anomalías etiquetadas (streaming/evaluar_detector.py). No escribe en InfluxDB.

  docker exec lambda26-aqua-streaming spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
      validar_detector.py [--sin-compensacion]

Sin watermark: las lecturas aumentadas tienen fechas simuladas de hace un mes y el detector no lo necesita (no usa
timeouts); el orden por sensor lo da Kafka (una partición por clave sensorId).
"""
import argparse
import os
import shutil
import uuid

from pyspark.sql import SparkSession

from procesar_stream import enriquecer, leer_eventos, log  # noqa: F401
import servicio_spark

TOPICO_ORIGEN = os.getenv("KAFKA_TOPIC_AUMENTADOS", "aqua-eventos-aumentados")
TOPICO_PRUEBA = os.getenv("TOPICO_VALIDACION", "servicio-eventos-validacion-f4")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sin-compensacion", action="store_true", help="desactiva la compensación térmica (comparación)")
    ap.add_argument("--salida", default="/datos/validacion")
    a = ap.parse_args()

    if a.sin_compensacion:
        # Copia de las reglas con la compensación apagada; el detector la usa en lugar de la montada
        import yaml
        reglas = yaml.safe_load(open(servicio_spark.REGLAS_PATH, encoding="utf-8"))
        reglas["compensacion_termica"]["activa"] = False
        ruta = "/tmp/reglas_sin_compensacion.yml"
        yaml.safe_dump(reglas, open(ruta, "w", encoding="utf-8"), allow_unicode=True)
        os.environ["REGLAS_SERVICIO_PATH"] = ruta
        servicio_spark.REGLAS_PATH = ruta

    nombre = "sin_compensacion" if a.sin_compensacion else "con_compensacion"
    os.makedirs(a.salida, exist_ok=True)
    jsonl = f"{a.salida}/eventos_{nombre}.jsonl"
    if os.path.exists(jsonl):
        os.remove(jsonl)
    checkpoint = f"/tmp/validacion-{nombre}-{uuid.uuid4().hex[:8]}"

    spark = (SparkSession.builder.appName(f"monitoraqua-validacion-{nombre}")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
             .config("spark.executorEnv.REGLAS_SERVICIO_PATH", servicio_spark.REGLAS_PATH)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    lecturas = leer_eventos(spark, TOPICO_ORIGEN, "earliest")
    q = (servicio_spark.eventos_servicio(lecturas, "aumentado").writeStream.queryName(f"validacion_{nombre}")
         .outputMode("append").foreachBatch(servicio_spark.a_servicio(TOPICO_PRUEBA, a_influx=False, jsonl=jsonl, log=log))
         .option("checkpointLocation", checkpoint).trigger(availableNow=True).start())
    q.awaitTermination()
    shutil.rmtree(checkpoint, ignore_errors=True)
    log(status="validacion_terminada", modo=nombre, jsonl=jsonl, topico=TOPICO_PRUEBA,
        lecturas=sum(p["numInputRows"] for p in q.recentProgress))
    spark.stop()


if __name__ == "__main__":
    main()
