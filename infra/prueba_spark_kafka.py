"""Prueba mínima F1: Spark lee 10 mensajes de aqua-eventos-real y los imprime.

Corre dentro del contenedor lambda26-pyspark (Spark 4.2.0, Scala 2.13), que está en la red
lambda26-kafka-net. El conector se pasa con --packages (no con spark.jars.packages en el
builder: con spark-submit la JVM ya arrancó y la opción se ignora):

  docker cp infra/prueba_spark_kafka.py lambda26-pyspark:/tmp/
  docker exec lambda26-pyspark spark-submit --conf spark.ui.enabled=false \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/prueba_spark_kafka.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "aqua-eventos-real")
N = 10

spark = SparkSession.builder.appName("monitoraqua-prueba-kafka").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Lectura batch (no streaming): basta para comprobar conector, red y bootstrap.
df = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

print(f"Spark {spark.version} · {BOOTSTRAP} · {TOPIC} · {df.count()} mensajes en el tópico")
(
    df.orderBy(F.col("offset").desc())
    .limit(N)
    .select("partition", "offset", "timestamp", F.col("value").cast("string").alias("value"))
    .show(N, truncate=False)
)
spark.stop()
