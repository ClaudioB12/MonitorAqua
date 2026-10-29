# Job de Spark Structured Streaming (F3). Misma base que lambda26/pyspark: Python 3.10, Java 21, PySpark 4.2.0.
# Contexto de build: raíz del repo.
FROM python:3.10-slim-trixie

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64 \
    PATH="/usr/lib/jvm/java-21-openjdk-amd64/bin:${PATH}" \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir pyspark==4.2.0 PyYAML==6.0.2 pandas==2.3.3 pyarrow==19.0.1

# Descarga el conector Kafka al caché de Ivy durante el build: el arranque no depende de Maven Central.
RUN python -c "from pyspark.sql import SparkSession as S; \
S.builder.config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0').getOrCreate().stop()"

WORKDIR /app/streaming
COPY streaming/*.py ./

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", \
     "--driver-memory", "1g", "--conf", "spark.ui.port=4040", "procesar_stream.py"]
