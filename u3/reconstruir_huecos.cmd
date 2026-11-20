@echo off
REM U3 - D4 - Reconstruccion rotulada de los huecos del ESP32: bronce -> PySpark -> Kafka -> PySpark -> aumentados.
REM No pasa por Firebase. Detalle en docs\u3-d4.md (seccion 12). Uso: u3\reconstruir_huecos.cmd   (o u3\reconstruir_huecos.cmd seco)
set PAQ=org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
set SECO=
if /I "%~1"=="seco" set SECO=--seco
echo === 1/3 Topico aqua-eventos-reconstruidos ===
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic aqua-eventos-reconstruidos --partitions 3 --replication-factor 1
if errorlevel 1 goto error
echo === 2/3 Generar y publicar en Kafka (PySpark) ===
docker run --rm --network lambda26-kafka-net --env-file .env -v "%cd%:/repo" -w /repo monitoraqua-streaming:latest spark-submit --packages %PAQ% --driver-memory 2g --conf spark.log.level=ERROR u3/reconstruir_huecos_esp32.py %SECO%
if errorlevel 1 goto error
if defined SECO goto fin
echo === 3/3 Kafka -> Parquet + InfluxDB aumentados (PySpark) ===
docker run --rm --network lambda26-kafka-net --env-file .env -e INFLUX_URL_INTERNA=http://influxdb:8086 -v "%cd%:/repo" -w /repo monitoraqua-streaming:latest spark-submit --packages %PAQ% --driver-memory 2g --conf spark.log.level=ERROR u3/cargar_reconstruccion_spark.py
if errorlevel 1 goto error
:fin
echo Listo.
exit /b 0
:error
echo FALLO: revisa el mensaje de arriba.
exit /b 1
