@echo off
REM U3 · D4: ejecución integrada (compuertas, batch, streaming, modelo, Prometheus y manifiesto).
REM Desde la raíz del repo (CMD):   u3\correr_d4.cmd
REM Demostrar una compuerta fallando:   set FORZAR_FALLA=modelo   y luego   u3\correr_d4.cmd   (después: set FORZAR_FALLA=)
REM Usa la imagen del job de streaming (PySpark 4.2.0 + conector de Kafka en caché) en la red del stack.
docker run --rm --name u3-d4-integrada --network lambda26-kafka-net --env-file .env ^
  -e "FORZAR_FALLA=%FORZAR_FALLA%" -e "VENTANA_STREAM_S=%VENTANA_STREAM_S%" -e "DIAS_BATCH=%DIAS_BATCH%" ^
  -v "%cd%:/repo" -w /repo monitoraqua-streaming:latest ^
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --driver-memory 1g ^
  u3/d4_ejecucion_integrada.py
