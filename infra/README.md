# infra/

Capa de servicio de MonitorAqua sobre el stack lambda26 (F1). No crea otro Kafka ni otro Prometheus: se une a
la red `lambda26-kafka-net`.

- `compose.yml`: InfluxDB 2 (`lambda26-influxdb`, puerto 48086) y Grafana (`lambda26-grafana`, puerto 43000).
- `grafana/provisioning/`: datasources Prometheus e InfluxDB, y proveedor de tableros (`../dashboards`).
- `verificar_stack.py`: chequeo de todo el stack en un comando.
- `prueba_spark_kafka.py`: prueba mínima de Spark leyendo `aqua-eventos-real`.

```bash
docker compose -f infra/compose.yml --env-file .env up -d
.venv/Scripts/python infra/verificar_stack.py
```

El estado completo y las trampas conocidas están en `docs/stack-actual.md`.
