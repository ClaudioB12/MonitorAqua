# streaming/

Speed layer: Spark Structured Streaming sobre `aqua-eventos-real` (F3). Detalle y verificación en `docs/streaming.md`.

| Archivo | Qué hace |
|---|---|
| `procesar_stream.py` | Job principal: procedencia, calidad, dos watermarks, deduplicación, ventanas, sumideros InfluxDB y bronce |
| `influx.py` | Escritura a InfluxDB 2 por HTTP (line protocol), solo con la biblioteca estándar |
| `publicar_prueba.py` | Publica eventos de prueba (tardío, duplicado) para verificar watermark y deduplicación |
| `Dockerfile` | Imagen `monitoraqua-streaming` (PySpark 4.2.0 + conector Kafka precargado) |

```bash
docker compose -f infra/compose.yml --env-file .env up -d --build aqua-streaming
```

Los eventos de servicio (F4) y la inferencia en vivo (F7) se agregan aquí.
