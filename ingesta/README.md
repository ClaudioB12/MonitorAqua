# ingesta/

Ingesta unificada (F2): todas las fuentes escriben en Firebase `/board1/lecturas/historial` y un solo puente
publica en Kafka `aqua-eventos-real`.

| Archivo | Qué hace |
|---|---|
| `colector_apis.py` | Consulta ThingSpeak, EA UK, USGS y Open-Meteo, convierte al contrato, valida y escribe en Firebase |
| `estaciones.yml` | Estaciones, canales e intervalos de consulta de cada fuente |
| `puente_firebase_kafka.py` | Escucha `historial` (SSE), completa y valida cada evento, y lo publica en Kafka con clave `sensorId` |
| `comun.py` | Sesión Firebase, log JSON, carga del contrato |
| `auditar_ingesta.py` | Compara Kafka con Firebase: mensajes por origen, `eventId` repetidos y lecturas del ESP32 faltantes |
| `Dockerfile` | Imagen común `monitoraqua-ingesta` (contexto: raíz del repo) |

```bash
docker compose -f infra/compose.yml --env-file .env up -d --build aqua-puente aqua-colector
docker logs -f lambda26-aqua-puente
# Prueba del colector sin escribir nada:
docker run --rm --env-file .env monitoraqua-ingesta:latest python colector_apis.py --seco --una-vez
```

El detalle completo (autenticación, estado del puente, backfill, estaciones y cuotas de Firebase) está en
`docs/ingesta.md`.
