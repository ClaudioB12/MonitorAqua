# MonitorAqua: Proyecto Sello del Equipo Horeb (Big Data, UPeU)

Observatorio domiciliario del servicio de agua de Juliaca. Responde a la pregunta: **¿cómo anticipar la
disponibilidad y la calidad del agua en hogares que dependen de tanques por el suministro intermitente?**
El brief completo está en `docs/brief.md` y la rúbrica U2 en `docs/referencias/u2-producto.md`.

Producto actual: **Unidad 2**, un pipeline streaming con ingesta, Spark Structured Streaming,
observabilidad/costos y BI/ML en Grafana. El trabajo de la Unidad 1 vive en `pyspark/` y `data/`: **no se borra
ni se mueve sin preguntar**.

## Arquitectura (Lambda)

```text
ESP32 físico ─┐
Colector APIs ┴─> Firebase RTDB (/board1/lecturas/historial) ─> puente ─> Kafka aqua-eventos-real
                                                                            │
           ┌────────────────────────────────────────────────────────────────┤
           v                                                                v
 Speed: Spark Structured Streaming                          Batch: Parquet + Spark (+ SUNASS/INEI/SENAMHI)
   (procedencia, watermark, dedup eventId, ventanas,          └─ modelos MLlib ─> cargados por el streaming
    checkpoint, inferencia de series de tiempo)
           ├─> InfluxDB ─> Grafana (BI por dimensión)
           └─> Kafka servicio-eventos (llegada_agua, alerta_fuga, autonomia_baja, umbral_termico)
 Observabilidad: Prometheus + kafka-exporter + métricas de Spark ─> Grafana
 Documentación: MkDocs Material (docs/) · Despliegue: Docker Compose (infra/)
```

- **Firebase:** la entrada única de todas las fuentes es **`/board1/lecturas/historial`**, la ruta real que ya usa el firmware del ESP32 (que no se modifica). El colector de APIs de F2 escribe en esa misma ruta (`FIREBASE_PATH` en `.env`).
- **Kafka:** se **reutiliza** el cluster `lambda26`; no se crea un segundo Kafka. El bootstrap es `localhost:49092` desde el host y `kafka:9092` desde la red `lambda26-kafka-net`. Detalle en `docs/stack-actual.md`.

## Contrato del evento

Schema: `contratos/evento_aqua.schema.json` (draft 2020-12). Validación: `python contratos/validar.py`.

- **Obligatorios:** `tipoEvento`, `eventId`, `sensorId`, `origen` y `timestamp` (epoch ms de la medición).
- **Opcionales en la entrada:** `fuenteTipo` e `ingestaTs` (epoch ms). El ESP32 no los envía; los completa el puente.
- **Medidas:** valen `null` si la fuente no las mide. Son `distanciaCm`, `nivelPct`, `nivelM`, `caudalM3s`, `temperaturaC`, `humedadPct`, `tempAguaC`, `ph`, `oxigenoMgL`, `lat` y `lon`.
- **Alcance del schema:** valida la forma, no la física. El -1 de un sensor fallido se filtra como regla de calidad en Spark.

| origen | fuenteTipo | tipoEvento |
|---|---|---|
| `esp32-fisico` | `iot-propio` | `aqua.tanque.lectura` |
| `thingspeak` | `api-publica` | `aqua.tanque.lectura` |
| `ea-uk` | `api-publica` | `aqua.red.lectura` |
| `usgs` | `api-publica` | `aqua.calidad.lectura` |
| `open-meteo` | `api-publica` | `aqua.clima.lectura` |

Si cambia el contrato, se actualizan en el mismo cambio el schema, `validar.py` y esta sección.

## Dimensiones U2 (cada integrante sustenta la suya)

| Dim. | Integrante | Indicador en vivo | Señal de entrada |
|---|---|---|---|
| D2 | Claudio Bustinza Inofuente | Horas de autonomía del tanque según la tendencia del nivel | `distanciaCm` / `nivelPct` |
| D4 | Wilbert Alex Mayta Arotaype | Hora estimada de la próxima llegada de agua | Subida brusca de nivel (caudalímetro YF-S201 por confirmar) |
| D6 | Joel Huillca Lima | Pérdida proyectada por fuga (nivel esperado vs. observado) | Nivel compensado |
| D8 | Josué Gabriel Ochoa Mamani | Cruce del umbral térmico del agua y tiempo de residencia | `tempAguaC` (sonda DS18B20 por confirmar) |

## Fases y mapa a la rúbrica U2

| Fase | Contenido | Criterio de la rúbrica |
|---|---|---|
| F0 | Preparación: estructura, contrato, `.env.example`, inventario del stack | — |
| F1 | Infraestructura: completar el stack con InfluxDB y Grafana (`infra/`) | Apoyo a 3, 4 y 6 |
| F2 | Ingesta unificada: colector de APIs y puente revisado (`ingesta/`) | 2 (IoT/sensores) |
| F3 | Spark Streaming: watermark, dedup, ventanas, checkpoint (`streaming/`) | 3 |
| F4 | Eventos de servicio derivados → `servicio-eventos` | 1 (eventos empresariales) |
| F5 | Grafana BI y observabilidad (`dashboards/`) | 4 y 6 |
| F6 | Batch y modelos MLlib (`batch/`) | 7 (entrenamiento) |
| F7 | Inferencia en vivo de series de tiempo | 7 (validada e integrada al BI) |
| F8 | Costos, MkDocs y ensayo de demo | 5 y 8 (sustentación) |

Se trabaja **una fase a la vez**: no se escribe código de una fase posterior sin que se pida.

## Convenciones

- **Lenguaje y procesamiento:** Python 3 y PySpark. Los jobs de streaming corren con `spark-submit` como proceso persistente; los notebooks quedan para exploración y batch.
- **Tópicos:** `aqua-eventos-real` (lecturas, 3 particiones, key `sensorId`) y `servicio-eventos` (eventos de negocio).
- **Credenciales:** solo en `.env`, que nunca se versiona (plantilla en `.env.example`). Las cuentas de servicio de Firebase van en `credenciales/` y también quedan fuera de git.
- **Archivos locales:** los checkpoints van en `checkpoints/` y los datos locales en `data/local/`; ninguno se versiona.
- **Restricciones:** no modificar el firmware del ESP32, no crear otro cluster Kafka y no tocar el U1 (`pyspark/`, `data/`) sin preguntar.
- **Commits:** solo con aprobación explícita del usuario, en ramas por fase (`u2-faseN`).
- **Docker en Windows:** en Git Bash, `docker exec -w /opt/...` necesita `MSYS_NO_PATHCONV=1`, o usar PowerShell.
