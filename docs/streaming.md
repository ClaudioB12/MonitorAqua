# Speed layer: Spark Structured Streaming (F3)

Cubre el criterio 3 de la rúbrica: ventanas, watermarking y checkpointing verificados en vivo sobre datos reales.

| | |
|---|---|
| Código | `streaming/procesar_stream.py`, `streaming/influx.py` |
| Contenedor | `lambda26-aqua-streaming` (servicio `aqua-streaming` de `infra/compose.yml`, imagen `monitoraqua-streaming`) |
| Versión | PySpark **4.2.0**, Scala 2.13, Java 21, Python 3.10 |
| Conector | `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`, pasado con `spark-submit --packages` y descargado en la imagen al construirla |
| Entrada | `aqua-eventos-real` (`startingOffsets = earliest` la primera vez; luego, lo que diga el checkpoint) |
| Spark UI | http://localhost:4043 |
| Checkpoints | volumen `monitoraqua_spark_checkpoints` (`/checkpoints/<consulta>`) |
| Bronce | `data/local/bronce/` (no versionado), Parquet `origen=…/fecha=…` |

```bash
docker compose -f infra/compose.yml --env-file .env up -d --build aqua-streaming
docker logs -f lambda26-aqua-streaming | grep '"progress"'
```

## Qué hace con cada lectura

1. **Catálogo de procedencia.** Conserva `origen`, `fuenteTipo`, `sensorId` y los metadatos de Kafka (partición,
   offset, timestamp), y agrega:
   - `es_dato_real`: `fuenteTipo` es `iot-propio` o `api-publica`.
   - `latencia_ingesta_s`: `(ingestaTs − timestamp) / 1000`.
   - `tiempo_evento`: el `timestamp` como fecha y hora; es el tiempo de evento.
2. **Calidad.** `lectura_valida` y `motivo_invalidez` (el primer motivo que aplica):

   | Motivo | Regla |
   |---|---|
   | `sin_eventId_o_timestamp` | faltan los campos de identidad o de tiempo |
   | `distanciaCm_-1` | código de falla del HC-SR04 |
   | `<medida>_fuera_de_rango` | distanciaCm 2-450 · nivelPct 0-100 · nivelM -10-100 · caudalM3s 0-10000 · temperaturaC -40-60 · humedadPct 0-100 · tempAguaC -5-45 · ph 0-14 · oxigenoMgL 0-25 |
   | `sin_medidas` | ninguna medida con valor |

   Las inválidas **se cuentan** (`n_invalidas` en cada ventana) pero **no entran** a promedios, mínimos ni máximos.
   En bronce se guardan con su motivo.

## Cuatro consultas, cada una con su checkpoint

| Consulta | Fuentes | Watermark | Ventana | Deduplicación | Sumidero | Trigger |
|---|---|---|---|---|---|---|
| `rapidas_agg` | esp32-fisico, thingspeak | **7 min** | 1 min, se desliza cada 30 s | `dropDuplicatesWithinWatermark(eventId)` | InfluxDB `lecturas_agg` (`ventana=1m`) | 30 s |
| `lentas_agg` | ea-uk, usgs, open-meteo | **90 min** | 15 min, fija | `dropDuplicatesWithinWatermark(eventId)` | InfluxDB `lecturas_agg` (`ventana=15m`) | 1 min |
| `esp32_detalle` | esp32-fisico, solo válidas | 7 min | — | `dropDuplicatesWithinWatermark(eventId)` | InfluxDB `lecturas_esp32`, una lectura por punto, para el panel en vivo | 10 s |
| `bronce` | todas, válidas e inválidas | — | — | no: es la copia cruda | Parquet `origen=…/fecha=…` | 1 min |

- **Etiquetas de `lecturas_agg`:** `sensorId`, `origen`, `fuenteTipo` y `ventana`.
- **Campos de `lecturas_agg`:** `n_total`, `n_validas`, `n_invalidas`, `es_dato_real`, la latencia media y máxima, la
  media de cada medida y el mínimo/máximo de distancia y nivel.
- **Escritura en InfluxDB:** un punto con el mismo measurement, etiquetas y tiempo reemplaza al anterior. Por eso el
  modo `update`, que reescribe la ventana en cada lote, y el reproceso de un lote tras un reinicio no duplican datos.
- **Métricas de cada lote:** un `StreamingQueryListener` las escribe en InfluxDB, measurement `spark_progreso`
  (filas, filas/s, duración, `numRowsDroppedByWatermark`, `numDroppedDuplicateRows`, filas en estado), para la
  observabilidad de F5.

### Por qué dos watermarks

Un watermark se calcula con el mayor tiempo de evento de **toda** la consulta. Si las fuentes rápidas y las lentas
compartieran consulta, el ESP32 (tiempo real) empujaría el watermark y descartaría casi todo lo de EA y USGS, que llega
con decenas de minutos de atraso. Atraso medido en Kafka el 2026-10-28, con el PC despierto:

| Origen | p50 | p90 | máx. |
|---|---|---|---|
| esp32-fisico | 0.1 s | 0.2 s | 59 s |
| thingspeak | 96 s | 152 s | 250 s |
| open-meteo | 536 s | 538 s | 538 s |
| ea-uk | 25 min | 26 min | 26 min |
| usgs | 29 min | 54 min | 54 min |

- **Rápidas: 7 min, no 5.** El colector consulta ThingSpeak cada 5 min, así que su atraso puede llegar a 5 min y
  algo más; con 5 min se descartarían las lecturas que caen justo en el borde.
- **Lentas: 90 min.** El mayor tiempo de evento lo pone Open-Meteo, con ~9 min de atraso, y USGS queda hasta ~45 min
  por detrás de él: hay margen.
- **Cuando el PC se suspende:** el puente se pone al día al despertar. Esa ráfaga entra completa en el primer lote,
  porque el watermark que aplica es el del lote anterior, de antes de la suspensión.

## Verificación en vivo (2026-10-29)

Los registros crudos de estas pruebas (progreso de las consultas, consultas a InfluxDB con resultado, bronce y los 54
duplicados) están en [`evidencias/f3/`](evidencias/f3/README.md).

**1. Ventanas.** A los pocos segundos de arrancar, `lecturas_agg` ya tenía puntos de los cinco orígenes: esp32 1,101
(1m) · thingspeak 342 (1m) · ea-uk 231 (15m) · usgs 51 (15m) · open-meteo 12 (15m). Ventana ESP32 de las 03:25 UTC:
`n_total 6, n_validas 4, n_invalidas 2, distanciaCm_media 17.08 (17.0-17.1), latencia_ingesta_s_media 0.19`.

**2. Checkpoint.** El job se detuvo a las 03:25:57 y se reinició a las 03:27:08 UTC. Las cuatro consultas volvieron
con el mismo `id`, o sea, el mismo checkpoint, y cada una retomó en el offset exacto donde había terminado:

| Consulta | Último lote antes | Primer lote después | Filas |
|---|---|---|---|
| `esp32_detalle` | batch 5, fin 6670 / 226 / 77 | batch 6, **inicio 6670 / 226 / 77** | 24 |
| `rapidas_agg` | batch 1, fin 6666 / 226 / 77 | batch 2, **inicio 6666 / 226 / 77** | 28 |
| `bronce` | batch 0, fin 6654 / 225 / 74 | batch 1, **inicio 6654 / 225 / 74** | 44 |
| `lentas_agg` | batch 0, fin 6654 / 225 / 74 | batch 1, **inicio 6654 / 225 / 74** | 44 |

Ni pérdida ni repetición: en bronce, cada par (partición, offset) de Kafka aparece **exactamente una vez**, sin
huecos. Son 6,997 filas de los offsets 0-6687, 0-229 y 0-78.

**3. Watermark.** Se publicó a mano en `aqua-eventos-real` un evento con `timestamp` de hace 2 horas
(`prueba-f3-tardio-f0c5fc05`, partición 1, offset 230). En el lote que lo contenía, `rapidas_agg` (batch 5) y
`esp32_detalle` (batch 11) reportaron **`numRowsDroppedByWatermark = 1`**, con un watermark de
2026-10-29T03:20:59Z. El evento no aparece en InfluxDB. Sí aparece en bronce, que no tiene watermark y guarda todo lo
que llega.

**4. Deduplicación.** Se publicó dos veces el mismo `eventId` (`prueba-f3-duplicado-7c6dedcc`, offsets 231 y 232).
`rapidas_agg` (batch 6) y `esp32_detalle` (batch 15) reportaron **`numDroppedDuplicateRows = 1`**. En InfluxDB cuenta
una sola vez: `n_total = 1` en las dos ventanas deslizantes que lo contienen y un único punto de detalle. Bronce
conserva las dos copias crudas.

Además, en el primer lote la deduplicación descartó **54 lecturas reales repetidas**. Las había publicado dos veces
el puente de la S08 al reenviar datos (offsets 1620-1727 de la partición 0).

!!! note "Sensores de prueba excluidos"
    Las pruebas dejaron 3 mensajes en `aqua-eventos-real` (`sensorId = esp32-prueba-f3`, `eventId` con prefijo
    `prueba-f3-`), y también quedaron en el Parquet bronce. Kafka no permite borrar mensajes sueltos.

    La lista única de sensores de prueba es **`ingesta/sensores_prueba.yml`**, y todos la leen de ahí:

    - **Spark:** las consultas `rapidas_agg`, `lentas_agg` y `esp32_detalle` descartan esos `sensorId` antes de
      agregar. El job lee el archivo montado al arrancar.
    - **Bronce:** conserva todo lo que llega, sin filtrar.
    - **F4 (eventos de servicio) y la capa batch:** deben filtrar con la misma lista. Al leer bronce, excluir
      `sensorId` en la lista.

## Operación

- **Reinicio seguro:** `docker restart lambda26-aqua-streaming` retoma desde el checkpoint.
- **Reprocesar desde cero:** borrar el volumen `monitoraqua_spark_checkpoints` y `data/local/bronce/`. Al arrancar
  sin checkpoint vuelve a leer desde `earliest`.
- **Cambiar watermarks o ventanas:** van por variables de entorno (`WATERMARK_RAPIDO`, `WATERMARK_LENTO`). Un cambio
  en la forma de una consulta con estado puede exigir un checkpoint nuevo.
- **Particiones del estado:** `spark.sql.shuffle.partitions = 4`, que queda fijado en el checkpoint.
