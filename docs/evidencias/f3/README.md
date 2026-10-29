# Evidencias de F3 (Spark Structured Streaming), 2026-10-29

Registros crudos de las cuatro pruebas en vivo descritas en `docs/streaming.md`. Los `.jsonl` son líneas del log de
`lambda26-aqua-streaming`, extraídas antes de recrear el contenedor:

- `"status": "progress"`: lo escribe el `StreamingQueryListener` en cada micro-lote.
- `"status": "query_started"`: arranque de cada consulta.
- `"status": "influx_write"`: escritura en InfluxDB.

Horas en UTC; `ts` en milisegundos epoch. Offsets de `aqua-eventos-real` como partición 0 / 1 / 2.

| Archivo | Prueba | Qué muestra |
|---|---|---|
| `streaming_log_completo.jsonl` | todas | las 203 líneas de progreso, arranque y escritura desde el primer arranque (03:24:58) |
| `influxdb_consultas.md` | ventanas, watermark, dedup | las consultas Flux y sus resultados |
| `checkpoint_ultimo_lote_antes.jsonl` | checkpoint | último lote de cada consulta antes de detener el job (03:25:57) |
| `checkpoint_primer_lote_despues.jsonl` | checkpoint | primer lote de cada consulta después de reiniciar (03:27:08): su `start` es el `end` anterior |
| `checkpoint_reinicio_query_started.jsonl` | checkpoint | las 4 consultas arrancan dos veces con el **mismo `id`**, o sea, con el mismo checkpoint |
| `watermark_lote_evento_tardio.jsonl` | watermark | lote con el evento de hace 2 h (partición 1, offset 230): `numRowsDroppedByWatermark = 1` |
| `dedup_lote_eventos_duplicados.jsonl` | deduplicación | lote con el `eventId` repetido (offsets 231 y 232): `numDroppedDuplicateRows = 1` |
| `primer_lote_54_duplicados_s08.jsonl` | hallazgo | primer lote: `numDroppedDuplicateRows = 54` en las consultas rápidas |
| `bronce_y_duplicados.md` | checkpoint, hallazgo | bronce sin repetidos ni huecos, los eventos de prueba y la lista de los 54 duplicados |

## Resumen

**Checkpoint.** El job se detuvo a las 03:25:57 y se reinició a las 03:27:08. Cada consulta retomó en el offset
exacto donde había terminado:

| Consulta | Antes (fin) | Después (inicio) |
|---|---|---|
| `esp32_detalle` | 6670 / 226 / 77 | 6670 / 226 / 77 |
| `rapidas_agg` | 6666 / 226 / 77 | 6666 / 226 / 77 |
| `bronce` | 6654 / 225 / 74 | 6654 / 225 / 74 |
| `lentas_agg` | 6654 / 225 / 74 | 6654 / 225 / 74 |

En bronce, cada (partición, offset) aparece una sola vez y sin huecos.

**Watermark.** `rapidas_agg` (batch 5) y `esp32_detalle` (batch 11): `numRowsDroppedByWatermark = 1`, con watermark en
2026-10-29T03:20:59Z y el evento con tiempo 01:28 UTC. La consulta a InfluxDB por `esp32-prueba-f3` entre 01:00 y
02:00 no devuelve filas: el bloque de resultado está vacío.

**Deduplicación.** `rapidas_agg` (batch 6) y `esp32_detalle` (batch 15): `numDroppedDuplicateRows = 1`. En InfluxDB
hay un solo punto de detalle y `n_total = 1` en cada una de las dos ventanas deslizantes que lo contienen.

**Ventanas.** `lecturas_agg` tiene puntos de los cinco orígenes.

La ventana del ESP32 de las 03:25 aparece con dos tamaños, y es correcto: la consulta corre en modo `update`, que
reescribe una ventana abierta en cada lote.

- **Al mirarla durante la prueba:** 6 lecturas, de las cuales 2 inválidas.
- **En `influxdb_consultas.md`, ya cerrada:** 12 lecturas, 4 inválidas y 8 válidas.

**Hallazgo.** 54 lecturas reales del ESP32 estaban dos veces en el tópico, en la partición 0, offsets 1620-1727. Las
publicó el puente de la S08 al reenviar datos, y la deduplicación de Spark las descarta.
