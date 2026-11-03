# Prueba de carga, corridas 1 y 2 (2026-10-30): saturación temprana e incidentes

**Montaje:**

- **Job de prueba:** `aqua-streaming-carga`, la misma imagen que producción, en paralelo con el job de producción y con
  `lambda26-pyspark` en la misma VM de Docker (24 núcleos, **7.47 GiB**).
- **Entrada:** `infra/prueba_carga.py` publica tanques sintéticos `carga-NNNNN` (una lectura cada 5 s) en un tópico de
  prueba.
- **Etapas:** 100, 500, 1,000, 2,000 y 5,000 ev/s.

## Corrida 1: `local[*]`, tópico `aqua-carga-prueba`, ~04:02-04:08 UTC

- **Memoria:** `OSError: [Errno 12] Cannot allocate memory` en el lote 5 y, a las 04:07:47, el job de prueba murió
  por falta de memoria (`OOMKilled=true`, código 137). Fue **en la etapa de 100 ev/s**.
- **Causa:**
  - Spark en `local[*]` abre hasta 24 workers de Python.
  - Cada uno carga el `tanques.yml` de prueba con **25,000 tanques**: 2.8 MB de YAML, **5.0 s de parseo con PyYAML**
    (el de producción, 0.007 s).
  - Todo eso comparte la VM con el job de producción (~1.5 GiB) y con `lambda26-pyspark`.

## Corrida 2: `local[6]`, tópico `aqua-carga-prueba-2`, 04:27-04:35 UTC

**Etapa de 100 ev/s (500 tanques), 04:27:27-04:33:43:** 36,000 eventos, 95.7 ev/s logrados.

| Consulta (trigger) | Lotes | Filas/lote (media) | Duración media | Duración máxima |
|---|---|---|---|---|
| `esp32_detalle` (10 s) | 19 | 1,540 | **13.0 s** | **87.2 s** |
| `eventos_servicio` (10 s) | 19 | 1,592 | **12.6 s** | **87.2 s** |
| `rapidas_agg` (30 s) | 11 | 2,750 | 13.9 s | 65.2 s |
| `latencias` (30 s) | 11 | 2,750 | 12.2 s | 61.8 s |
| `predicciones` (30 s) | 12 | 2,521 | 13.7 s | 59.6 s |
| `bronce` (60 s) | 6 | 4,542 | 13.6 s | 56.9 s |
| `lentas_agg` (60 s) | 6 | 4,625 | 15.7 s | 61.2 s |

**Inferencia (`predicciones`):** de 83 a 2,124 ms por lote, con 750-8,162 filas.

**Lectura:**

- **Las dos consultas de 10 s ya no dan abasto a 100 ev/s:** su duración media supera el trigger, y los picos de
  55-87 s coinciden en todas las consultas, que comparten los mismos núcleos.
- **Estos picos no miden el volumen:** coinciden con las recargas del `tanques.yml` de 25,000 tanques en los workers
  de Python.
- **`lagOffsets` (latestOffset − endOffset al planificar el lote) quedó en 0:** no ve lo que llega mientras el lote
  corre. La señal de saturación es la duración del lote frente al trigger.

**Incidente al final de la etapa (04:31-04:35 UTC):**

- **DNS:** el DNS interno de Docker dejó de resolver nombres: `Temporary failure in name resolution` hacia `influxdb`, y
  Kafka sin conectar a su propio controlador `kafka:9093`.
- **Job de prueba:** falló en un `foreachBatch` (timeout de Kafka y error de DNS) y terminó.
- **Job de producción:** cayó una vez (consulta `rapidas_agg`) y la política `unless-stopped` lo reinició a las
  04:35:16. **Retomó desde sus checkpoints:** mismo id `90e9da13…`, estado de la inferencia cargado en el lote 180.
- **Puente:** tuvo `ConnectionError` y se reconectó retomando desde su `desdeTs`. Firebase guarda todo, así que no se
  pierden lecturas.
- **Verificación:** a las 04:36:31 UTC, `verificar_stack.py` dio todo en OK.
- **Etapas sin correr:** se detuvo la prueba antes de la etapa de 500 ev/s.

## Incidente: eventos de prueba en el tópico real `servicio-eventos`

**Qué pasó:** el job de prueba tomaba `KAFKA_TOPIC_SERVICIO` del `.env`, así que publicó sus eventos de servicio en
el tópico real.

**Cuántos y cuándo:** **120 eventos de tanques `carga-*`**, entre las 04:03:40 y las 04:32:37 UTC.

- **Por tipo:** `llegada_agua` 50, `umbral_termico` 34, `autonomia_baja` 24, `falla_sensor` 12.
- **Offsets:** partición 0 entre 81 y 137; partición 1 entre 6 y 48; partición 2 entre 0 y 19. Quedan mezclados con
  los 87 eventos reales.

**Alcance:**

- **Consumidor de eventos:** solo los registró en su log.
- **Buckets reales de InfluxDB:** ninguno tiene datos `carga-*` (comprobado con Flux sobre `monitoraqua` y
  `monitoraqua_aumentados`).
- **Respaldo:** `servicio_eventos_respaldo.json`, tomado a las 03:50 UTC, tiene solo los 87 eventos reales.

**Corrección:** el servicio de prueba fija `KAFKA_TOPIC_SERVICIO: ${CARGA_TOPICO}-servicio` (`infra/compose.yml`).

**Limpieza:** pendiente de decisión. Kafka no permite borrar mensajes sueltos: `kafka-delete-records` corta un prefijo
de cada partición y se llevaría también eventos reales. Los 120 vencen con la retención de 7 días (~2026-11-06) y se
distinguen por `sensorId` (`carga-*`).
