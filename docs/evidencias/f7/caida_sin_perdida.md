# Caída del primer despliegue de F7: no se perdieron lecturas (2026-10-30)

**Qué pasó:**
- **Causa:** a las 02:53:40 UTC, la consulta nueva `predicciones` lanzó una excepción (InfluxDB respondió 422: un campo
  con dos tipos).
- **Efecto:** el `foreachBatch` sin protección detuvo **todo el job**, no solo esa consulta, y el contenedor se
  reinició en bucle hasta el arreglo, desplegado a las 02:56:37 UTC.
- **Hoy:** la consulta atrapa sus errores (`inferencia_error`) y no detiene a las demás.

**Comprobaciones:**

| Comprobación | Resultado |
|---|---|
| Las consultas retomaron desde su checkpoint | las 7 consultas (`rapidas_agg`, `lentas_agg`, `esp32_detalle`, `bronce`, `latencias`, `eventos_servicio`, `predicciones`) con **un único id** en todos sus arranques (mismo checkpoint, `runId` nuevo) |
| Bronce sin huecos | ESP32 entre 02:40 y 03:20 UTC: **479 lecturas de ~480 esperadas** (una cada 5 s), hueco máximo de 10 s, ninguno mayor de 15 s. Las **72** lecturas que entraron a Kafka durante la caída están en bronce |
| ThingSpeak en el tramo | 41 lecturas de 7 canales en bronce |
| Descartes por watermark tras el reinicio | `rapidas_agg`, `esp32_detalle`, `eventos_servicio`, `latencias` y `predicciones`: **0** |

**Lo único descartado, y por qué no fue la caída:**

- **Qué se descartó:** `lentas_agg` (EA, USGS y Open-Meteo, watermark de 90 min) descartó 98 filas en su primer lote
  tras el reinicio.
- **Por qué no fue la caída:** se debió al **reinicio del colector** (02:53:36 UTC), que se recreó para agregar `field2`
  de 3418755. Al arrancar, el colector hace su recarga y publicó 318 lecturas lentas, 218 nuevas y con más de 90 min de
  atraso (EA hasta 207 min).
- **Dónde están esas lecturas:**
  - **en bronce,** que guarda todo sin watermark;
  - **no en las ventanas de 15 min de `lecturas_agg`,** que por diseño ignoran lo que llega más tarde que el
    watermark.
- **Contexto:** `lentas_agg` descartó 741 filas tardías el 29-oct sin ningún reinicio. Es su comportamiento normal con
  fuentes atrasadas.

Un reinicio del colector vuelve a producir este efecto, como se vio también en el redespliegue con Open-Meteo de la
ubicación de 3418755 a las 03:38 UTC.
