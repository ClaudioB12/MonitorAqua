# Guion de la demo en vivo (sustentación U2)

**Formato:** 8-10 minutos de demo técnica, después del pitch de cada integrante, que lleva su propio apoyo visual
(subaspecto 6). Se recorre el pipeline en el orden en que viaja un dato.

- Cada criterio de la rúbrica tiene su minuto y su plan B.
- Cada integrante sustenta su dimensión (subaspecto 1) en el bloque 6.

**Pestañas abiertas antes de empezar** (en este orden, de izquierda a derecha):

| # | Qué | Dirección |
|---|---|---|
| 1 | Firebase Console → Realtime Database → `/board1/lecturas/historial` | consola de Firebase |
| 2 | Kafbat (Kafka UI) | http://localhost:48085 |
| 3 | Spark UI del job de streaming | http://localhost:4043 |
| 4 | Grafana · "Salud del pipeline" | http://localhost:43000/d/monitoraqua-pipeline |
| 5 | Grafana · "Salud del servicio de agua – Juliaca" | http://localhost:43000/d/monitoraqua-bi |
| 6 | Grafana · "Prueba de carga (F8)" | http://localhost:43000/d/monitoraqua-carga |
| 7 | Sitio MkDocs (página *Producto U2*) | ver README → Documentación |
| 8 | Terminal en la raíz del repo, con `.venv` | — |

## Orden de la demo

| Min | Bloque | Qué mostrar | Criterio |
|---|---|---|---|
| 0:00-0:45 | **Arranque verificado** | `.venv/Scripts/python infra/verificar_stack.py`: todo en OK, con la ingesta viva (último mensaje de hace < 120 s) y las 7 consultas de Spark | 4 (soporte) |
| 0:45-2:00 | **Ingesta IoT** | Mover la mano frente al sensor del tanque. Firebase recibe la lectura; en Kafbat aparece en `aqua-eventos-real` (partición 0, clave `esp32-tanque-real-01`) con el contrato `evento_aqua`. En el BI, panel "Nivel del tanque" en los últimos 5 min | 2 |
| 2:00-3:15 | **Spark Structured Streaming** | Spark UI: 7 consultas activas. Ventanas de 1 y 15 min (`lecturas_agg`), watermark de 7 min, `dropDuplicatesWithinWatermark`. En vivo: `publicar_prueba.py tardio --horas 2` y `duplicado --veces 2`; en "Salud del pipeline" suben "Filas descartadas por watermark" y "Duplicados descartados". Checkpoint: el id de cada consulta no cambia tras un reinicio (`evidencias/f7/caida_sin_perdida.md`) | 3 |
| 3:15-4:15 | **Eventos empresariales** | Tópico `servicio-eventos` en Kafbat (contrato `evento_servicio`), log del consumidor (`docker logs --tail 5 lambda26-aqua-consumidor-eventos`) y panel "Eventos de servicio por tipo". Si hay tiempo: vaciado controlado del tanque → `llegada_agua` o `consumo` | 1 |
| 4:15-5:15 | **Observabilidad** | "Salud del pipeline": mensajes/s por tópico, lag, duración del lote frente al trigger, latencia p95 por origen, volumen por tópico, latencia de la inferencia; las 6 alertas en Alerting | 4 |
| 5:15-8:15 | **BI y series de tiempo, una dimensión por integrante (45 s cada uno)** | Tabla F7 de su dimensión (real y, con `origenLecturas = aumentado`, la flota), su métrica de validación (página `modelo-dX`), su panel en "Pronóstico vs real". D2 Claudio · D4 Wilbert · D6 Joel · D8 Josué | 6, 7, 8 |
| 8:15-9:30 | **Costos y escalado** | "Prueba de carga (F8)": dónde se satura el stack y por qué. Tabla de escenarios (actual, 100 tanques, Juliaca) y qué servicio gratuito deja de alcanzar ([costos y escalado](costos-escalado.md)) | 5 |
| 9:30-10:00 | **Reproducibilidad y documentación** | README (arranque desde cero) y el sitio MkDocs (*Producto U2* con la rúbrica y la evidencia) | 8 (subaspectos 4 y 5) |

**Qué decir de cada criterio, en una línea:**

| Criterio | Una línea |
|---|---|
| 1 | Spark convierte lecturas en eventos de negocio con contrato propio; Kafka los transporta y un consumidor los recibe en vivo. |
| 2 | El sensor del equipo y 41 sensores de APIs públicas entran por la misma puerta (Firebase → puente → Kafka) con un solo contrato. |
| 3 | Ventanas, watermark y checkpoint, verificados con eventos tardíos y duplicados reales y con un reinicio sin pérdida. |
| 4 | Métricas de Kafka (Prometheus) y de Spark (listener → InfluxDB) en un mismo tablero, correlacionadas por tiempo, con alertas. |
| 5 | El volumen medido, escenarios con precios oficiales y un punto de saturación medido con una prueba de carga, no supuesto. |
| 6 | Un tablero de KPIs del flujo en vivo con las cuatro dimensiones y sus predicciones. |
| 7 | Cuatro modelos validados contra líneas base en datos que no vieron, aplicados en vivo y con su error en vivo en el tablero. |

## Si algo falla

| Falla | Señal | Qué hacer en la demo |
|---|---|---|
| El ESP32 no publica (Wi-Fi o energía) | alerta "ESP32 sin datos por más de 5 min"; hueco en "Lecturas del ESP32 por minuto" | Decirlo, que es justo lo que muestra el panel de huecos. Seguir con ThingSpeak, las APIs y la flota: la capa de streaming es la misma |
| Sin internet en la sala | Firebase y las APIs no responden | Kafka, Spark y Grafana son locales: mostrar el último periodo con datos (rango 24 h) y la flota (`origenLecturas = aumentado`). Las capturas de `docs/evidencias/f5/` y `f7/` son el respaldo |
| Spark caído | Spark UI no abre; `verificar_stack.py` sin las consultas | `docker logs --tail 30 lambda26-aqua-streaming`; `docker restart lambda26-aqua-streaming`. Retoma desde los checkpoints en ~1 min; mientras, se explica el checkpoint con `caida_sin_perdida.md` |
| Lotes lentos | alerta "Lote de Spark más largo que su trigger" | Detener lo que no es de la demo: `docker stop lambda26-pyspark lambda26-aqua-streaming-carga` |
| Grafana sin datos de InfluxDB | paneles "No data" | `docker restart lambda26-influxdb lambda26-grafana`; si no, capturas de `evidencias/` |
| Kafka caído | `verificar_stack.py` en FALLA en Kafka | `docker start lambda26-kafka`. **Nunca `docker compose down`.** Firebase guarda las lecturas mientras tanto; el puente se pone al día al volver |
| Una predicción muestra un motivo en lugar de un valor | columna `motivo` | Es parte del diseño: el modelo dice por qué no aplica (geometría provisional, sin llegadas, filtro en arranque). Mostrar la flota, donde sí aplica |

## Lista de verificación del día

**La noche anterior:**

- [ ] `git pull` en `main`; `git status` limpio.
- [ ] Stack arriba desde hace al menos 2 h. Las ventanas de 15 min y D2/D4 necesitan historia; D8 sale del arranque con
  48 h.
- [ ] `infra/verificar_stack.py` y `infra/verificar_tableros.py` en OK.
- [ ] Capturas recientes de los 3 tableros como respaldo (renderizador de Grafana).
- [ ] Sitio MkDocs publicado y accesible.
- [ ] Batería y Wi-Fi del ESP32; sensor bien montado (zona ciega del JSN-SR04T: no medir a menos de 25 cm).

**Una hora antes:**

- [ ] PC enchufada, suspensión desactivada, Docker Desktop abierto.
- [ ] `docker ps`: Kafka, puente, colector, streaming, InfluxDB, Grafana y Prometheus en `Up`. El job de carga
  (`lambda26-aqua-streaming-carga`) y `lambda26-pyspark` detenidos.
- [ ] Panel "Lecturas del ESP32 por minuto" sin huecos en la última hora.
- [ ] Las 8 pestañas abiertas y con sesión iniciada (Grafana y Firebase).
- [ ] Terminal en la raíz del repo, con los comandos de la demo a mano (`publicar_prueba.py`, `docker logs`).
- [ ] Zoom del navegador al 125 % para el proyector.

**Al terminar:** anotar en `docs/arranque-diario.md` los offsets y los `batchId` si se apaga la PC.
