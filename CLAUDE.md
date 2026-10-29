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
- **Capa de servicio (F1):** `infra/compose.yml` levanta InfluxDB 2 (`localhost:48086`, bucket `monitoraqua`) y es el
  **dueño de Grafana** (`lambda26-grafana`, `localhost:43000`). El provisioning va en `infra/grafana/` y los tableros en
  `dashboards/`. Prometheus sigue en `lambda26/obs`; su servicio `grafana` no se vuelve a levantar.
- **Spark:** corre en el contenedor `lambda26-pyspark` (PySpark 4.2.0, Scala 2.13). El conector se pasa con
  `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`.
- **Verificación:** `.venv/Scripts/python infra/verificar_stack.py` comprueba todo el stack en un comando.
- **Kafka no tiene volumen:** usar `docker start`/`stop`; nunca `compose down`, porque borra los mensajes.
- **Ingesta (F2):** `infra/compose.yml` levanta `lambda26-aqua-puente` y `lambda26-aqua-colector` (imagen
  `monitoraqua-ingesta`, código en `ingesta/`). Detalle y estaciones en `docs/ingesta.md`.
  - **Colector:** escribe en Firebase con la clave `origen_estacion_timestamp` (PUT, idempotente).
  - **Recarga al arranque (principal):** el colector local, al arrancar, revisa 24 h de Firebase y recarga de cada
    API desde el primer hueco o la última lectura de ese origen hasta ahora (tope `recarga_max_dias` por API en
    `ingesta/estaciones.yml`), escribiendo SOLO claves nuevas. `RECARGA_ARRANQUE=0` la desactiva; prueba con
    `--solo-recarga --seco`. El código del colector está dentro de la imagen `monitoraqua-ingesta`, que se
    construye desde el servicio `aqua-puente`: `build aqua-puente` y luego `up -d --no-deps --force-recreate
    aqua-colector`.
  - **Colector en la nube (respaldo):** `.github/workflows/colector_apis.yml` (GitHub Actions, cada 30 min, `EDAD_MAX_H=7
    --una-vez --ventana-min 360`, un PATCH por fuente) sigue alimentando Firebase con la PC apagada; corre a la vez
    que el local sin duplicar. **GitHub no garantiza el cron** (la primera noche no disparó entre 02:07 y 08:07): la
    ventana de 6 h es la protección. El workflow vive en `main` y toma el código de la variable `COLECTOR_REF`
    (`u2-colector-nube`), que es **temporal**: se elimina al fusionar las ramas U2 en `main`. Credenciales solo
    como Secrets de GitHub. Si cambia el colector, se prueba con `--seco --ventana-min 360` antes del push.
  - **Arranque diario:** checklist en `docs/arranque-diario.md` (verificar_stack, puente sin duplicados, checkpoint
    de Spark, ejecuciones nocturnas, huecos con `ingesta/auditar_huecos.py`). Un hueco de APIs se recarga con el
    colector en modo ventana solo con claves nuevas (`docs/ingesta.md` → *Recarga puntual*).
  - **Puente:** lee `historial` con `orderBy="timestamp"` (necesita `.indexOn: "timestamp"` en las reglas), completa el
    contrato, valida, usa `sensorId` como clave de Kafka y guarda su estado (cursor, piso y claves vistas) en el volumen
    `monitoraqua_ingesta_estado`.
  - **Entrega:** al-menos-una-vez; Spark deduplica por `eventId`.
  - **Puente viejo:** `lambda26-aqua-bridge` queda detenido y con `restart=no`. **Nunca correrlo junto al nuevo**, porque
    cada lectura llegaría dos veces a Kafka.
  - **El -1 del ESP32** (fallo del sensor) viaja tal cual a Kafka; se filtra en Spark (F3).
- **Speed layer (F3):** `lambda26-aqua-streaming` (`streaming/procesar_stream.py`, Spark UI en `localhost:4043`)
  corre cuatro consultas sobre `aqua-eventos-real`, cada una con su checkpoint en el volumen
  `monitoraqua_spark_checkpoints`. Detalle y verificación en `docs/streaming.md`.

  | Consulta | Fuentes | Watermark | Ventana | Sumidero |
  |---|---|---|---|---|
  | `rapidas_agg` | ESP32, ThingSpeak | 7 min | 1 min / 30 s | InfluxDB `lecturas_agg` |
  | `lentas_agg` | EA, USGS, Open-Meteo | 90 min | 15 min | InfluxDB `lecturas_agg` |
  | `esp32_detalle` | ESP32 | — | — | InfluxDB `lecturas_esp32` |
  | `bronce` | todas | — | — | Parquet `data/local/bronce/origen=…/fecha=…` |

  - **Deduplicación** por `eventId` con `dropDuplicatesWithinWatermark`.
  - **Escritura a InfluxDB con reintentos** (`streaming/influx.py`): 5 reintentos, 1-16 s, solo ante fallas
    transitorias; un cambio en `streaming/*.py` exige reconstruir la imagen y recrear solo ese contenedor
    (`docker compose -f infra/compose.yml --env-file .env up -d --build --no-deps aqua-streaming`).
  - **Lecturas inválidas** (`motivo_invalidez`): se cuentan pero no entran a los agregados.
  - **Progreso por lote** en InfluxDB, measurement `spark_progreso`.
  - **Eventos de prueba:** `sensorId = esp32-prueba-f3` marca 3 eventos que quedaron en el tópico real; hay que
    excluirlos en los análisis.
- **Eventos de servicio (F4):** 5.ª consulta del job, `eventos_servicio`.
  - **Funcionamiento:** detector con estado por `sensorId` (`streaming/detector_servicio.py` dentro de
    `applyInPandasWithState`). Publica en **`servicio-eventos`** (clave `sensorId`) y en InfluxDB `eventos_servicio`.
  - **Contrato:** `contratos/servicio_evento.schema.json`; `eventoId = tipo-sensorId-inicioTs` es determinístico.
  - **Configuración:** umbrales en `streaming/reglas_servicio.yml` y geometría en `ingesta/tanques.yml`; el detector
    relee los dos al cambiar. El tanque del ESP32 tiene medidas **provisionales** (`por_medir: true`).
  - **Consumidor:** `lambda26-aqua-consumidor-eventos`, grupo `aqua-servicio-consumidor`, con su lag en Prometheus.
  - **Validación:** `streaming/validar_detector.py` sobre `aqua-eventos-aumentados`, con un tópico de prueba que se
    borra al terminar.
  - **Reglas CONGELADAS en f4.3 desde 2026-10-28 23:37:40 (Lima)**, registrado en `reglas_servicio.yml` →
    `congelamiento`. Lo real anterior es el conjunto de diseño y nunca se reporta como rendimiento. El rendimiento
    real sale solo de lecturas y bitácora posteriores (`streaming/comparar_bitacora.py`). Un cambio de reglas exige
    versión y congelamiento nuevos.
  - **Tres niveles de validación que no se mezclan:**
    - **Consistencia detector-generador** (`evaluar_detector.py`): mide definiciones compartidas, **no** rendimiento.
    - **Sensibilidad:** `sensibilidad.py`.
    - **Rendimiento real:** bitácora `docs/evidencias/f4/bitacora_real.csv`.
    Todas las proporciones llevan su IC 95 % de Wilson.
  - Detalle en `docs/eventos-servicio.md`.
- **Tableros y alertas (F5):** todo por provisioning.
  - **Tableros:** `dashboards/bi_salud_agua.json` (criterio 6) y `dashboards/pipeline_salud.json` (criterio 4), generados
    por `dashboards/generar_tableros.py`. **Nunca se editan a mano**: se cambia el script y se regenera.
  - **Alertas:** `infra/grafana/provisioning/alerting/reglas_monitoraqua.yml` (5 reglas).
  - **Measurements nuevas:** `nivel_tanque`, `estado_tanque`, `latencia_ingesta` (consulta Spark `latencias`) y
    `kafka_volumen` (servicio `aqua-volumen`).
  - **Buckets:** los datos aumentados van al bucket **`monitoraqua_aumentados`** (sin vencimiento); lo real, a
    `monitoraqua` (30 días). La variable `origenLecturas` del tablero BI elige el bucket. Historial:
    `streaming/cargar_historico_influx.py`.
  - **Verificación y capturas:** `infra/verificar_tableros.py`; capturas con `lambda26-grafana-renderer`.
  - **Grupos de consumidores:** `aqua-monitor-group` (S09) y `aqua-servicio-consumidor-relectura` se borraron en F5
    (abandonados).
  - Detalle en `docs/tableros.md`.
- **Datos aumentados (F2b):** la **flota extrapolada** (`ingesta/generador.py` + `generador.yml`, perfil en
  `batch/perfiles/perfil_real.json`) publica en **`aqua-eventos-aumentados`** (3 particiones, clave `sensorId`) y
  guarda Parquet en `batch/datos/aumentados/` (no versionado).
  - **Identificación:** `origen = flota-extrapolada`, `fuenteTipo = aumentado`, más `runId`, `esAnomalia`,
    `tipoAnomalia`, `episodioId` y `distanciaRealCm`.
  - **Separación:** nunca van a Firebase ni a `aqua-eventos-real`.
  - **Uso:** sirven para volumen y para evaluar modelos. La demo en vivo y la métrica final de cada modelo usan solo
    datos reales. Detalle en `docs/datos-aumentados.md`.
  - **Terminología:** se dice "datos aumentados" y "flota extrapolada", nunca "sintéticos".
  - **Dos modelos del generador:** f2b.1 (`generador.yml`, corrida de referencia `r2026-45facae2`) y f2b.2
    (`generador_f2b2.yml`: fugas persistentes de 1-4 días y toda llegada omitida etiquetada como corte).
- **Capa batch y dataset de modelos (F6 común):** Spark en batch (`batch/capas.py` + `batch/oro.py`, imagen
  `monitoraqua-streaming`, `local[4]`). Fuentes en `batch/datos/fuentes/` (`batch/descargar_fuentes.py`).
  - **Plata** (`batch/datos/plata/`): deduplicado por `eventId`, sin sensores de prueba, inválidas **marcadas** y
    nivel con `Detector.nivel` (guarda `geometria_version`).
  - **Oro** (`batch/datos/oro/`): `d2_autonomia`, `d4_llegadas`, `d6_noches` y `d8_termica`, por tramos continuos y
    sin interpolar huecos. Los eventos salen del detector congelado f4.3. En `d4_llegadas`,
    `artefacto_reanudacion = true` marca la reconexión del ESP32, que no es servicio real.
  - **Partición única:** `batch/particion_modelos.yml` (`p6.1`), columna `particion` en cada tabla oro. Es temporal.
    La prueba del ESP32 es intocable y no existe mientras la geometría sea provisional. Ningún modelo define su
    propia partición.
  - **Evaluación común:** `batch/evaluacion.py`. Métricas con IC por bootstrap de bloques; líneas base de
    persistencia y de tendencia, obligatorias; reporte separado por nivel (consistencia, ThingSpeak real, ESP32
    real). Los modelos D2, D4, D6 y D8 se evalúan solo con este módulo.
  - **SUNASS, INEI y SENAMHI:** no se usan (ninguna dimensión U2 los necesita).
  - Calidad y diccionario de datos en `docs/dataset-modelos.md`; evidencia en `docs/evidencias/f6/`.

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

Hardware real: **solo HC-SR04 (distancia) y DHT11 (aire de la cámara, ±2 °C, ±5 % HR)**. No hay caudalímetro,
presión, DS18B20 ni ORP. Por eso las dimensiones U2 se adecuaron en `docs/adecuacion-dimensiones-u2.md`, que
complementa el Brief S2 sin modificarlo.

| Dim. | Integrante | Indicador U2 | Señal / método | Fuentes | Fuera de alcance |
|---|---|---|---|---|---|
| D2 | Claudio Bustinza Inofuente | Horas de autonomía proyectadas | Tendencia del nivel (sin cambios) | ESP32 · ThingSpeak · flota extrapolada | — |
| D4 | Wilbert Alex Mayta Arotaype | Hora estimada de la próxima llegada | Llenados detectados en la serie de nivel + caudal de llenado estimado (L/min = velocidad de subida × capacidad / altura útil) | ESP32 · ThingSpeak · flota extrapolada | Presión de línea |
| D6 | Joel Huillca Lima | Pérdida proyectada por fuga | Nivel esperado vs. observado sobre distancia compensada con el DHT11 (sin cambios) | ESP32 · ThingSpeak · flota extrapolada | — |
| D8 | Josué Gabriel Ochoa Mamani | Temperatura del agua estimada, cruce del umbral, residencia | Agua estimada desde el aire de la cámara (DHT11) + Open-Meteo Juliaca, validada con ThingSpeak 3418755 (70 días de aire en tanque + agua, el agua sigue al aire con 5-7 h de atraso); residencia desde los llenados del ESP32 | ESP32 · Open-Meteo · ThingSpeak 3418755 · flota extrapolada | ORP |

Environment Agency y USGS no sostienen ninguna dimensión: aportan volumen real, demuestran el watermark de fuentes
lentas y muestran el escalado.

**Pendiente común:** la geometría real del tanque del ESP32 (altura útil y capacidad), que necesitan D2, D4 y D6
para pasar de cm a litros.

## Fases y mapa a la rúbrica U2

| Fase | Contenido | Criterio de la rúbrica |
|---|---|---|
| F0 | Preparación: estructura, contrato, `.env.example`, inventario del stack | — |
| F1 | Infraestructura: completar el stack con InfluxDB y Grafana (`infra/`) | Apoyo a 3, 4 y 6 |
| F2 | Ingesta unificada: colector de APIs y puente revisado (`ingesta/`) | 2 (IoT/sensores) |
| F2b | Datos aumentados: perfil real, flota extrapolada con anomalías etiquetadas, tópico aparte y Parquet | Apoyo a 3 y 7 (volumen, evaluación) |
| F3 | Spark Streaming: watermark, dedup, ventanas, checkpoint (`streaming/`) | 3 |
| F4 | Eventos de servicio derivados → `servicio-eventos` | 1 (eventos empresariales) |
| F5 | Grafana BI y observabilidad (`dashboards/`, alertas por provisioning) — hecha | 4 y 6 |
| F6 | Batch y modelos MLlib (`batch/`). Parte común hecha (capas, partición, evaluación); siguen F6-D2 (plantilla), D4, D6 y D8 | 7 (entrenamiento) |
| F7 | Inferencia en vivo de series de tiempo | 7 (validada e integrada al BI) |
| F8 | Costos, MkDocs y ensayo de demo. **Pendiente:** traer la configuración de Prometheus (`lambda26/obs/`, hoy solo en el repo local del curso, commit `c536731` sin push) a `infra/` de MonitorAqua | 5 y 8 (sustentación) |

Se trabaja **una fase a la vez**: no se escribe código de una fase posterior sin que se pida.

## Convenciones

- **Lenguaje y procesamiento:** Python 3 y PySpark. Los jobs de streaming corren con `spark-submit` como proceso persistente; los notebooks quedan para exploración y batch.
- **Tópicos:** `aqua-eventos-real` (lecturas, 3 particiones, key `sensorId`) y `servicio-eventos` (eventos de negocio).
- **Credenciales:** solo en `.env`, que nunca se versiona (plantilla en `.env.example`). Las cuentas de servicio de Firebase van en `credenciales/` y también quedan fuera de git.
- **Archivos locales:** los checkpoints van en `checkpoints/` y los datos locales en `data/local/`; ninguno se versiona.
- **Restricciones:** no modificar el firmware del ESP32, no crear otro cluster Kafka y no tocar el U1 (`pyspark/`, `data/`) sin preguntar.
- **Commits:** solo con aprobación explícita del usuario, en ramas por fase (`u2-faseN`).
- **Docker en Windows:** en Git Bash, `docker exec -w /opt/...` necesita `MSYS_NO_PATHCONV=1`, o usar PowerShell.
