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
    ventana de 6 h es la protección. El workflow vive en `main` y usa el código de `main` (la variable temporal
    `COLECTOR_REF` quedó sin uso con la fusión U2 del 2026-10-29). Credenciales solo como Secrets de GitHub. Si cambia el colector, se prueba con `--seco --ventana-min 360` antes del push.
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
  - **Reglas CONGELADAS en f4.4**, desde la hora y con el commit de `reglas_servicio.yml` → `congelamiento`
    (f4.3 estuvo congelada desde 2026-10-28 23:37:40, commit `b400e7d`). Lo real anterior al congelamiento es el
    conjunto de diseño y nunca se reporta como rendimiento. El rendimiento real sale solo de lecturas y bitácora
    posteriores (`streaming/comparar_bitacora.py`). Un cambio de reglas exige versión y congelamiento nuevos.
  - **f4.4** corrige las tres limitaciones de f4.3: subida sostenida en canales de nivel (%/m), intermitencia del
    sensor (4 eventos en 5 min → inestable) y reanudación tras huecos de datos. Comparación lado a lado:
    `streaming/comparar_versiones.py` (evidencia en `docs/evidencias/f4/f4.4/`). Los IC de f4.3 y f4.4 se solapan
    en los datos aumentados: lo demostrable es la eliminación de los falsos positivos conocidos del historial.
  - **El repositorio no se edita en caliente:** el contenedor monta `streaming/` e `ingesta/` y relee
    `reglas_servicio.yml` y `tanques.yml` al cambiar. Unas reglas nuevas se preparan en otro archivo y se copian
    recién al desplegar el código que las entiende.
  - **Compensación térmica en el firmware:** el firmware del ESP32 (`firmware/esp32/MonitorAqua_esp32.ino`; `secrets.h`
    nunca a git) ya compensa la distancia con el DHT11. `tanques.yml` → `compensacion_en_firmware.desde` evita la
    doble corrección (supuesto documentado: desde el inicio; confirmado desde el 28-oct). La flota (f2b.1/f2b.2)
    imita un sensor sin compensación; **f2b.3** (`generador_f2b3.yml`) imita al ESP32. El sensor real es un
    JSN-SR04T/AJ-SR04M con zona ciega de ~20-25 cm.
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
  - **Tres modelos del generador:** f2b.1 (`generador.yml`, corrida de referencia `r2026-45facae2`), f2b.2
    (`generador_f2b2.yml`: fugas persistentes de 1-4 días y toda llegada omitida etiquetada como corte) y f2b.3
    (`generador_f2b3.yml`: f2b.2 con la compensación en el firmware, DHT11 ±2 °C redondeado; solo sensibilidad).
- **Capa batch y dataset de modelos (F6 común):** Spark en batch (`batch/capas.py` + `batch/oro.py`, imagen
  `monitoraqua-streaming`, `local[4]`). Fuentes en `batch/datos/fuentes/` (`batch/descargar_fuentes.py`).
  - **Plata** (`batch/datos/plata/`): deduplicado por `eventId`, sin sensores de prueba, inválidas **marcadas** y
    nivel con `Detector.nivel` (guarda `geometria_version` y `compensacionOrigen`: detector, firmware o
    desconocida; `d6_noches` excluye "desconocida").
  - **Oro** (`batch/datos/oro/`): `d2_autonomia`, `d4_llegadas`, `d6_noches` y `d8_termica`, por tramos continuos y
    sin interpolar huecos. Los eventos salen del detector con las reglas de `reglas_servicio.yml` **del momento de
    la corrida**: la última regeneración (29-oct, antes de desplegar f4.4) usó f4.3, y es la entrada de d2.1-d2.2 y
    d4.1. Una corrida de `capas.py` posterior al congelamiento de f4.4 recalcula `d4_llegadas` con f4.4 (entrada
    de d4.2). En `d4_llegadas`,
    `artefacto_reanudacion = true` marca la reconexión del ESP32, que no es servicio real.
  - **Partición única:** la vigente es **`p6.2`** (`batch/particion_modelos_p6.2.yml`, `particion.VIGENTE`); `p6.1`
    (`batch/particion_modelos.yml`) se conserva sin cambios para reproducir d2.1-d2.2 y d4.1. p6.2 solo cambia el
    inicio de la prueba del ESP32 al congelamiento de f4.4 (legítimo: la prueba nunca empezó). Columna
    `particion` en cada tabla oro; flota y ThingSpeak salen idénticas con p6.1 y p6.2. Es temporal.
    La prueba del ESP32 es intocable y no existe mientras la geometría sea provisional. Ningún modelo define su
    propia partición.
  - **Evaluación común:** `batch/evaluacion.py`. Métricas con IC por bootstrap de bloques; líneas base de
    persistencia y de tendencia, obligatorias; reporte separado por nivel (consistencia, ThingSpeak real, ESP32
    real). Los modelos D2, D4, D6 y D8 se evalúan solo con este módulo.
  - **SUNASS, INEI y SENAMHI:** no se usan (ninguna dimensión U2 los necesita).
  - Calidad y diccionario de datos en `docs/dataset-modelos.md`; evidencia en `docs/evidencias/f6/`.
- **Modelo D2, horas de autonomía (F6-D2, plantilla para D4, D6 y D8):** `batch/modelo_d2.py` + `batch/modelo_d2.yml`,
  corre con `spark-submit` en la imagen `monitoraqua-streaming` (~12 min).
  - **Principal:** Holt (nivel + tendencia) en `batch/holt.py`, sin dependencias, para reutilizarlo tal cual en el
    streaming de F7, **ajustado por el error de autonomía** (ajustado por el nivel a 15-60 min, en ThingSpeak se
    vuelve persistencia y no proyecta bajadas; queda como `holt_pronostico`, solo comparación). **Comparación:**
    `GBTRegressor` de Spark MLlib (cambio de nivel a 15 y 60 min).
  - **Resultado d2.1:** en datos reales ningún modelo supera a la persistencia (nivel) ni a las líneas base
    (autonomía, 2 episodios no censurados); el consumo intermitente es la limitación principal. ESP32 pendiente.
  - **Autonomía** = horas hasta el umbral (`umbral_pct`, 10 %) con la pendiente reciente. Un llenado (subida > 10 pp
    sostenida, sobre la mediana de 15 min) antes del umbral **censura** el caso: solo se evalúa lo no censurado y se
    informan episodios y censurados.
  - **Ajuste** solo con la partición `entrenamiento` de p6.1 (su último 20 % es el conjunto de ajuste);
    prueba/consistencia solo al final. `evaluacion.py` ganó el bootstrap por grupos (`grupos=`), un grupo por episodio.
  - **Modelos** en `batch/modelos/d2/<version>-<fecha>/`: `holt.json` y `manifiesto.json` se versionan; los GBT de
    Spark (`gbt_*/`) no (`.gitignore`), se regeneran con el script.
  - **Snapshot de datos:** re-descargar las fuentes mueve la ventana de prueba de ThingSpeak (últimos 7 días de cada
    canal); los resultados de D2 valen para el snapshot de `docs/evidencias/f6-d2/manifiesto_fuentes_d2.json`.
  - **d2.2** (`batch/modelo_d2_perfil.py` + `.yml`, `batch/perfil_consumo.py`, ~2 min): perfil de consumo por
    día × hora (P10/P50/P90) aprendido del entrenamiento de cada tanque; autonomía proyectando el perfil, con
    banda P10-P90. Bases: consumo medio y Holt d2.1 (re-aplicados sin reentrenar, mismas filas). Métricas: MAE
    en horas (no censurados) + **C-index** (`evaluacion.evaluar_concordancia`, usa los censurados).
    Resultado: autonomía de 30-35 h de error (d2.1) a 2-7 h; el **consumo medio** gana al perfil en MAE; en
    nivel a 1 h la persistencia sigue imbatible en datos reales. Modelo F7: `batch/modelos/d2/d2.2-*/perfil.json`.
    **F7 (decisión del equipo):** principal = **perfil** (mejor en el consumo observado); comparación = consumo medio;
    se muestra hasta **12 h** (autonomía > 12 h -> "más de 12 h"; 24 h no se muestra); rango = banda P10-P90 calibrada
    por predicción conforme (factor 1.03 flota, 1.80 ThingSpeak/ESP32); advertir que en ThingSpeak el perfil es
    optimista. Se reportan C-index global y por
    tanque (el global es optimista).
    **Evidencia por umbral** (10/25/50 %, `evaluacion` en `modelo_d2_perfil.yml`): suficiente = ≥ 10 episodios no
    censurados; IC del MAE por episodio, "no informativo" con < 5. Hoy **ningún umbral es suficiente** (máx. 5
    episodios al 50 % en la flota): la cifra al 10 % es indicativa; hay sesgo de selección por censura (los no
    censurados son los vaciados rápidos).
    **Evidencia principal = consumo previsto** (6/12/24 h vs consumo real, pasos de llenado excluidos, IC por bloques
    por tanque, sesgo): el perfil es el mejor en la flota y empata o gana por poco en ThingSpeak; la autonomía al 10 %
    es indicativa. **Pendiente prueba en vivo del ESP32:** vaciados controlados registrados en la bitácora (episodios
    reales de autonomía, sin censura ni sesgo de selección).
  - Detalle y sustentación en `docs/modelo-d2.md`; evidencia en `docs/evidencias/f6-d2/`.
- **Modelo D4, próxima llegada de agua (F6-D4, d4.1):** `batch/modelo_d4.py` + `.yml` y `batch/llegadas.py` (sin
  dependencias, para F7), ~2 min en `.venv`.
  - **Llegadas:** `d4_llegadas` (detector f4.3) sin `artefacto_reanudacion` ni `no_verificable` (se reportan aparte).
    **El detector f4.3 no ve los llenados de los canales nivelPct de ThingSpeak** (1233810 tiene 36 por subida
    sostenida y 0 en el detector): complemento rotulado "subida sostenida" (> 10 pp, criterio de D2/adecuación).
    f4.4 ya los detecta (criterio "sostenida"); d4.1 se queda con f4.3 y el complemento.
  - **Modelo:** supervivencia en tiempo discreto por hora: riesgo según horas desde la última llegada (Kaplan-Meier o
    Weibull con censura si hay ≥ 10 intervalos) × hora del día, por IPF; la flota se agrupa por zona. Salidas: hora
    más probable, ventana P10-P90 calibrada (conforme, ajuste de p6.1, ≥ 5 episodios) y probabilidad de llegada en
    6/12/24 h (`evaluacion.evaluar_probabilidad`: Brier, skill, calibración).
  - **Alcance:** la conclusión se apoya en la flota y en ThingSpeak **1233810** (red). **3418755 (agua de lluvia) está
    fuera del alcance de D4** (`fuera_de_alcance` en `modelo_d4.yml`) y se reporta aparte.
  - **Resultado:** flota: Weibull 1.6 h vs bases 2.1 h (IC solapados) y skill 0.64 vs 0.50 a 6 h; a 24 h gana la base
    "misma hora" (cambio de ritmo 25 -> 23.3 h; mejora d4.2: más peso a intervalos recientes). 1233810: 5 episodios,
    indicativo, ~20 h de error para todos, con sesgo de selección (29 % censurados). ESP32 pendiente. Detalle en `docs/modelo-d4.md`; evidencia en `docs/evidencias/f6-d4/`.

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
| F6 | Batch y modelos MLlib (`batch/`). Parte común hecha (capas, partición, evaluación), F6-D2 (autonomía, plantilla) y F6-D4 (próxima llegada); siguen D6 y D8 | 7 (entrenamiento) |
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
