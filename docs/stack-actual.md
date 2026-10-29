# Stack actual (cluster `lambda26` + capa MonitorAqua)

Estado verificado el 2026-10-28: F1 cerró con `infra/verificar_stack.py` en verde (12/12). En F2 se sumaron el puente,
el colector y un chequeo de ingesta viva (ver `docs/ingesta.md`).

## Quién levanta qué

El stack lo levantan tres compose: dos del repo del curso `C:\upeu\Big Data\u2\lambda26` y uno de este repo.
Ninguno duplica Kafka ni Prometheus.

| Compose | Proyecto | Servicios |
|---|---|---|
| `lambda26/kafka/compose.yml` | `lambda26-kafka` | Kafka, kafka-exporter, Kafbat UI |
| `lambda26/obs/compose.yml` | `lambda26-obs` | **solo Prometheus** (su servicio `grafana` ya no se usa, ver abajo) |
| `MonitorAqua/infra/compose.yml` | `monitoraqua-infra` | InfluxDB 2, Grafana, puente Firebase → Kafka, colector de APIs, Spark Structured Streaming |

Otros contenedores de lambda26:

- **`lambda26-pyspark`** (Jupyter + Spark): lo sigue usando el proyecto.
- **`lambda26-aqua-bridge`** (puente de la S08): reemplazado en F2 por `lambda26-aqua-puente`. Queda **detenido y con
  `restart=no`**, sin borrar. Nunca correrlo junto al nuevo, porque cada lectura llegaría dos veces a Kafka.
- **`lambda26-aqua-consumer`** (S09): detenido. Su grupo `aqua-monitor-group` se **borró en F5** porque estaba
  abandonado, con lag creciente. Si se vuelve a encender el contenedor, el grupo reaparece. El consumidor vigente es
  `lambda26-aqua-consumidor-eventos` (grupo `aqua-servicio-consumidor`).

!!! warning "Grafana ahora pertenece a `infra/compose.yml`"
    El contenedor `lambda26-grafana` se recreó desde este repo, con el mismo nombre, el mismo puerto 43000 y el mismo
    volumen de datos (`lambda26-obs_lambda26_grafana_data`). Ahora toma el provisioning de `infra/grafana/` y los
    tableros de `dashboards/`, donde está la copia del tablero S09. **No volver a levantar el servicio `grafana` de
    `lambda26/obs`**: chocaría por el nombre del contenedor. Para Prometheus usar
    `docker compose -f obs/compose.yml up -d prometheus`.

## Contenedores y puertos

Todos están en la red Docker **`lambda26-kafka-net`**.

| Contenedor | Imagen | Host → contenedor | Nombre en la red |
|---|---|---|---|
| `lambda26-kafka` | `apache/kafka:4.3.1` (KRaft, 1 nodo) | `49092 → 49092` | `kafka:9092` |
| `lambda26-kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `48085 → 8080` | — |
| `lambda26-kafka-exporter` | `danielqsj/kafka-exporter:v1.10.0` | `49308 → 9308` | `kafka-exporter:9308` |
| `lambda26-prometheus` | `prom/prometheus:v3.14.0` | `49090 → 9090` | `prometheus:9090` |
| `lambda26-grafana` | `grafana/grafana:11.4.0` | `43000 → 3000` | — |
| `lambda26-influxdb` | `influxdb:2.7.12` | `48086 → 8086` | `influxdb:8086` |
| `lambda26-pyspark` | build local (Python 3.10, Java 21, PySpark 4.2.0) | `4488 → 8888` (Jupyter), `4042 → 4040` (Spark UI) | — |
| `lambda26-aqua-puente` | `monitoraqua-ingesta` (build de `ingesta/Dockerfile`) | — | — |
| `lambda26-aqua-colector` | `monitoraqua-ingesta` | — | — |
| `lambda26-aqua-consumidor-eventos` | `monitoraqua-ingesta` (consumidor de `servicio-eventos`, grupo `aqua-servicio-consumidor`) | — | — |
| `lambda26-aqua-volumen` | `monitoraqua-ingesta` (bytes y mensajes por tópico cada 5 min → InfluxDB `kafka_volumen`) | — | — |
| `lambda26-grafana-renderer` | `grafana/grafana-image-renderer:3.11.0` (capturas de tableros, `/render`) | — | `renderer:8081` |
| `lambda26-aqua-streaming` | `monitoraqua-streaming` (build de `streaming/Dockerfile`, PySpark 4.2.0) | `4043 → 4040` (Spark UI) | — |

## Kafka

- **Bootstrap:** `localhost:49092` desde el host, `kafka:9092` desde un contenedor en `lambda26-kafka-net`.
- **UI:** cluster `lambda26` en Kafbat UI, http://localhost:48085.
- **Auto-creación de tópicos:** `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`, pero un tópico creado solo queda con 1 partición. Los tópicos del proyecto se crean siempre a mano.
- **Sin volumen:** el compose de Kafka no monta volumen, así que los mensajes viven en la capa del contenedor. Se usa `docker start`/`stop` y **nunca** `docker compose down` ni `up --force-recreate`, que borrarían el historial.

| Tópico | Particiones | Retención | Mensajes (28-oct) | Key | Uso |
|---|---|---|---|---|---|
| `aqua-eventos-real` | 3 | 7 días (`retention.ms=604800000`) | 4,228 al cierre de F2: 2,649 del puente de la S08 (offsets 0–2648, partición 0) y el resto del puente nuevo | `sensorId` | lecturas de todas las fuentes |
| `servicio-eventos` | 3 | 7 días (`retention.ms=604800000`) | 51 al arrancar F4 (historial real) | `sensorId` | eventos de negocio derivados por Spark (F4; contrato en `docs/eventos-servicio.md`) |
| `aqua-eventos-aumentados` | 3 | 7 días (`retention.ms=604800000`, `CreateTime`) | 172,800 (corrida `r2026-45facae2`, F2b) | `sensorId` | datos aumentados de la flota extrapolada; nunca se mezclan con el real (`docs/datos-aumentados.md`) |

Con 42 sensores (ESP32 + 41 de API) la clave `sensorId` ya reparte entre las 3 particiones. El ESP32 sigue en la
partición 0.

Así se creó `servicio-eventos`:

```bash
MSYS_NO_PATHCONV=1 docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --if-not-exists --topic servicio-eventos --partitions 3 --replication-factor 1 --config retention.ms=604800000
```

## Spark

- **Dónde corre:** dentro del contenedor `lambda26-pyspark`, con PySpark **4.2.0** (Scala 2.13), Java 21 y Python 3.10. No hay Spark instalado en Windows.
- **Conector:** `org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0`, ya cacheado en el Ivy del contenedor (junto con `kafka-clients-3.9.2`).
- **Trampa con `spark-submit`:** el conector se pasa con `--packages`. Ponerlo en `spark.jars.packages` dentro del builder no tiene efecto, porque la JVM ya arrancó, y falla con `Failed to find data source: kafka`.
- **Montaje:** el contenedor monta `lambda26/pyspark/sesiones → /opt`, no este repo. Para correr scripts de MonitorAqua se copian con `docker cp`. En F3 se decide si el job de streaming tiene su propio contenedor.

Prueba mínima (`infra/prueba_spark_kafka.py`), que leyó los 10 últimos mensajes sin errores:

```bash
docker cp infra/prueba_spark_kafka.py lambda26-pyspark:/tmp/
MSYS_NO_PATHCONV=1 docker exec lambda26-pyspark spark-submit --conf spark.ui.enabled=false \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 /tmp/prueba_spark_kafka.py
```

```text
Spark 4.2.0 · kafka:9092 · aqua-eventos-real · 2649 mensajes en el tópico
|0 |2648|2026-10-23 02:11:09.563|{"tipoEvento": "aqua.tanque.lectura", "eventId": "-P4aWg-9A8Y3N3drWoH-", "sensorId": "esp32-tanque-real-01", "distanciaCm": 16.2, ...}
```

## InfluxDB 2

- **Acceso:** http://localhost:48086, org `horeb`, bucket **`monitoraqua`** con retención de **30 días**. Treinta días cubren el ciclo de U2 y la demo de S12; el histórico largo va a Parquet en la capa batch (F6).
- **Credenciales:** usuario, contraseña y token de administrador viven solo en `.env` (`INFLUX_*`). El setup inicial (`DOCKER_INFLUXDB_INIT_*`) solo se aplica la primera vez que se crea el volumen `monitoraqua_influxdb_data`.

## InfluxDB: buckets (F5)

| Bucket | Retención | Contenido |
|---|---|---|
| `monitoraqua` | 30 días | todo lo real en vivo (Spark, `aqua-volumen`) |
| `monitoraqua_aumentados` | sin vencimiento | flota extrapolada (corrida `r2026-45facae2`, fechas simuladas 28-sep a 27-oct), cargada por `streaming/cargar_historico_influx.py` |

## Grafana

- **Acceso:** http://localhost:43000, con las credenciales de `.env` (`GRAFANA_ADMIN_*`, que solo aplican si el volumen es nuevo; el volumen actual conserva `admin`).
- **Datasources provisionados (`infra/grafana/provisioning/datasources/`):**
  - `Prometheus` (uid `prometheus`, por defecto): `http://prometheus:9090`. Responde "Successfully queried the Prometheus API".
  - `InfluxDB` (uid `influxdb`, Flux): `http://influxdb:8086`, con el token tomado de la variable `INFLUX_TOKEN`. Responde "datasource is working".
- **Tableros:** se cargan de `dashboards/` a la carpeta *MonitorAqua*. Hoy está "S9 - Observabilidad del pipeline MonitorAqua".

## Prometheus y métricas de Kafka

Prometheus raspa `prometheus` y `kafka-exporter:9308` cada 15 s, y ambos targets están `up`. Hoy no hay métricas de Spark: se agregan en F5.

Métricas de Kafka disponibles:

| Métrica | Para qué sirve |
|---|---|
| `kafka_topic_partition_current_offset` | último offset por partición: con `rate()` da los **mensajes por segundo** de cada tópico |
| `kafka_topic_partition_oldest_offset` | primer offset retenido: con el anterior da los mensajes guardados |
| `kafka_topic_partitions` | particiones por tópico |
| `kafka_consumergroup_lag`, `kafka_consumergroup_lag_sum` | **lag** por grupo/partición y total |
| `kafka_consumergroup_current_offset[_sum]` | offset confirmado por el grupo |
| `kafka_consumergroup_members` | consumidores activos del grupo |
| `kafka_brokers`, `kafka_broker_info` | brokers vivos |
| `kafka_topic_partition_in_sync_replica`, `..._under_replicated_partition`, `..._leader`, `..._leader_is_preferred`, `..._replicas` | salud de la replicación |

Valores el 28-oct:

- **Mensajes por tópico:** `aqua-eventos-real`=2649, `servicio-eventos`=0.
- **Lag:** `aqua-monitor-group` sobre `aqua-eventos-real` = 1.

Hay un límite: Spark Structured Streaming **no registra consumer group** en Kafka, así que su lag no aparece aquí. Por eso existe `aqua-consumer`; el progreso de Spark se medirá con sus propias métricas en F5.

## Verificación en un comando

```bash
.venv/Scripts/python -m pip install -r infra/requirements.txt   # una vez
.venv/Scripts/python infra/verificar_stack.py
```

Chequea Kafka por protocolo desde el host, los dos tópicos con sus particiones y mensajes, la salud de InfluxDB y el
bucket, Grafana y la salud de sus dos datasources, los targets de Prometheus y `kafka_brokers` del exporter. Desde F2
también chequea la **ingesta viva**: el último mensaje de `aqua-eventos-real` debe tener menos de 120 s. Sale con
código 1 si algo falla.

Los chequeos de datasources usan `GRAFANA_ADMIN_USER` y `GRAFANA_ADMIN_PASSWORD` de `.env`. Si la contraseña de
`admin` se cambia en la interfaz de Grafana, hay que actualizarla también en `.env`; si no, esos dos chequeos dan 401.

## Arranque del stack (orden)

```bash
docker start lambda26-kafka lambda26-kafka-exporter lambda26-kafka-ui lambda26-prometheus   # existentes
docker compose -f infra/compose.yml --env-file .env up -d   # InfluxDB, Grafana, puente, colector y Spark streaming
docker start lambda26-pyspark                                                                 # si se usa Spark
```

## Trampas conocidas

- **Tópico tras un reset de Docker:** si Kafka se recrea, crear `aqua-eventos-real` y `servicio-eventos` con 3 particiones **antes** de arrancar el puente.
- **Rutas en Git Bash:** `docker exec` con rutas `/opt/...` necesita `MSYS_NO_PATHCONV=1`; si no, Git Bash las convierte en `C:/Program Files/Git/opt/...`.
- **Montaje de `lambda26-pyspark`:** a veces queda apuntando a otra copia del repo del curso; verificarlo con `docker inspect lambda26-pyspark`.
- **Reloj del PC:** va unos 1.4 s atrasado respecto a la hora NTP que usa el ESP32; las latencias calculadas en el host pueden salir negativas.
- **Python local:** usar 3.12 para `.venv`. En 3.15 PyYAML (y por tanto mkdocs) no instala sin compilador de C++.
