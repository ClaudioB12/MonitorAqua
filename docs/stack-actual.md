# Stack actual (cluster `lambda26`)

Inventario de solo lectura, tomado el 2026-10-28 con `docker ps -a` y `docker inspect`, y con los `compose.yml` de
`C:\upeu\Big Data\u2\lambda26` (`kafka/`, `obs/`, `aqua-bridge/`, `pyspark/`). No se modificó nada.

!!! warning "Estado al momento del inventario"
    Docker Desktop estaba encendido, pero **todos los contenedores estaban detenidos**. Kafka, el puente y
    pyspark se detuvieron el 2026-10-23; Prometheus y Grafana, el 2026-10-16. Los puertos y la red salen de la
    configuración de los contenedores, no de un servicio respondiendo.

## Contenedores

| Contenedor | Imagen | Puerto host → contenedor | Proyecto compose | Estado |
|---|---|---|---|---|
| `lambda26-kafka` | `apache/kafka:4.3.1` (KRaft, 1 nodo) | `49092 → 49092` | `lambda26/kafka` | detenido |
| `lambda26-kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | `48085 → 8080` | `lambda26/kafka` | detenido |
| `lambda26-kafka-exporter` | `danielqsj/kafka-exporter:v1.10.0` | `49308 → 9308` | `lambda26/kafka` | detenido |
| `lambda26-prometheus` | `prom/prometheus:v3.14.0` | `49090 → 9090` | `lambda26/obs` | detenido |
| `lambda26-grafana` | `grafana/grafana:11.4.0` | `43000 → 3000` | `lambda26/obs` | detenido |
| `lambda26-aqua-bridge` | build local (`aqua-bridge/`) | — | `lambda26/aqua-bridge` | detenido |
| `lambda26-aqua-consumer` | build local (`aqua-bridge/`) | — | `lambda26/aqua-bridge` | detenido |
| `lambda26-pyspark` | build local (Jupyter + Spark) | `4488 → 8888`, `4042 → 4040` | `lambda26/pyspark` | detenido |
| `lambda26-uso-campo-electrico` | build local | — | (ajeno a MonitorAqua) | detenido |

Todos están en la red Docker **`lambda26-kafka-net`**. `pyspark` además está en `lambda26-pyspark_default`.

## Kafka: bootstrap server real

Según `KAFKA_ADVERTISED_LISTENERS` en `lambda26/kafka/compose.yml`:

| Desde dónde | Bootstrap |
|---|---|
| Host (Windows, scripts locales) | `localhost:49092` (listener `EXTERNAL`) |
| Contenedor en `lambda26-kafka-net` | `kafka:9092` (listener `INTERNAL`) |

- Nombre del cluster en Kafbat UI: `lambda26`.
- `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true`: un tópico que se crea solo queda con **1 partición**.

## Tópicos conocidos

| Tópico | Particiones | Productor | Consumidores |
|---|---|---|---|
| `aqua-eventos-real` | 3 | `lambda26-aqua-bridge` | Spark (notebooks S08–S10), `aqua-consumer` (grupo `aqua-monitor-group`) |
| `servicio-eventos` | — | (pendiente, F4) | — |

La lista viva está pendiente de verificar con el stack arriba:

```bash
docker exec lambda26-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe
```

## Observabilidad

- Prometheus raspa `localhost:9090` y `kafka-exporter:9308` cada 15 s.
- Grafana entra con `admin`/`admin`. Tiene provisionado el datasource `prometheus` y el tablero `s09-monitoraqua`.
- Todavía **no hay métricas de Spark**: se agregan en F5.

## Puente Firebase → Kafka existente

`lambda26/aqua-bridge/bridge_firebase_kafka.py`:

- **Lectura de Firebase:** escucha `FIREBASE_PATH=/board1/lecturas/historial` por SSE, autenticado con `FIREBASE_API_KEY`, `FIREBASE_EMAIL` y `FIREBASE_PASSWORD`.
- **Publicación en Kafka:** publica en `aqua-eventos-real` con key `sensorId`, convierte -1 en `null` y tiene un modo de reenvío `--replay N --once`.
- Se revisa en F2.

## Trampas conocidas

- **Tópico tras un reset de Docker:** si se resetea Docker, crear `aqua-eventos-real` con 3 particiones **antes** de arrancar el puente; si no, se crea solo con 1 partición.
- **Montaje de `lambda26-pyspark`:** monta `C:\upeu\Big Data\u2\lambda26\pyspark\sesiones → /opt`. A veces queda apuntando a otra copia del repo; verificarlo con `docker inspect lambda26-pyspark`.
- **Rutas en Git Bash:** `docker exec -w /opt/...` falla por la conversión de rutas; usar PowerShell o `MSYS_NO_PATHCONV=1`.
- **Reloj del PC:** va unos 1.4 s atrasado respecto a la hora NTP que usa el ESP32, así que las latencias calculadas en el host pueden salir negativas.

## Arranque (referencia, no ejecutado)

```bash
cd "C:/upeu/Big Data/u2/lambda26"
docker compose -f kafka/compose.yml up -d
docker compose -f obs/compose.yml up -d
docker compose -f aqua-bridge/compose.yml up -d
```
