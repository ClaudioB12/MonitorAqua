# Costos y criterios de escalado (F8, criterio 5)

**Qué contiene esta página:**

1. El volumen real del pipeline, medido.
2. Los escenarios de crecimiento: hoy, 100 tanques y toda Juliaca.
3. Su costo mensual en servicios administrados, con precios de las páginas oficiales.
4. La prueba de carga que mide dónde se satura el stack actual.
5. Los criterios para agregar particiones, brokers o ejecutores.

**Cálculo reproducible:**

- **Escenarios y costos:** `infra/costos_escenarios.py`.
- **Prueba de carga:** `infra/prueba_carga.py`.
- **Evidencia:** `docs/evidencias/f8/`.

## 1. Volumen real medido

**Tabla 1. Volumen del pipeline (medido el 2026-10-30, salvo donde se indica)**

| Dato | Valor | Fuente | Fecha |
|---|---|---|---|
| Eventos/día en `aqua-eventos-real` | **19,115**: ESP32 13,795 · ThingSpeak 2,799 · EA UK 1,863 · USGS 573 · Open-Meteo 85 | bronce Parquet, por `ingestaTs` | día completo 2026-10-29 |
| Tasa de publicación en `aqua-eventos-real` | 19,660 mensajes en 22.66 h → **~20,800/día (0.24 ev/s)** | `kafka_volumen` (panel "Bytes por día por tópico"), `mensajes_publicados` 8,418 → 28,078 | 29-oct 05:04 → 30-oct 03:43 UTC |
| Bytes en disco de `aqua-eventos-real` | 9.4 MB para 25,533 mensajes retenidos → **369 B/mensaje** (~7.7 MB/día) | `kafka_volumen.bytes` (`describe_log_dirs`) | 30-oct 03:43 UTC |
| `aqua-eventos-aumentados` | 172,800 mensajes, 81.1 MB (469 B/mensaje). Carga única de la flota, no diaria | `kafka_volumen` | 30-oct |
| `servicio-eventos` | 60 → 87 mensajes en 22.66 h (**~29/día**), 42.6 KB (490 B/mensaje) | `kafka_volumen` | 29-30 oct |
| Disco total de Kafka | 101 MB (`aqua-eventos-real` 9.2 MB, `aqua-eventos-aumentados` 78 MB, `servicio-eventos` 104 KB) | `du` en el contenedor | 30-oct |
| Escrituras en Firebase | ~19,000/día: el ESP32 escribe cada 5 s (17,280/día nominales, 13,795 el 29-oct) y el colector ~5,300/día | bronce (todo lo que entra a Firebase pasa a Kafka) | 29-oct |
| Almacenamiento en Firebase | **30,261 registros, 5.29 MB de JSON (175 B/registro)** desde el 2-oct; plan gratuito: 1 GB | lectura completa de `/board1/lecturas/historial` (solo lectura) | 30-oct 03:47 UTC |
| InfluxDB | motor 15 MB: `monitoraqua` 3.3 MB, `monitoraqua_aumentados` 8.9 MB, `_monitoring` 0.24 MB | `du` en `/var/lib/influxdb2/engine/data` | 30-oct |
| Escrituras a InfluxDB | **390,216 valores/día** en `monitoraqua`: `spark_progreso` 99k, `nivel_tanque` 82.5k, `lecturas_agg` 73k, `lecturas_esp32` 70k, `latencia_ingesta` 41.5k, `estado_tanque` 15k, `predicciones` 4.7k… | Flux `count()` por measurement, 24 h | 30-oct |
| Series en InfluxDB | 151 en `monitoraqua`; **16-20 por tanque** (ESP32 16; un tanque de la flota con predicciones 20) | `influxdb.cardinality()` | 30-oct |
| Parquet | bronce 10.9 MB (28,200 filas, **386 B/fila**, archivos chicos sin compactar); plata 19.0 MB; oro 4.6 MB; fuentes 19.5 MB; aumentados 9.5 MB | tamaño en disco | 30-oct |
| Prometheus | 24.1 MB | `du /prometheus` | 30-oct |
| GitHub Actions | 5 ejecuciones del colector (4 el 29-oct, 1 el 30-oct), ~25-30 s cada una. El cron pide 48 al día; GitHub ejecutó muchas menos. Con 48/día serían ~1,440 min/mes | API de Actions (`/actions/runs`) | 29-30 oct |

**Lectura:**

- **Volumen diminuto:** hoy el pipeline mueve **0.22-0.24 eventos/s**. Todo el historial cabe en ~200 MB.
- **Lo que pesa por tanque:**
  - **InfluxDB:** cada lectura del ESP32 se convierte en ~14 valores escritos (detalle, nivel, estado, latencia,
    agregados).
  - **Firebase:** un tanque escribe cada 5 s.
- Esas dos son las variables que deciden el costo al crecer.

## 2. Escenarios

**Fuentes del número de tanques de Juliaca:**

| Escenario | Número | Fuente |
|---|---|---|
| **Principal** | **53,724 conexiones activas de agua potable** de EPS SEDAJULIACA S.A. a octubre de 2023 (64,347 conexiones totales, 17 % inactivas) | SUNASS, *Estudio tarifario de EPS SEDAJULIACA S.A., periodo regulatorio 2025-2029*, p. 45, párrafo 74 y Cuadro 14. https://cdn.www.gob.pe/uploads/document/file/6722102/5351549-estudio-tarifario-final-aprobado-texto-completo.pdf (consultado el 2026-10-30) |
| **Conservador** | **35,160 viviendas con red pública** en el distrito de Juliaca: 33,567 dentro de la vivienda y 1,593 fuera de la vivienda pero dentro de la edificación, de 56,995 viviendas con ocupantes presentes | INEI, *Puno: Resultados Definitivos de los Censos Nacionales 2017*, Tomo XII, Cuadro 9, p. 14628. https://www.inei.gob.pe/media/MenuRecursivo/publicaciones_digitales/Est/Lib1563/21TOMO_12.pdf (consultado el 2026-10-30) |

**Por qué difieren (53,724 frente a 35,160):**

- **Hay seis años de diferencia:** el censo es de 2017; las conexiones de SUNASS, de octubre de 2023.
  - **El Cuadro 14 de SUNASS** muestra el crecimiento: 50,699 conexiones activas en 2019 y 53,724 en 2023.
- **Abarcan territorios distintos:** el INEI cuenta el distrito de Juliaca. SEDAJULIACA atiende la ciudad, que se
  extiende sobre parte de otros distritos, y factura también conexiones no domésticas (comercio, Estado).
- **El 35 % de las viviendas del distrito usaba pozo en 2017** (20,054): son hogares sin red que el censo no cuenta.

**Supuestos:**

- **Un tanque monitoreado por conexión.** Es un techo: no toda conexión tiene tanque elevado ni querría el sensor. Si
  la adopción fuera parcial, se escala linealmente.
- **Cadencia de 5 s por tanque,** la del ESP32 actual: 17,280 lecturas/día.
- **Más el colector de APIs,** con ~5,300 lecturas/día, que no crecen con los tanques.

**Tabla 2. Escenarios de volumen** (`infra/costos_escenarios.py`, capacidad de Spark medida en la sección 4)

<!-- ESCENARIOS -->

| | Actual (1 ESP32 + 7 ThingSpeak + APIs) | 100 tanques | Juliaca conservador: 35,160 viviendas con red pública (INEI 2017) | Juliaca principal: 53,724 conexiones activas (SUNASS, oct-2023) | Juliaca principal, diseño a escala |
|---|---:|---:|---:|---:|---:|
| tanques | 8 | 100 | 35,160 | 53,724 | 53,724 |
| eventos_dia | 19,115 | 1,733,320 | 607,570,120 | 928,356,040 | 77,367,880 |
| eps | 0.22 | 20.06 | 7,032 | 10,745 | 895 |
| brokers | 1 | 3 | 3 | 3 | 3 |
| gb_mes_kafka_ingreso | 0.21 | 19.19 | 6,726 | 10,277 | 856 |
| gb_kafka_retenido | 0.05 | 4.48 | 1,569 | 2,398 | 200 |
| gb_mes_parquet | 0.22 | 20.07 | 7,036 | 10,750 | 896 |
| gb_mes_firebase | 0.10 | 9.10 | 3,190 | 4,874 | 0.00 |
| mb_dia_influx | 5.47 | 496 | 173,765 | 265,510 | 15,474 |
| series_influx | 260 | 2,100 | 703,300 | 1,074,580 | 1,074,580 |

!!! warning "Pendiente de la prueba de carga limpia"
    Las filas de particiones, núcleos de Spark y ejecutores dependen de la capacidad de Spark por núcleo, que mide la sección 4. Se agregan cuando esa prueba termine.


## 3. Costo mensual estimado

**Tabla 3. Precios de lista usados** (USD, región us-east-1 o la región estándar; consultados el 2026-10-29/30)

| Servicio | Precio | Página oficial |
|---|---|---|
| Confluent Cloud Basic | primer eCKU gratis, luego $0.14/eCKU-h; red $0.05/GB (ingreso y egreso); almacenamiento $0.08/GB-mes; hasta 1,500 particiones | https://www.confluent.io/confluent-cloud/pricing/ |
| Confluent Cloud Standard | $0.75/eCKU-h; red $0.035-0.05/GB; $0.08/GB-mes | ídem |
| Amazon MSK (provisionado) | kafka.m7g.large $0.204/broker-h; kafka.t3.small $0.0456/broker-h; almacenamiento $0.10/GB-mes; sin capa gratuita | https://aws.amazon.com/msk/pricing/ (y el archivo de precios `msk.json` de AWS) |
| AWS EMR Serverless (x86) | $0.052624/vCPU-h + $0.0057785/GB-h | https://aws.amazon.com/emr/pricing/ |
| Google Serverless for Apache Spark | $0.06/DCU-h estándar (0.6 DCU por vCPU + 0.1 DCU por GiB) | https://cloud.google.com/products/managed-service-for-apache-spark/pricing |
| Databricks | **no verificado:** la página oficial carga los precios con JavaScript y no se pudo leer; no se usa | https://www.databricks.com/product/pricing/lakeflow-jobs |
| InfluxDB Cloud | Free: 5 MB de escritura cada 5 min, 10k series, 30 días, 2 buckets. Usage-Based: $0.0025/MB escrito, $0.012 por 100 consultas, $0.002/GB-h | https://docs.influxdata.com/influxdb/cloud/account-management/limits/ · https://www.influxdata.com/influxdb-pricing/ |
| Amazon Timestream for InfluxDB | db.influx.medium $0.120/h; almacenamiento $0.10/GB-mes | archivo de precios de AWS (`AmazonTimestream`, us-east-1) |
| Grafana Cloud | Free: 10k series activas, 3 usuarios activos, 14 días. Pro: desde $19/mes + uso | https://grafana.com/pricing/ |
| Firebase Realtime Database | Spark (gratis): 1 GB guardado, 10 GB/mes de descarga, 100 conexiones simultáneas. Blaze: $5/GB guardado sobre 1 GB, $1/GB descargado sobre ~10 GB/mes | https://firebase.google.com/pricing |
| GitHub Actions | Free: $12/mes de uso incluido; Linux 2 núcleos $0.006/min (~2,000 min) | https://docs.github.com/en/billing/concepts/product-billing/github-actions |

**Supuestos del cálculo** (marcados en el código):

- **Mes:** 730 h.
- **Kafka:**
  - retención de 7 días;
  - el job lee el tópico real dos veces (consultas generales y predicciones);
  - Confluent cobra la red de entrada y de las dos lecturas;
  - un eCKU por cada ~5,000 ev/s (supuesto);
  - en MSK, 3 brokers m7g.large con factor de replicación 3.
- **Spark administrado:**
  - corre 24/7, porque es streaming;
  - un driver de 2 vCPU más los núcleos que pide la sección 4, con el doble de holgura;
  - 4 GB por vCPU.
- **InfluxDB:** 20 bytes de line protocol por valor (supuesto); almacenamiento de un 10 % de lo escrito en el mes.
- **Grafana:** el equipo son 4 personas, así que se cuenta Pro ($19); Free admite 3 usuarios activos.
- **Firebase:** guarda 3 meses de historial.

**Tabla 4. Costo mensual por escenario** (USD/mes, `infra/costos_escenarios.py`)

<!-- COSTOS -->

| | Actual (1 ESP32 + 7 ThingSpeak + APIs) | 100 tanques | Juliaca conservador: 35,160 viviendas con red pública (INEI 2017) | Juliaca principal: 53,724 conexiones activas (SUNASS, oct-2023) | Juliaca principal, diseño a escala |
|---|---:|---:|---:|---:|---:|
| usd_confluent_basic | 0.04 | 3.24 | 1,237 | 1,938 | 144 |
| usd_msk | 149 | 448 | 918 | 1,166 | 507 |
| usd_influx | 0.00 | 0.00 | 13,793 | 21,076 | 1,228 |
| usd_grafana | 19 | 19 | 19 | 19 | 19 |
| usd_firebase | 0.00 | 131 | 51,021 | 77,967 | 0.00 |
| influx_free_alcanza | sí | sí | no | no | no |
| firebase_free_alcanza | sí | no | no | no | sí |

!!! warning "Pendiente de la prueba de carga limpia"
    El costo de Spark administrado (EMR Serverless y Google Serverless for Apache Spark) depende de los núcleos que fije la prueba de carga y se agrega con ella. Las filas de esta tabla no dependen de esa medición.


**Qué servicios gratuitos alcanzan hoy y cuándo dejan de alcanzar:**

| Servicio gratuito | Hoy | 100 tanques | Juliaca |
|---|---|---|---|
| Firebase Spark (1 GB, 10 GB/mes de descarga, 100 conexiones) | **alcanza:** 5.3 MB en 4 semanas | **no alcanza:** ~9 GB/mes; el GB gratis se llena en ~3 días y la descarga del puente roza el límite; 100 tanques son el tope de conexiones simultáneas | no: Firebase deja de ser la puerta de entrada (ver diseño a escala) |
| InfluxDB Cloud Free (5 MB/5 min, 10k series) | alcanza: 5.5 MB/día, 260 series | **alcanza justo:** ~0.5 GB/día (1.7 MB cada 5 min) y ~2,100 series | no: 700k-1.1M series y ~174-266 GB/día |
| Confluent Basic (primer eCKU gratis) | **alcanza:** pagaría solo red y disco, ~$0.04 | alcanza con ~$3 de red y disco | sí en eCKU, pero la red y el disco ya son ~$1,200-1,900 |
| Grafana Cloud Free | no por usuarios (4 > 3); sí por series | ídem | ídem |
| GitHub Actions (Free, $12) | **alcanza:** ~1,440 min/mes ≈ $8.6 si GitHub ejecutara las 48 diarias | igual: el colector no crece con los tanques | igual |
| Spark | no hay capa gratuita administrada: hoy corre en la PC del equipo | — | — |

## 4. Prueba de carga

<!-- PRUEBA -->

!!! warning "Pendiente de la prueba de carga limpia"
    La corrida limpia (25 a 1,000 ev/s, con producción y `lambda26-pyspark` detenidos y Docker con 12 GB) todavía no se hizo. Las corridas 1 y 2 quedaron como hallazgos: saturación temprana por la carga del YAML de prueba en cada worker de Python y un incidente de DNS de Docker bajo carga. Detalle en [prueba de carga, corridas 1 y 2](evidencias/f8/prueba_carga_corridas_1_y_2.md).


## 5. Criterios de escalado

<!-- CRITERIOS -->

!!! warning "Pendiente de la prueba de carga limpia"
    Los umbrales para agregar particiones, brokers o ejecutores salen de la prueba de carga limpia. Hasta entonces, la señal de saturación que se vigila es la **duración del lote frente a su trigger**, en el tablero "Salud del pipeline": en las corridas 1 y 2, `lagOffsets` quedó en 0 aunque las consultas de 10 s ya no daban abasto.

