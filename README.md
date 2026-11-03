# MonitorAqua

Observatorio domiciliario del servicio de agua de Juliaca — Proyecto Sello del Equipo Horeb (curso Big Data, UPeU).

**Qué hace el pipeline de la Unidad 2:**
1. Las lecturas del tanque del equipo (ESP32) y de APIs públicas (ThingSpeak, EA UK, USGS, Open-Meteo) entran por
   Firebase.
2. Pasan a Kafka y Spark Structured Streaming las procesa: ventanas, watermark, checkpoint, eventos de negocio e
   inferencia en vivo de 4 modelos de series de tiempo.
3. Los resultados se ven en Grafana, sobre InfluxDB.

**Documentación:** carpeta [docs/](docs/), publicada como sitio MkDocs (ver [Documentación](#documentación)).

## Arranque desde cero

### Requisitos

| Requisito | Versión probada | Notas |
|---|---|---|
| Docker Desktop (con Docker Compose v2) | 4.x, Compose v5.6 | **8 GB de memoria** para la VM de Docker. Con 7.5 GB, un segundo job de Spark muere por falta de memoria (`docs/costos-escalado.md`) |
| Git | 2.4x | — |
| Python | **3.12** | Para los scripts del host. En 3.15 PyYAML y mkdocs no instalan sin compilador de C++ |
| Puertos libres | 49092, 48085, 49308, 49090, 49091, 48086, 43000, 4043 | Kafka, Kafka UI, kafka-exporter, Prometheus, Pushgateway, InfluxDB, Grafana, Spark UI |
| Cuenta de Firebase Realtime Database | — | Usuario con correo y contraseña. Lo pide el puente; el colector de GitHub Actions usa los mismos datos como *secrets* |

### 1. Clonar y configurar

```bash
git clone https://github.com/ClaudioB12/MonitorAqua.git
cd MonitorAqua
cp .env.example .env        # completar los valores; .env nunca se versiona
```

**Variables de `.env` (guía en `.env.example`):**
- **Obligatorias:**
  - `FIREBASE_DB_URL`, `FIREBASE_API_KEY`, `FIREBASE_EMAIL`, `FIREBASE_PASSWORD`.
  - `INFLUX_ORG`, `INFLUX_BUCKET=monitoraqua`, `INFLUX_RETENTION=30d`, `INFLUX_ADMIN_USER`, `INFLUX_ADMIN_PASSWORD`.
  - `INFLUX_TOKEN`, un texto largo cualquiera:
    `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
  - `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`.
- **Ya traen valor:** `KAFKA_BOOTSTRAP=localhost:49092` y los tópicos.
- **Solo se aplican la primera vez,** cuando se crean los volúmenes: las credenciales de InfluxDB y Grafana.

### 2. Entorno de Python del host

```bash
python -m venv .venv
.venv/Scripts/python -m pip install -r infra/requirements.txt -r ingesta/requirements.txt -r batch/requirements.txt
# Linux/macOS: .venv/bin/python
```

### 3. Levantar el stack

```bash
docker compose -f infra/compose.yml --env-file .env up -d --build
```

**Qué levanta:** un solo compose (`infra/compose.yml`, proyecto `monitoraqua-infra`).

| Grupo | Servicios |
|---|---|
| Kafka | Kafka (KRaft, volumen persistente), creación de los 3 tópicos con 3 particiones, kafka-exporter, Kafka UI |
| Observabilidad | Prometheus, Pushgateway |
| Almacenamiento y tableros | InfluxDB (crea `monitoraqua` y `monitoraqua_aumentados`), Grafana con los tableros y alertas provisionados, renderizador |
| Ingesta y eventos | puente Firebase → Kafka, colector de APIs, consumidor de eventos de servicio, medidor de volumen |
| Streaming | job de Spark Structured Streaming con sus 7 consultas |

**Primer arranque:** construye dos imágenes (`monitoraqua-ingesta`, `monitoraqua-streaming`). Tarda ~10 min; el job de
Spark tarda ~1 min más en arrancar sus consultas.

### 4. Flota extrapolada (datos aumentados, opcional)

```bash
.venv/Scripts/python ingesta/generador.py --parquet --kafka       # 172,800 lecturas en aqua-eventos-aumentados
.venv/Scripts/python streaming/cargar_historico_influx.py --solo aumentado
```

Con esto, el tablero BI tiene datos con `origenLecturas = aumentado` y la inferencia en vivo procesa la flota.

### 5. Verificar

```bash
.venv/Scripts/python infra/verificar_stack.py      # sale con código 0 si todo está OK
.venv/Scripts/python infra/verificar_tableros.py   # ejecuta cada consulta de cada panel
```

**`verificar_stack.py` chequea:**
- Kafka y sus tópicos, con las particiones esperadas;
- que la ingesta esté viva: último mensaje de hace menos de 120 s;
- InfluxDB y su bucket;
- el progreso de las consultas de Spark;
- Grafana y sus datasources;
- los targets de Prometheus y el exporter.

**Ingesta viva sin el ESP32:** el colector de APIs basta, porque publica cada pocos minutos.

### Dónde mirar

| Qué | Dirección |
|---|---|
| Grafana (tableros "Salud del servicio de agua – Juliaca", "Salud del pipeline", "Prueba de carga (F8)") | http://localhost:43000 |
| Kafka UI | http://localhost:48085 |
| Spark UI del job | http://localhost:4043 |
| Prometheus | http://localhost:49090 |

### Detener y volver a arrancar

```bash
docker compose -f infra/compose.yml --env-file .env stop     # detiene sin borrar nada
docker compose -f infra/compose.yml --env-file .env start
```

**Nunca `down -v`:** borra los volúmenes (Kafka, checkpoints de Spark, InfluxDB, Grafana).
- **Datos que no se pierden:** Firebase y el bronce Parquet guardan todas las lecturas.
- **Lo que sí se pierde:** los checkpoints. Spark reprocesaría desde el inicio de lo retenido en Kafka.

**Arranque diario con la PC apagada de noche:** `docs/arranque-diario.md`.

## Estructura del repositorio

| Carpeta | Contenido |
|---|---|
| `contratos/` | JSON Schema del evento de lectura (`evento_aqua`) y del evento de servicio |
| `ingesta/` | puente Firebase → Kafka, colector de APIs, generador de la flota, consumidor de eventos, configuración (`estaciones.yml`, `tanques.yml`) |
| `streaming/` | job de Spark Structured Streaming, detector de eventos de servicio (reglas congeladas f4.4), inferencia en vivo (F7) |
| `batch/` | capas bronce → plata → oro, partición, modelos de D2, D4, D6 y D8 y sus parámetros versionados (`batch/modelos/`) |
| `dashboards/` | tableros de Grafana (JSON generado por `generar_tableros.py`) |
| `infra/` | `compose.yml` del stack, provisioning de Grafana, `prometheus/`, scripts de verificación y la prueba de carga |
| `firmware/esp32/` | firmware del tanque (`secrets.h` no se versiona) |
| `docs/` | sitio MkDocs y evidencias por fase (`docs/evidencias/`) |
| `pyspark/`, `data/` | trabajo de la Unidad 1 |

## Documentación

```bash
.venv/Scripts/python -m pip install mkdocs-material
.venv/Scripts/mkdocs serve          # http://127.0.0.1:8000
.venv/Scripts/mkdocs build          # genera site/
```

**Publicación con GitHub Pages:**
1. **Requisito:** el repositorio es privado y su dueño tiene GitHub Free, así que Pages exige que sea público. Con
   GitHub Pro sí publica desde un repo privado, aunque el sitio queda visible para cualquiera.
   - **GitHub Pro gratis:** viene con el GitHub Student Developer Pack (https://education.github.com/pack).
2. **Construir y subir:** `.venv/Scripts/mkdocs gh-deploy --force`. Construye el sitio y lo sube a la rama `gh-pages`.
3. **Activar:** Settings → Pages → *Deploy from a branch* → `gh-pages` / `(root)`.
4. **Dirección:** queda en `site_url` de `mkdocs.yml`, https://claudiob12.github.io/MonitorAqua/.
