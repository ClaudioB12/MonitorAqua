# Arranque diario

Checklist para cuando se enciende la PC después de tenerla apagada (por ejemplo, de noche).

Con la PC apagada siguen funcionando el **ESP32**, que escribe directo a Firebase por Wi-Fi, y el **colector de
GitHub Actions**, cada 30 min (`docs/ingesta.md` → *Colector en la nube*). Todo lo demás está detenido: Kafka, el
puente, Spark, InfluxDB y Grafana. Al encender, el puente pasa a Kafka lo acumulado en Firebase y Spark lo procesa.

## Referencia del último apagado

Se actualiza en cada apagado; sirve para comprobar que todo retoma donde quedó.

| Dato | Valor (2026-10-29, ~01:45 Lima) |
|---|---|
| Offsets de `aqua-eventos-real` al apagar | `0:8906, 1:705, 2:163` (01:42) |
| Última lectura del ESP32 en Firebase | **01:13:49**: el ESP32 dejó de escribir ~30 min antes del apagado (corte de energía o de Wi-Fi; revisarlo) |
| `batchId` de Spark al apagar | `esp32_detalle` 1006 · `eventos_servicio` 810 · `rapidas_agg` 359 · `lentas_agg` 185 · `bronce` 185 · `latencias` 148 |

`id` de cada consulta de Spark. Es fijo mientras se conserve su checkpoint: si cambia, la consulta arrancó de cero.

| Consulta | `id` |
|---|---|
| `rapidas_agg` | `90e9da13-eeb7-44a7-ae65-07a0745766f3` |
| `lentas_agg` | `eb55e2eb-00e8-4f92-8914-14b3f202cd17` |
| `esp32_detalle` | `7895a005-5882-4b83-871f-49d244a37fa7` |
| `bronce` | `67dac576-f1ec-44c1-93b3-f8d58c84a326` |
| `latencias` | `a1c28d2b-d513-4f98-962c-a765922c0e9b` |
| `eventos_servicio` | `beb70aef-fe65-4024-888a-8aac219c89f1` |

## Qué se enciende solo

Docker Desktop está en el arranque de Windows (clave `Run`), aunque su archivo de ajustes dice `AutoStart: false`.
**Si no se abre solo, ábrelo a mano.** Al iniciar Docker, cada contenedor sigue su política de reinicio:

| Contenedor | Proyecto compose | Política | ¿Se enciende solo? |
|---|---|---|---|
| `lambda26-kafka`, `lambda26-kafka-ui`, `lambda26-kafka-exporter` | `lambda26-kafka` (lambda26) | `unless-stopped` | Sí |
| `lambda26-aqua-puente` | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-aqua-colector` (colector local) | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-aqua-streaming` (Spark) | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-aqua-consumidor-eventos` | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-aqua-volumen` | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-influxdb` | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-grafana`, `lambda26-grafana-renderer` | `monitoraqua-infra` | `unless-stopped` | Sí |
| `lambda26-prometheus` | `lambda26-obs` (lambda26) | `unless-stopped` | Sí |
| `lambda26-pyspark` (Jupyter, U1) | `lambda26-pyspark` (lambda26) | `unless-stopped` | Sí |
| `lambda26-aqua-bridge` (puente viejo de S08) | `lambda26-aqua-bridge` (lambda26) | `no` | No, y así debe quedar |
| `lambda26-aqua-consumer` (consumidor de S09) | `lambda26-aqua-bridge` (lambda26) | `unless-stopped`, detenido a mano | No: `unless-stopped` respeta la detención manual |
| `lambda26-uso-campo-electrico` | otro curso | `no` | No |

`unless-stopped` vuelve a encender el contenedor al iniciar Docker, **salvo que se haya detenido a mano** con
`docker stop`. Si detienes uno a propósito, no vuelve hasta un `docker start`.

> **Nunca `docker compose down` en el proyecto de Kafka.** Los datos de Kafka viven en un volumen anónimo:
> `down` borra el contenedor y el siguiente `up` crea uno nuevo con un volumen vacío. Se pierden los tópicos y los
> offsets, y con ellos el checkpoint de Spark. Para reiniciar: `docker restart lambda26-kafka`, o
> `docker compose stop` / `start`.

## Checklist

Comandos desde la raíz del repo, en Git Bash. `docker exec` y `docker run` con rutas necesitan `MSYS_NO_PATHCONV=1`.

### 1. El stack está arriba

```bash
docker ps --format '{{.Names}}\t{{.Status}}' | sort
.venv/Scripts/python infra/verificar_stack.py
```

- [ ] Los contenedores marcados "Sí" en la tabla están `Up`. Espera 1-2 min a que Kafka termine de arrancar.
- [ ] `verificar_stack.py` sale con código 0: Kafka, tópicos, Prometheus, InfluxDB y Grafana en OK.

### 2. El puente se pone al día desde Firebase, sin duplicados

```bash
docker logs --since 30m lambda26-aqua-puente 2>&1 | grep -E '"status": "(starting|streaming|reconnecting|error)"'
docker logs --since 30m lambda26-aqua-puente 2>&1 | grep -c '"status": "published"'
MSYS_NO_PATHCONV=1 docker run --rm --network lambda26-kafka-net --env-file .env -e KAFKA_BOOTSTRAP=kafka:9092 \
  monitoraqua-ingesta:latest python auditar_ingesta.py --desde 0:8906,1:705,2:163
```

- [ ] El puente arrancó **desde su estado guardado**, no con `sin_estado`, y llegó a `streaming`. Lee desde su
      cursor menos 3 h hasta el presente: toda la noche.
- [ ] `published` creció en cientos o miles: es lo acumulado de la noche.
- [ ] `auditar_ingesta.py` (offsets de la referencia de arriba): **0 `eventId` repetidos** y **0 lecturas del ESP32
      en Firebase que no llegaron a Kafka**. Si las repeticiones no son 0, revisa si coinciden con las 54 conocidas
      de S08 o si son nuevas.

### 3. Spark retoma desde su checkpoint

```bash
docker logs --since 30m lambda26-aqua-streaming 2>&1 | grep -E '"status": "(started|query_started|query_terminated)"'
docker logs --since 10m lambda26-aqua-streaming 2>&1 | grep '"status": "progress"' | tail -6
ls data/local/bronce/origen=esp32-fisico/
```

- [ ] Las 6 consultas aparecen en `query_started` con **el mismo `id`** de la tabla de referencia. Un `id` nuevo
      significa que esa consulta perdió su checkpoint y arrancó de cero.
- [ ] El `batchId` de cada consulta es **mayor** que el del apagado, no 0.
- [ ] No hay ningún `query_terminated`.
- [ ] El bronce tiene la carpeta `fecha=` del día con lecturas de la noche.
- [ ] **Aviso:** lo nocturno llega con horas de atraso, más que el watermark de 7 min. Los agregados por ventana
      (`rapidas_agg`, `lentas_agg`) descartan esas lecturas tardías, así que **el tablero en vivo no muestra la
      noche**. El bronce sí las guarda, y la capa batch (`batch/capas.py`) las recupera. Es lo esperado en la
      arquitectura Lambda, no una pérdida.

### 4. Las ejecuciones nocturnas de GitHub Actions

GitHub → MonitorAqua → **Actions** → `colector-apis` → filtro `event:schedule`.

- [ ] Hay ejecuciones de la noche (minutos 7 y 37), en verde. **GitHub no garantiza el cron:** la primera noche no
      disparó ninguna entre 02:07 y 08:07 (ver `docs/ingesta.md`).
- [ ] Si falta alguna o llegó atrasada, no hay problema mientras dos ejecuciones verdes seguidas estén a menos de 6 h
      (la ventana de cada pasada). Si hay un salto de más de 6 h, ese tramo de las APIs quedó sin cubrir: se recupera
      con una recarga como la del 2026-10-29 (`docs/ingesta.md` → *Recarga puntual*), antes de que el puente se
      reconecte.
- [ ] Si alguna está en rojo, abre su log:
  - `login rechazado` → revisa los secrets.
  - `fuente_error` de una sola fuente → esa API estuvo caída; la siguiente ventana la cubre.

### 5. Huecos nocturnos en Firebase, por origen (incluido el ESP32)

```bash
MSYS_NO_PATHCONV=1 docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/ingesta monitoraqua-ingesta:latest \
  python auditar_huecos.py --desde 2026-10-29T01:45
```

`--desde` es la hora del apagado (Lima). Solo lee: lista cada sensor con sus huecos y un resumen por origen.

- [ ] Aparecen los 5 orígenes: `esp32-fisico`, `thingspeak`, `ea-uk`, `usgs` y `open-meteo`. Si falta uno, no escribió
      nada en toda la noche.
- [ ] **ESP32:** sin huecos de más de 2 min si estuvo encendido toda la noche. Cada hueco es un corte de energía o de
      Wi-Fi: anótalo con su hora. En este apagado el ESP32 ya no escribía desde las 01:13.
- [ ] **APIs (ThingSpeak, EA UK, Open-Meteo):** sin huecos. Si hay alguno, cruza con el paso 4: ¿hubo más de 6 h sin
      ejecución verde?
- [ ] **USGS:** su último tramo siempre aparece como hueco final de ~60-75 min, porque USGS publica con ~1 h de atraso.
      No es una falla; solo cuentan los huecos internos.

### 6. La latencia del ESP32 sube tras el apagado y no es una falla

- [ ] En Grafana (*Salud del pipeline*), el **p95 de latencia de ingesta del ESP32 sube a horas** después de
      encender. Es la recuperación: cada lectura nocturna se mide desde su `timestamp` (cuando el ESP32 midió) hasta
      que llegó a Kafka (al encender la PC). Baja sola a segundos cuando la ventana del panel deja atrás la
      recuperación (1 h o 24 h, según el panel).
- [ ] Alertas que pueden dispararse justo después de encender, sin que haya falla:
  - "Latencia p95 de una fuente rápida sobre el watermark (420 s)", por la recuperación.
  - "Lote de Spark más largo que su trigger", por el primer lote, que trae toda la noche.

  Deben resolverse solas en minutos.
- [ ] "ESP32 sin datos por más de 5 min" **sí es real**: indica que el ESP32 no está escribiendo ahora. Revisa
      su energía y su Wi-Fi.
