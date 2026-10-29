# Ingesta unificada (F2)

Todas las fuentes, el ESP32 físico y cuatro APIs públicas reales, escriben en **una sola entrada**: Firebase
`/board1/lecturas/historial`. Un único puente publica de ahí al tópico Kafka **`aqua-eventos-real`**, cumpliendo el
contrato `contratos/evento_aqua.schema.json`. Cubre el criterio 2 de la rúbrica (ingesta IoT/sensores).

```text
ESP32 físico (cada 5 s, firmware sin cambios) ──┐
ThingSpeak  (7 canales, cada 5 min) ────────────┤
EA UK       (23 estaciones, cada 15 min) ───────┼─> Firebase /board1/lecturas/historial
USGS        (10 estaciones, cada 15 min) ───────┤          │  (índice .indexOn: "timestamp")
Open-Meteo  (Juliaca, cada 15 min) ─────────────┘          v
             lambda26-aqua-colector        lambda26-aqua-puente ─> Kafka aqua-eventos-real (clave sensorId)
```

## Puente Firebase → Kafka

| | |
|---|---|
| Código | `ingesta/puente_firebase_kafka.py` (+ `ingesta/comun.py`) |
| Contenedor | `lambda26-aqua-puente`, servicio `aqua-puente` de `infra/compose.yml`, imagen `monitoraqua-ingesta` |
| Ejecución | `docker compose -f infra/compose.yml --env-file .env up -d aqua-puente` |
| Autenticación | Firebase Auth Email/Password por REST, con un usuario dedicado al pipeline (`FIREBASE_EMAIL` / `FIREBASE_PASSWORD` en `.env`). El idToken se renueva 10 min antes de vencer y el stream se reabre con el token nuevo. Nunca se registra la URL con `?auth=` |
| Lee | Solo `FIREBASE_PATH=/board1/lecturas/historial`; si la ruta es otra, el puente no arranca. `/board1/lecturas/ultima`, `/board1/sensor` y `/board1/outputs` no se leen |
| Publica | `aqua-eventos-real` con clave `sensorId`, `acks=all` |

Reemplaza al puente de la S08 (`lambda26/aqua-bridge`, contenedor `lambda26-aqua-bridge`), que queda **detenido y con
`restart=no`**, sin borrar. Nunca deben correr los dos a la vez.

**Qué hace con cada lectura:**

1. **Completa el contrato.** El ESP32 escribe solo `sensorId`, `distanciaCm`, `temperaturaC`, `humedadPct` y `timestamp`;
   el puente agrega `eventId` (la push key de Firebase), `origen=esp32-fisico`, `fuenteTipo=iot-propio`,
   `tipoEvento=aqua.tanque.lectura` e `ingestaTs` (ahora). Las lecturas del colector ya traen el contrato completo; solo
   se asegura `eventId` = clave y se completan `fuenteTipo` e `ingestaTs` si faltan.
2. **Conserva `distanciaCm = -1`** (fallo del sensor) tal cual. La regla de calidad vive en Spark (F3). El puente de la
   S08 lo convertía en `null`; este ya no.
3. **Valida** contra el schema. Si no cumple, registra `status=rejected` con los errores y **no publica**.
4. **Publica** con clave `sensorId`.

**Reanudación sin republicar el historial.** Las push keys del ESP32 (`-P53...`) y las claves del colector
(`usgs_...`) no ordenan en el tiempo entre sí, así que el cursor es el campo `timestamp`. Esto exige
`".indexOn": "timestamp"` en las reglas de `/board1/lecturas/historial` (ya publicado). El estado vive en el volumen
`monitoraqua_ingesta_estado` (`/estado/puente.json`):

- **`cursor`:** el mayor `timestamp` publicado. El stream se abre desde `cursor - VENTANA_H` (3 h), porque las APIs
  publican con atraso: EA ~45 min, USGS <1 h.
- **`vistas`:** las claves ya publicadas dentro de la ventana. Lo re-leído al reconectar se salta.
- **`piso`:** nada con `timestamp` anterior se publica. Al arrancar **sin estado** vale "ahora", así que solo pasan
  lecturas nuevas.

**Backfill explícito:** `python puente_firebase_kafka.py --backfill-horas H [--once]` baja el piso y publica las
lecturas de las últimas H horas que no se hayan publicado.

**Reconexión.** Si Firebase o Kafka se caen (error HTTP, stream cancelado, broker no disponible, timeout de envío), el
puente registra `status=error` y reintenta con espera creciente de 2 a 60 s. Si el error es de Kafka, recrea el
productor. La lectura que no se pudo publicar no quedó en `vistas`, así que vuelve a llegar en el siguiente stream.

**Garantía:** al-menos-una-vez. Si el proceso muere entre publicar y guardar el estado (cada 5 s), una lectura puede
repetirse; Spark deduplica por `eventId` (F3).

## Colector de APIs → Firebase

| | |
|---|---|
| Código | `ingesta/colector_apis.py` + configuración `ingesta/estaciones.yml` |
| Contenedor | `lambda26-aqua-colector`, servicio `aqua-colector` de `infra/compose.yml` |
| Prueba sin escribir | `docker run --rm --env-file .env monitoraqua-ingesta:latest python colector_apis.py --seco --una-vez [--fuente usgs]` |

Cada lectura se convierte al contrato y se valida antes de escribir; si no cumple, se registra y no se escribe. Se
escribe con **PUT** en `historial/<origen>_<estación>_<timestamp>`, con `print=silent` para que Firebase no devuelva el
cuerpo y no consuma descarga. Una misma lectura consultada dos veces cae en la misma clave y no se duplica; si el dato
es idéntico, Firebase ni siquiera emite evento.

- **Fuentes aisladas:** cada fuente corre en su propio hilo y con su propio intervalo; si una API falla, se registra
  `fuente_error` y las demás siguen.
- **Lecturas antiguas:** no se escriben las de más de `VENTANA_H` (3 h), porque el puente ya no las vería.
- **Unidades:** se convierte cuando la fuente no viene en °C, m o m³/s: °F→°C, ft³/s→m³/s, ft→m, y escalas por canal
  (cm→m). Hoy USGS entrega °C y mg/L, EA m y m³/s, y Open-Meteo °C.

### Estaciones elegidas (verificadas el 2026-10-28)

**ThingSpeak:** 212 canales candidatos (búsqueda por *water level*, *water tank*, *tank level*, *ultrasonic*, etc.),
22 con lectura de menos de 24 h. Se eligieron los 7 que miden nivel de tanque:

| Canal | Nombre | Medidas del contrato | Actualiza |
|---|---|---|---|
| 283977 | House Water Tank Reporting | `nivelPct` | ~12/h |
| 480876 | Water Tank Levels & Temp/Humidity | `nivelPct`, `temperaturaC`, `humedadPct` | ~60/h |
| 1233810 | House Water Tank (24k L) | `nivelPct` | ~6/h |
| 1233815 | Veggie Water Tank (24k L) | `nivelPct` | ~3/h |
| 1272968 | WaterLevelMonitor | `distanciaCm` | ~60/h |
| 3418755 | Rain water harvesting monitoring | `nivelM` (cm × 0.01), `tempAguaC` | ~12/h |
| 3019316 | Nivel Cárcamo | `distanciaCm` | ~4/h |

Canales activos descartados:

- **No son tanques:**
  - Napkollektor (718576, colector solar)
  - AquaControl32-4R (2421172, acuario)
  - Trout Pond (24768)
  - Metering (500688, energía)
  - Well level (1127756, pozo)
  - HydroSense (3527530, hidroponía)
  - Water Tank 61103 (unidades de rango no documentadas)
  - Docklands London (521315, temperatura de río)
  - Bayfront Canal (1172149, canal)
  - FHWS Volume (812321, unidades no documentadas)
  - Project ALIGN (3185316)
  - CO2 + AIR (159156)
- **Datos inválidos:** Main Water Tank (668025), con "Percent Full" = -37.83.
- **Más de 3 h sin publicar:** Water tank (1483281, 9.9 h) y plzenskaAggregated (765201, 17 h, en ceros).

**Environment Agency UK:** las 23 estaciones activas de la cuenca **Thames from Hurley to Teddington**, una sola cuenca
para que sean comparables. `nivelM` = *stage* aguas arriba (mASD; Trowlock Island y Thames Ditton Island en mAOD).
`caudalM3s` lo dan las 6 estaciones de aforo.

| Ref | Estación | Río | Caudal |
|---|---|---|---|
| 2497TH | Hurley Lock | River Thames | |
| 2499TH | Marlow Lock | River Thames | |
| 2601TH | Cookham Lock | River Thames | |
| 2602TH | Boulters Lock | River Thames | |
| 2603TH | Taplow | Jubilee River | ✓ |
| 2604TH | Maidenhead | River Thames | ✓ |
| 2607TH | Bray Lock | River Thames | |
| 2655TH | Raymill Road | Maidenhead Ditch | |
| 2698TH | Romney Lock | River Thames | |
| 2700TH | Windsor Park | River Thames | ✓ |
| 2702TH | Bell Weir | River Thames | |
| 2703TH | Old Windsor Lock | River Thames | |
| 2900TH | Staines | River Thames | ✓ |
| 2901TH | Penton Hook | River Thames | |
| 2902TH | Chertsey Lock | River Thames | |
| 2904TH | Shepperton Lock | River Thames | |
| 3100TH | Walton | River Thames | ✓ |
| 3101TH | Sunbury Lock | River Thames | |
| 3102TH | Molesey Lock | River Thames | |
| 3104TH | Thames Ditton Island | River Thames | |
| 3400TH | Kingston | River Thames | ✓ |
| 3401TH | Teddington Lock | River Thames | |
| 3404TH | Trowlock Island | River Thames | |

Excluidas: Ray Farm (SU89_82, agua subterránea, 14 h sin datos) y una estación de la cuenca que responde 404.

**USGS** (`api.waterdata.usgs.gov/ogcapi/v1`, colección `latest-continuous`, una sola consulta para las 10): 475
estaciones de EE. UU. tenían los tres parámetros con lectura de menos de 3 h. Se eligieron 10 de la **cuenca del río
Delaware**. Todas miden temperatura del agua (00010, °C → `tempAguaC`), pH (00400 → `ph`) y oxígeno disuelto (00300,
mg/L → `oxigenoMgL`). La API devuelve también series viejas (2020, 2025) y valores `null`: el colector se queda con la
lectura más reciente con valor de cada parámetro.

| ID | Estación |
|---|---|
| USGS-01433500 | Mongaup River near Mongaup NY |
| USGS-01438500 | Delaware River at Montague NJ |
| USGS-01454700 | Lehigh River at Glendon PA |
| USGS-01463500 | Delaware River at Trenton NJ |
| USGS-01465798 | Poquessing Creek at Philadelphia PA |
| USGS-01467200 | Delaware River at Penn's Landing, Philadelphia PA |
| USGS-01473500 | Schuylkill River at Norristown PA |
| USGS-01474000 | Wissahickon Creek at Mouth, Philadelphia PA |
| USGS-01474500 | Schuylkill River at Philadelphia PA |
| USGS-01481500 | Brandywine Creek at Wilmington DE |

**Open-Meteo:** Juliaca (lat -15.49, lon -70.13), `temperature_2m` → `temperaturaC` y `relative_humidity_2m` →
`humedadPct`.

## Colector en la nube (GitHub Actions)

El contenedor `lambda26-aqua-colector` solo corre con la PC encendida y pide **la última** lectura de cada
estación. Con la PC apagada, esas horas de EA UK y USGS se perdían. El workflow
`.github/workflows/colector_apis.yml` corre el mismo colector en GitHub cada 30 min, en **modo ventana**:

```bash
EDAD_MAX_H=7 python colector_apis.py --una-vez --ventana-min 360
```

| Fuente | Cómo pide la ventana |
|---|---|
| ThingSpeak | `feeds.json?minutes=360` |
| EA UK | `readings?since=<hace 6 h>`; las 23 estaciones en paralelo (8 hilos), porque la API tarda ~4 s por estación |
| USGS | colección `continuous` con `time=<desde>/<hasta>` (en vivo se usa `latest-continuous`) |
| Open-Meteo | `minutely_15` con `past_minutely_15=24`: los mismos instantes de 15 min que `current` |

- **GitHub no garantiza el cron.** Puede atrasar una ejecución o no dispararla. Ver la tabla de abajo.
- **La protección es la ventana de 6 h.** Cada pasada pide las últimas 6 h, doce veces el intervalo del cron. Basta
  una pasada verde cada menos de 6 h para que no quede ningún hueco: una pasada tardía rellena lo que dejaron las
  que faltaron.
- **Sin duplicados:** la clave determinística `origen_estacion_timestamp` hace que las lecturas que se repiten entre
  pasadas se sobrescriban en Firebase en vez de agregarse.
- **Escritura:** un `PATCH` multi-ruta por fuente sobre `/board1/lecturas/historial`. Tiene el mismo efecto que un
  PUT por clave y es una sola petición. Verificado contra las reglas de Firebase el 2026-10-29.
- **Edad máxima:** el colector descarta lecturas más viejas que `EDAD_MAX_H`. Si no se define, vale `VENTANA_H`
  (3 h). El workflow la sube a 7 h, por encima de la ventana, y el colector rechaza una ventana mayor que la edad
  máxima.
- **Con la PC apagada:** las lecturas quedan en Firebase. Al encender, el puente lee desde su cursor menos 3 h hasta
  el presente y pasa a Kafka todo lo acumulado: se recupera la noche entera.
- **Con la PC encendida:** el puente escucha en vivo las lecturas con `timestamp` posterior a su cursor menos 3 h.
  Puede pasar que GitHub se salte más de 3 h y una pasada tardía escriba lecturas nuevas de hace 3-6 h. Esas
  lecturas quedan **solo en Firebase**: no llegan a Kafka ni al bronce.
  - ThingSpeak y Open-Meteo no se pierden para los modelos: la capa batch las baja directo de sus APIs
    (`batch/descargar_fuentes.py`).
  - EA UK y USGS no alimentan ninguna dimensión.
  - Si hiciera falta, se recuperan con una recarga como la del 2026-10-29 (abajo).
- **Los dos colectores a la vez:** con la PC encendida corren el local (`lambda26-aqua-colector`) y el de GitHub. No
  hay duplicados: los dos escriben la misma clave fija, y el puente publica cada clave una sola vez en Kafka
  (`vistas`). El local da frescura de 5 min para la demo y el de GitHub cubre las horas con la PC apagada.
- **Fallas:** si una fuente falla, la ejecución sale con código 1 y GitHub avisa por correo. Una estación suelta
  caída solo queda en el log, porque la próxima ventana la cubre.
- **Medición** (2026-10-29, en seco):

  | Ventana | Lecturas | Duración | Detalle |
  |---|---|---|---|
  | 2 h | 537 | ~20 s | ThingSpeak 2 s, USGS 2 s, Open-Meteo 1 s, EA UK 20 s |
  | 6 h | 1,739 | 25 s | EA UK 24 s |

  0 rechazadas en ambas.

### Lo que hizo el cron la primera noche (2026-10-29)

| Hora (Lima) | Ejecución | Qué se vio en Firebase |
|---|---|---|
| ~01:31 y ~01:36 | #1 manual y su re-run | Pasadas de 515 y 538 lecturas (evidencia en `docs/evidencias/colector-nube/`) |
| 02:07 a 08:07 | Ninguna | **GitHub no disparó el cron.** No hay lecturas de APIs entre las 02:00 y las 06:30 |
| 08:40 | #2, la **primera programada** ("Triggered via schedule", 43 s, verde): la de las 08:37, 3 min tarde | 543 lecturas, con la ventana de 2 h de entonces |
| 09:07 y 09:37 | No aparecen en Firebase a las 10:04 | El cron todavía no dispara con regularidad |

GitHub suele tardar en activar el cron de un workflow recién agregado, y en horas de carga atrasa o descarta
ejecuciones. Por eso la ventana pasó de 2 h a 6 h el mismo día. Con 2 h, cualquier racha de más de 2 h sin
ejecuciones dejaba un hueco.

### Recarga puntual del hueco nocturno (2026-10-29)

El hueco de las APIs de esa noche (01:48 → 09:15) se recargó a mano con el colector en modo ventana, filtrado a ese
rango y **solo con las claves que todavía no existían en Firebase**. Las que ya existían pueden estar publicadas en
Kafka y el puente las repetiría, porque su memoria de claves (`vistas`) solo cubre 3 h. El script y su salida están
en `docs/evidencias/colector-nube/`.

| | ThingSpeak | EA UK | USGS | Open-Meteo |
|---|---|---|---|---|
| Lecturas nuevas escritas en Firebase | 871 | 513 | 236 | 21 |
| Publicadas por el puente en Kafka | 871 | 513 | 236 | 21 |
| En el bronce, 01:48-06:30 (únicas) | 748 | 414 | 187 | 18 |

- **Sin duplicados:** `auditar_ingesta.py` dio 0 `eventId` repetidos y 0 lecturas en Firebase sin su copia en Kafka.
- **Por qué el puente las vio:** su conexión de esa mañana había arrancado con `desdeTs` = el cursor del apagado
  menos 3 h. Por eso escuchaba todo lo de la noche. Una recarga hecha después de su siguiente reconexión (cada
  ~50 min) solo quedaría en Firebase.
- **Fuera del watermark:** estas lecturas llegaron con horas de atraso, más que el watermark de las consultas
  agregadas (7 min las rápidas y 90 min las lentas). Por eso **no aparecen en los agregados en vivo de Grafana**
  (`lecturas_agg`) ni generan eventos de servicio, porque el detector descarta lo que llega fuera de orden. Sí
  quedan en el **bronce**, que guarda todo sin watermark, y de ahí en la **capa batch** (`batch/capas.py`). Es el
  reparto de trabajo de la arquitectura Lambda: la capa rápida prioriza lo reciente y la batch completa la historia.

### Configuración (una sola vez, en GitHub → Settings → Secrets and variables → Actions)

1. **Secrets:** `FIREBASE_API_KEY`, `FIREBASE_DB_URL`, `FIREBASE_EMAIL` y `FIREBASE_PASSWORD`, con los mismos valores
   de `.env`. `USGS_API_KEY` es opcional. Nunca se escriben en el repo.
2. **Variable:** `COLECTOR_REF` = `u2-colector-nube`, la rama con este código. **Es temporal:** existe solo porque
   `main` todavía no tiene las fases U2. Cuando las ramas U2 se fusionen en `main`, la variable **se elimina** y el
   workflow pasa a usar el código de `main`.
3. **El workflow tiene que estar en `main`:** GitHub solo dispara los `schedule` desde la rama por defecto. Cambiar
   la ventana o el cron exige un commit en `main`.
4. **Prueba:** Actions → colector-apis → *Run workflow*.

### Costo

- **Actions:** el repo es privado y el plan Free incluye 2,000 min/mes. Cada ejecución tarda ~45 s y se factura
  1 min. A 48 ejecuciones por día son ~1,440 min/mes.
- **Si los minutos no alcanzan,** el cron pasa a una vez por hora (`"7 * * * *"`), unos 720 min/mes, **sin perder
  datos**. La ventana de 6 h cubre seis horas de ejecuciones perdidas. Solo cambia la frescura: las lecturas llegan
  a Firebase hasta con una hora de atraso.
- **Firebase:** las reescrituras no aumentan el almacenamiento (misma clave). Con la PC encendida, cada PATCH de
  6 h (~650 KB) llega por el stream del puente: ~31 MB/día de descarga extra, ~0.9 GB/mes, dentro de los 10 GB/mes
  del plan.

## Volumen frente al plan gratuito de Firebase (Spark)

| Fuente | Lecturas nuevas/día | Bytes/lectura | Almacenamiento/día |
|---|---|---|---|
| ThingSpeak (7, máx. 1 cada 5 min) | ~1,460 | 370 | 0.54 MB |
| EA UK (23 × 96) | 2,208 | 370 | 0.82 MB |
| USGS (10 × 96) | 960 | 370 | 0.36 MB |
| Open-Meteo (1 × 96) | 96 | 330 | 0.03 MB |
| **Colector** | **~4,720** | | **~1.75 MB** |
| ESP32 (cada 5 s) | 17,280 | 120 | 2.07 MB |
| **Total** | **~22,000** | | **~3.8 MB** |

| Límite del plan Spark (RTDB) | Uso estimado | % |
|---|---|---|
| Escrituras: sin cuota diaria; tope 1,000/s | 0.25/s | 0.03 % |
| Almacenamiento: 1 GB | 3.8 MB/día (el ESP32 aporta 55 %) | 1 GB en ~260 días |
| Descarga: 10 GB/mes | ~17.5 MB/día ≈ 0.53 GB/mes | ~5 % |
| Conexiones simultáneas: 100 | 1 stream + REST cortas | ~2 % |

La descarga se reparte así:

- **Stream del puente:** recibe cada lectura una vez, 3.8 MB/día.
- **Relectura al reconectar:** cada ~50 min el puente vuelve a leer la ventana de 3 h (0.47 MB), ~13.7 MB/día.
- **Escrituras del colector:** con `print=silent` no devuelven nada.

## Verificación (2026-10-28, 22:35–22:53 UTC)

La auditoría con `ingesta/auditar_ingesta.py` lee todo lo publicado por el puente nuevo (desde el offset 2649 de la
partición 0) y lo compara con Firebase:

| Origen | Mensajes | Sensores |
|---|---|---|
| esp32-fisico | 1,523 | 1 |
| ea-uk | 23 | 23 |
| thingspeak | 21 | 7 |
| usgs | 11 | 10 |
| open-meteo | 1 | 1 |

- **Rechazados por el schema:** 0 en el puente, 0 en el colector. Tampoco hubo errores de fuente, de escritura ni de
  conexión.
- **`eventId` repetidos:** 0, aunque el puente y el colector se reiniciaron a mitad de la prueba. Al reiniciar, el
  colector reescribió en Firebase las mismas claves de EA, USGS y ThingSpeak, y no volvieron a Kafka.
- **ESP32 una sola vez:** 1,523 lecturas en Firebase en el rango auditado y 1,523 `eventId` únicos en Kafka; ninguna
  falta y ninguna sobra. 25 traen `distanciaCm = -1` y pasaron tal cual.
- **Clave Kafka = `sensorId`:** 1,579/1,579. Con 42 sensores, las particiones ya se reparten (0: 1544, 1: 26, 2: 9).
- **Un incidente durante la prueba:** el primer arranque del puente, sin estado, publicó las 3 h previas del ESP32
  (1,319 lecturas). No son duplicados (Kafka no tenía nada después del 23-oct), pero no eran "nuevas". Se corrigió con
  el `piso`, y un arranque sin estado contra un tópico temporal publicó solo las lecturas posteriores al arranque.

```bash
docker run --rm --network lambda26-kafka-net --env-file .env -e KAFKA_BOOTSTRAP=kafka:9092 \
  monitoraqua-ingesta:latest python auditar_ingesta.py --desde 0:2649,1:0,2:0
```
