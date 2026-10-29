# Dataset para los modelos (F6)

Esta fase prepara los datos y el marco de evaluación **comunes** a los cuatro modelos (D2, D4, D6 y D8). Todavía no
se entrena ningún modelo de dimensión: cada uno parte de estas tablas, de la misma partición y del mismo marco de
evaluación.

| Pieza | Archivo | Qué hace |
|---|---|---|
| Descarga de fuentes | `batch/descargar_fuentes.py` | Firebase, ThingSpeak y Open-Meteo → `batch/datos/fuentes/` |
| Capas plata y oro | `batch/capas.py` + `batch/oro.py` | Spark en batch → `batch/datos/plata/`, `batch/datos/oro/`, `calidad.json` |
| Partición | `batch/particion_modelos.yml` + `batch/particion.py` | Separación temporal única para todos |
| Evaluación | `batch/evaluacion.py` | Métricas con IC por bootstrap, líneas base y reporte por nivel |
| Evidencia | `docs/evidencias/f6/` | `calidad.json` y las líneas base de D2 |

`batch/datos/` no se versiona; todo se regenera con los comandos del final.

## 1. Fuentes

| Fuente | Cómo entra | Periodo | Filas crudas |
|---|---|---|---|
| ESP32 (Firebase `/board1/lecturas/historial`) | API REST con el usuario de Firebase Auth; solo entradas del firmware (sin `origen`) | 2026-10-01 23:10 → 2026-10-29 00:56 | 9,636 |
| Bronce de F3 (`data/local/bronce/`) | Parquet de Kafka, todos los orígenes | 2026-10-28 → 2026-10-29 | 9,186 (8,315 ESP32) |
| ThingSpeak, 7 canales | API pública: últimas 8,000 lecturas de cada canal; **3418755 completo** paginando hacia atrás (20,082 lecturas, 70 días) | 2026-07-26 → 2026-10-29 | 68,082 |
| Open-Meteo Juliaca | Archivo horario (ERA5, `archive-api`) desde el inicio de ThingSpeak; las últimas horas que el archivo aún no tiene salen del pronóstico con `past_days` | 2026-07-25 → 2026-10-29 | 2,304 + 22 |
| Flota extrapolada f2b.1 | Parquet `runId=r2026-45facae2` (semilla 2026) | 2026-09-28 → 2026-10-27 | 172,800 |
| Flota extrapolada f2b.2 | Parquet `runId=r7-79a6b935` (semilla 7, `generador_f2b2.yml`) | 2026-09-28 → 2026-10-27 | 172,800 |

Todas las fechas están en hora de Lima. Las filas del ESP32 que están a la vez en Firebase y en el bronce se quedan
una sola vez (ver plata).

**SUNASS, INEI y SENAMHI no se incorporan.** Ninguna dimensión U2 las necesita:

- D2, D4 y D6 trabajan sobre la serie de nivel de cada tanque.
- D8 toma el aire exterior de Open-Meteo, que es horario y cubre todo el periodo. SENAMHI solo daría estaciones
  diarias o con huecos.
- SUNASS (continuidad de servicio por EPS) e INEI (hogares) son agregados anuales por ciudad. Sirven al Brief como
  contexto y no como variable de un modelo horario. Quedan como fuentes batch de contexto para la sustentación (F8).

## 2. Capa plata (`batch/datos/plata/fuente=*/`)

Una fila por lectura. Columnas: las del contrato, más las que agrega esta capa.

1. **Unión** de todas las fuentes en un esquema común. Cada fila guarda su `fuente` (`esp32`, `thingspeak`,
   `open-meteo`, `ea-uk`, `usgs`, `aumentado_f2b1` o `aumentado_f2b2`) y su `origen_dato`, que dice por dónde llegó.
2. **Deduplicado por `eventId`** (y `runId` en los aumentados). Si una lectura llegó por dos caminos, se conserva el
   original: Firebase o la API antes que la copia de Kafka. Del bronce del ESP32 solo sobreviven 237 lecturas, las
   posteriores a la descarga de Firebase. La copia en Kafka de los canales de ThingSpeak se descarta porque la API
   completa los cubre.
3. **Sin sensores de prueba:** se excluyen los de `ingesta/sensores_prueba.yml` (`esp32-prueba-f3`).
4. **Lecturas inválidas marcadas, no borradas:** `lectura_valida` y `motivo_invalidez`. Las reglas de validez son las
   mismas de F3 (`RANGOS` de `streaming/procesar_stream.py`), más una regla geométrica: la distancia no puede quedar
   más allá del fondo del tanque.
5. **Nivel:** `distanciaCompensadaCm` (velocidad del sonido con el DHT11), `nivelPct`, `nivelCm` y `litros`. Se
   calculan con la misma función del detector de F4 (`Detector.nivel`) y la geometría de `ingesta/tanques.yml`. Cada
   fila guarda `geometria_version` (hash de su entrada en `tanques.yml`) y `geometria_por_medir`.

| Fuente | Sensores | Filas plata | Válidas | Periodo (Lima) |
|---|---|---|---|---|
| esp32 | 1 | 9,873 | 67.8 % | 10-01 23:10 → 10-29 00:56 |
| thingspeak | 7 | 68,082 | 94.9 % | 07-26 18:58 → 10-29 00:36 |
| open-meteo | 1 | 2,326 | 100 % | 07-25 19:00 → 10-29 18:00 |
| ea-uk | 23 | 434 | 98.6 % | 10-28 15:15 → 10-29 00:15 |
| usgs | 10 | 94 | 100 % | 10-28 16:00 → 10-29 00:45 |
| aumentado_f2b1 | 4 | 172,800 | 98.0 % | 09-28 00:00 → 10-27 23:59 |
| aumentado_f2b2 | 4 | 172,800 | 98.2 % | 09-28 00:00 → 10-27 23:59 |
| **Total** | | **426,409** | | |

Por canal de ThingSpeak: 3418755 tiene 20,082 lecturas (100 % válidas). 1233810, 1233815 y 1272968 están al 100 % y
3019316 al 99.5 %. 283977 (79.2 %) y 480876 (77.9 %) informan `nivelPct` fuera de 0-100 en ~21 % de sus lecturas.

**Motivos de invalidez**

| Fuente | Motivo | Lecturas |
|---|---|---|
| esp32 | `distanciaCm_mas_alla_del_fondo` | 2,240 |
| esp32 | `distanciaCm_-1` (HC-SR04 sin eco) | 900 |
| esp32 | `humedadPct_fuera_de_rango` | 31 |
| esp32 | `distanciaCm_fuera_de_rango` | 8 |
| thingspeak | `nivelPct_fuera_de_rango` | 3,431 |
| thingspeak | `distanciaCm_-1` | 39 |
| ea-uk | `caudalM3s_fuera_de_rango` | 6 |
| aumentado f2b.1 / f2b.2 | `distanciaCm_-1` · `humedadPct_fuera_de_rango` · `distanciaCm_mas_alla_del_fondo` | 1,716 / 1,497 · 1,290 / 1,097 · 428 / 557 |

El motivo más común en el ESP32, `distanciaCm_mas_alla_del_fondo`, **depende de la geometría provisional**
(`por_medir: true`). Cuando el equipo mida el tanque, plata se regenera y esa cifra cambia; por eso cada fila guarda
`geometria_version`. En los aumentados, los inválidos son las fallas que el generador inyecta a propósito
(`falla_sensor`).

## 3. Huecos del ESP32

El ESP32 escribe directo a Firebase por Wi-Fi, y la PC no interviene. Los huecos de Firebase son, por lo tanto,
**sesiones en que el ESP32 estuvo apagado**, no suspensiones de la PC.

| Sesión (Lima) | Duración | Lecturas | Válidas |
|---|---|---|---|
| 10-08 22:31 → 23:27 | 56 min | 672 | 102 |
| 10-15 20:46 → 23:18 | 152 min | 1,751 | 1,694 |
| 10-22 19:43 → 21:37 | 113 min | 1,360 | 1,272 |
| 10-28 15:45 → 20:09 | 264 min | 3,138 | 1,978 |
| 10-28 21:34 → 10-29 00:56 | 202 min | 2,425 | 1,635 |

- **Tramos:** con un umbral de 2 min hay 9 tramos continuos; 5 duran más de 30 min, y los otros son lecturas sueltas
  (por ejemplo, la del 10-01 23:10). Hay 8 huecos de más de 5 min y 4 de más de 1 h. El mayor dura **166 h** (7 días):
  el ESP32 se enciende una vez por semana, de tarde o de noche.
- **Huecos dentro de una sesión:** cortes de Wi-Fi o rachas de `-1` del HC-SR04. Las lecturas inválidas cortan los
  tramos de oro, que se construyen solo con lecturas válidas.
- **Suspensiones de la PC:** solo afectan el camino Firebase → puente → Kafka. Del bronce del 28-oct, **2,343
  lecturas llegaron a Kafka con más de 5 min de atraso**, recibidas entre las 17 y las 22 h con una mediana de 37 min.
  Corresponden a la puesta en marcha del puente y a las suspensiones de la PC. Como plata usa el `timestamp` del
  ESP32 (Firebase), ese atraso **no crea huecos** en el dataset.
- **Regla:** nada se interpola entre tramos. Cada variable derivada (deltas, tendencia, agua estimada) se calcula
  dentro de su tramo y queda vacía cuando no tiene datos suficientes. Los umbrales de hueco son 2 min en el ESP32,
  3 min en la flota (paso de 1 min) y 4 veces el intervalo mediano en cada canal de ThingSpeak.
- **Consecuencia para los modelos:** el tramo más largo del ESP32 dura 2.4 h. Un horizonte de 1 h con 1 h de
  historia necesita 2 h continuas, y el conjunto de diseño del ESP32 (la noche del 28-oct) no las tiene. A 15 min sí
  hay 8 puntos de evaluación (sección 6). **El ESP32 necesita sesiones más largas** para evaluar horizontes de una
  hora.

## 4. Capa oro (`batch/datos/oro/<tabla>/fuente=*/`)

Una tabla por dimensión, construida por sensor (y por corrida, en los aumentados) con `applyInPandas`. Los eventos
salen de **reprocesar plata con el detector congelado f4.3** (`streaming/detector_servicio.py`), la misma clase que
corre en vivo.

| Tabla | Grano | Filas | aumentado f2b.1 / f2b.2 | ThingSpeak | ESP32 |
|---|---|---|---|---|---|
| `d2_autonomia` | 5 min | 111,178 | 34,399 / 34,400 | 42,235 | 144 |
| `d4_llegadas` | 1 evento | 335 | 123 / 122 | 76 | 14 |
| `d6_noches` | 1 noche (00:00-03:30 Lima) | 241 | 120 / 120 | — | 1 |
| `d8_termica` | 1 h | 7,506 | 2,880 / 2,880 | 1,726 | 20 |

ThingSpeak no aparece en D6:

- Los canales con nivel (1233810, 1233815, 283977, 480876 y 3418755) lo informan ya calculado (% o m), sin distancia
  cruda que compensar.
- Los dos que miden distancia (1272968 y 3019316) no tienen geometría conocida (`modo: sin_nivel` en `tanques.yml`,
  decisión de F4) ni temperatura. Tampoco tienen nivel en D2 ni D8: solo aportan su validez de lectura.

**Notas por tabla**

- **D4:** los 14 eventos del ESP32 incluyen los artefactos de reconexión y los dos falsos de f4.3 (23:55 y 00:27,
  limitación conocida de F4). La columna `artefacto_reanudacion` marca los 7 que caen al inicio de una sesión, y
  `bitacora` trae `no_verificable` en 21:46 y 23:08. **Un modelo de D4 no debe aprender de las filas con
  `artefacto_reanudacion = true`.**
- **D6:** el ESP32 tiene una sola noche (28 → 29-oct): el ESP32 casi nunca está encendido de madrugada. D6 se
  entrena con la flota (`etiqueta` = `fuga_lenta` / `deriva_termica`) y se valida en vivo.
- **D8:** el canal 3418755 aporta 1,596 horas con aire y agua reales. En el ESP32 hay 20 horas con aire de la cámara,
  Juliaca y residencia.

### Diccionario de datos

Columnas comunes a las cuatro tablas:

| Columna | Tipo | Descripción |
|---|---|---|
| `fuente` | str (partición) | `esp32`, `thingspeak`, `aumentado_f2b1`, `aumentado_f2b2` |
| `sensorId` | str | Sensor del contrato |
| `runId` | str | Corrida del generador (solo aumentados) |
| `ts` | int64 | Epoch ms (UTC) del inicio del paso, del evento o de la noche |
| `particion` | str | `entrenamiento`, `embargo`, `prueba` o `consistencia` (sección 5) |
| `particion_version` | str | Versión de `particion_modelos.yml` (`p6.1`) |

**`d2_autonomia`** (paso de 5 min, D2 autonomía)

| Columna | Tipo | Descripción |
|---|---|---|
| `tramo` | int64 | Tramo continuo dentro del sensor (no se cruzan huecos) |
| `nivel_pct` | float64 | Mediana del nivel (%) en el paso |
| `litros` | float64 | Mediana del volumen (L); vacío si el tanque no tiene capacidad en `tanques.yml` |
| `n_lecturas` | int64 | Lecturas válidas en el paso |
| `delta_pct_5min` / `delta_pct_1h` | float64 | Cambio del nivel desde el paso anterior y desde 12 pasos antes (mismo tramo) |
| `tendencia_pct_h_3h` | float64 | Pendiente (%/h) por mínimos cuadrados sobre las últimas 3 h del tramo (mínimo 6 pasos) |
| `en_subida` | bool | `delta_pct_5min > 0.5` |
| `llegada_detectada` | bool | El detector f4.3 abrió una `llegada_agua` en este paso |
| `hora_lima` / `dia_semana` | float64 / int64 | Hora decimal en Lima; día (0 = lunes) |
| `tipo_anomalia` / `es_anomalia` | str / bool | Etiqueta del generador (solo aumentados) |

**`d4_llegadas`** (un evento por fila, D4 próxima llegada)

| Columna | Tipo | Descripción |
|---|---|---|
| `tipo` | str | `llegada_agua` o `corte_prolongado` |
| `eventoId` | str | Identificador determinístico del detector (`tipo-sensorId-inicioTs`) |
| `fin_ts` | int64 | Fin del evento (epoch ms) |
| `hora_lima` / `dia_semana` | float64 / int64 | Hora y día del inicio, en Lima |
| `valor` / `unidad` | float64 / str | Valor del evento (subida en % o horas sin llegada) |
| `subida_pct` | float64 | Subida del nivel en una llegada |
| `caudal_lpm` | float64 | Caudal de llenado estimado (L/min), si hay geometría |
| `fuera_franja` | bool | La llegada cayó fuera de la franja habitual |
| `criterio` | str | Criterio de f4.3 que confirmó la llegada: `rapido` o `lento` (vacío en `corte_prolongado`) |
| `horas_sin_llegada` | float64 | Para `corte_prolongado` |
| `intervalo_desde_anterior_h` | float64 | Horas desde la llegada anterior **sin artefacto** del mismo sensor (objetivo natural de D4) |
| `artefacto_reanudacion` | bool | Evento en los primeros 15 min de una sesión que sigue a más de 30 min sin lecturas: es la reconexión, no servicio real |
| `bitacora` | str | Acción de `docs/evidencias/f4/bitacora_real.csv` a ±5 min (por ejemplo, `no_verificable`) |
| `version_reglas` | str | `f4.3` |
| `tipo_anomalia` | str | Etiqueta del generador más frecuente en la ventana del evento (solo aumentados) |

**`d6_noches`** (una noche por fila, de 00:00 a 03:30 en Lima, D6 fuga)

| Columna | Tipo | Descripción |
|---|---|---|
| `noche` | str | Fecha (Lima) de la noche |
| `minutos_con_datos` / `cobertura` | int64 / float64 | Minutos con lectura y su fracción de los 210 min |
| `descenso_cm_compensado` / `descenso_cm_crudo` | float64 | Mediana de los últimos 15 min menos la de los primeros 15 (distancia compensada y cruda; > 0 = el agua bajó) |
| `tasa_cm_h_compensada` | float64 | Descenso compensado por hora |
| `distancia_media_cm` | float64 | Distancia compensada media |
| `temp_inicio_c` / `temp_fin_c` | float64 | Aire de la cámara al inicio y al final (la deriva térmica que corrige la compensación) |
| `hubo_subida` | bool | Hubo una subida de más de 2 % (llenado nocturno) |
| `frac_fuga_lenta` / `frac_deriva_termica` | float64 | Fracción de lecturas con esa etiqueta (aumentados) |
| `etiqueta` | str | `fuga_lenta`, `deriva_termica` o vacío (más de la mitad de la noche) |

**`d8_termica`** (paso de 1 h, D8 temperatura y residencia)

| Columna | Tipo | Descripción |
|---|---|---|
| `temp_aire_camara_c` | float64 | Aire de la cámara (DHT11) o aire en el tanque (3418755 `field2`) |
| `temp_agua_real_c` | float64 | Agua medida (solo ThingSpeak 3418755, DS18B20 `field3`): el objetivo para validar D8 |
| `temp_juliaca_c` | float64 | Aire exterior de Open-Meteo Juliaca (ESP32 y flota) |
| `temp_agua_estimada_provisional_c` | float64 | Primer orden con τ = 25.2 h de F4, dentro del tramo. **Es la estimación provisional del detector**, no el modelo de D8 |
| `horas_residencia` | float64 | Horas desde la última `llegada_agua` |
| `nivel_pct`, `n_lecturas`, `tramo`, `hora_lima` | | Como en D2 |

## 5. Partición (`batch/particion_modelos.yml`, versión `p6.1`)

Definida una sola vez para los cuatro modelos. Siempre temporal, nunca aleatoria.

| Origen | Entrenamiento / diseño | Embargo | Prueba |
|---|---|---|---|
| ESP32 | Todo lo anterior a max(congelamiento f4.3 = 2026-10-28 23:37:40, `geometria_desde`) | — | Desde esa hora; **intocable** hasta la evaluación final |
| ThingSpeak | Hasta 8 días antes del final de cada canal | 24 h | Últimos 7 días de cada canal |
| Aumentados | Primeros 21 días de cada corrida | 24 h | No hay prueba: los últimos ~8 días son `consistencia` (nunca métrica final) |

Filas por partición (entrenamiento / embargo / prueba o consistencia):

| Tabla | ESP32 | ThingSpeak | f2b.1 | f2b.2 |
|---|---|---|---|---|
| `d2_autonomia` | 144 / 0 / **0** | 36,113 / 631 / 5,491 | 24,065 / 1,141 / 9,193 | 24,107 / 1,143 / 9,150 |
| `d4_llegadas` | 14 / 0 / **0** | 54 / 0 / 22 | 87 / 5 / 31 | 85 / 5 / 32 |
| `d6_noches` | 1 / 0 / **0** | — | 84 / 4 / 32 | 84 / 4 / 32 |
| `d8_termica` | 20 / 0 / **0** | 1,404 / 24 / 298 | 2,016 / 96 / 768 | 2,016 / 96 / 768 |

- **El ESP32 todavía no tiene periodo de prueba.** `tanques.yml` tiene `por_medir: true` y `geometria_desde: null`.
  Cuando el equipo mida el tanque y fije `geometria_desde`, la prueba empieza sola en esa hora, sin tocar el YAML de la
  partición.
- **Embargo:** separa entrenamiento y prueba para que las ventanas móviles (1 h, 3 h) no vean el futuro.
- La columna `particion` de cada tabla oro ya trae la asignación. `batch/evaluacion.py` la respeta: las filas
  `prueba` solo se leen con `final=True`.

## 6. Marco de evaluación (`batch/evaluacion.py`)

- **Métricas:**
  - Regresión: MAE, RMSE y MAPE. El MAPE excluye los |y| menores que `mape_minimo` e informa cuántas filas quedaron.
  - Clasificación: precisión, exhaustividad, F1 y exactitud, con los conteos.
- **IC 95 %:** bootstrap de **bloques móviles**.
  - Los errores consecutivos de una serie están correlacionados; remuestrear puntos sueltos daría intervalos
    demasiado estrechos.
  - Por defecto se usan bloques de 12 pasos (1 h en D2) en la regresión, y de 1 en la clasificación por evento o por
    noche.
  - Con menos de dos bloques de datos no se informa el IC.
- **Líneas base obligatorias**, calculadas dentro de cada tramo:
  - **Persistencia:** ŷ(t+h) = y(t).
  - **Tendencia lineal:** recta de mínimos cuadrados sobre las últimas k observaciones, extrapolada h pasos.
  - Todo modelo informa `mejora_vs_persistencia` y `mejora_vs_tendencia` (1 − MAE_modelo / MAE_base).
- **Reporte por nivel, nunca promediado entre niveles:**
  - `consistencia`: la flota, partición `consistencia`.
  - `thingspeak_real`.
  - `esp32_real`.
- **Conjuntos:**
  - `diseño` (por defecto): el último 20 % del entrenamiento de cada sensor, para elegir y ajustar.
  - `prueba`: solo con `final=True`.
  - `entrenamiento()` devuelve lo que queda para ajustar, sin la cola de diseño.

**Demostración: líneas base de D2** (nivel % a horizonte h, k = 12 pasos de 5 min, conjunto de diseño;
`docs/evidencias/f6/lineas_base_d2_h60.json` y `_h15.json`)

| Nivel | h | Predictor | n | MAE [IC 95 %] | RMSE | MAPE % |
|---|---|---|---|---|---|---|
| consistencia | 60 min | persistencia | 17,820 | 2.67 [2.41, 2.95] | 6.55 | 3.69 |
| consistencia | 60 min | tendencia | 17,820 | 3.23 [2.86, 3.69] | 9.70 | 3.88 |
| thingspeak_real | 60 min | persistencia | 6,220 | 0.64 [0.54, 0.74] | 1.56 | 1.42 |
| thingspeak_real | 60 min | tendencia | 6,220 | 0.95 [0.82, 1.09] | 2.27 | 2.18 |
| esp32_real | 60 min | — | 0 | sin tramos de 2 h en el conjunto de diseño | | |
| consistencia | 15 min | persistencia | 18,021 | 0.73 [0.65, 0.81] | 2.30 | 1.03 |
| thingspeak_real | 15 min | persistencia | 4,281 | 0.23 [0.18, 0.28] | 0.97 | 0.32 |
| esp32_real | 15 min | persistencia | 8 | 0.87 (n < 2 bloques: sin IC) | 1.14 | 0.92 |
| esp32_real | 15 min | tendencia | 8 | 3.18 (sin IC) | 3.29 | 3.41 |

**Lectura:** la persistencia gana a la tendencia lineal en todos los niveles. El nivel de un tanque cambia a
escalones (llenados) y la recta los extrapola mal. Es la vara que deberá superar el modelo de D2 en F6-D2.

## 7. Cómo regenerar

```bash
# 1. Fuentes (imagen de ingesta, credenciales en .env)
docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/batch monitoraqua-ingesta:latest python descargar_fuentes.py
# 2. Flota f2b.2 en Parquet (si falta); f2b.1 ya está en batch/datos/aumentados/
.venv/Scripts/python ingesta/generador.py --config ingesta/generador_f2b2.yml --semilla 7 --parquet
# 3. Plata, oro y calidad (Spark en batch, imagen de streaming; local[4] para no agotar la memoria de Docker)
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/streaming:/repo/batch \
  monitoraqua-streaming:latest spark-submit --driver-memory 3g batch/capas.py
# 4. Líneas base de D2
.venv/Scripts/python batch/evaluacion.py --demo-d2 --horizonte-min 60
.venv/Scripts/python batch/evaluacion.py --demo-d2 --horizonte-min 15
```

El paso 3 tarda unos 2 min. Al leer el bronce, Spark avisa `FileNotFoundException ... origen=*`: es inofensivo. Se lee
con un glob porque el `_spark_metadata` del sink de streaming guarda rutas del contenedor de streaming
(`/datos/bronce`).
