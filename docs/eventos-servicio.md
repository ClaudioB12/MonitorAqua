# Eventos de servicio (F4)

Cubre el criterio 1 de la rúbrica: eventos de negocio publicados y consumidos en vivo, con contrato de evento y
tópico documentados.

Spark detecta los eventos en vivo a partir de las lecturas de los tanques, los publica en **`servicio-eventos`** y
los escribe en InfluxDB (measurement `eventos_servicio`) para las anotaciones de Grafana de F5. Un consumidor con
grupo propio los lee en vivo y los valida contra el contrato.

```text
aqua-eventos-real ─> Spark (consulta eventos_servicio: watermark 7 min, dedup eventId, estado por sensor)
                        ├─> Kafka servicio-eventos (clave sensorId) ─> consumidor_eventos.py (grupo aqua-servicio-consumidor)
                        └─> InfluxDB eventos_servicio (tiempo = inicioTs)
```

## Tópico `servicio-eventos`

| | |
|---|---|
| Particiones | 3 |
| Retención | 7 días (`retention.ms = 604800000`) |
| Clave | `sensorId`: todos los eventos de un tanque quedan en orden en la misma partición |
| Valor | JSON según `contratos/servicio_evento.schema.json` |
| Productor | `lambda26-aqua-streaming`, consulta `eventos_servicio` (checkpoint propio) |
| Consumidor | `lambda26-aqua-consumidor-eventos`, grupo **`aqua-servicio-consumidor`**; su lag llega a Prometheus por kafka-exporter (`kafka_consumergroup_lag{consumergroup="aqua-servicio-consumidor"}`) |

## Contrato

`contratos/servicio_evento.schema.json` (JSON Schema 2020-12, `additionalProperties: false`):

| Campo | Tipo | Significado |
|---|---|---|
| `eventoId` | string | **determinístico**: `<tipo>-<sensorId>-<inicioTs>`. Si Spark reprocesa un lote tras un reinicio, el evento sale con el mismo id y el consumidor lo descarta |
| `tipo` | enum | `llegada_agua`, `corte_prolongado`, `alerta_fuga`, `autonomia_baja`, `umbral_termico`, `falla_sensor` |
| `sensorId`, `zona` | string | tanque y zona (`ingesta/tanques.yml`) |
| `severidad` | enum | `info`, `advertencia`, `critica` |
| `inicioTs`, `finTs` | int (epoch ms) | intervalo de la evidencia, en tiempo de evento; `finTs ≥ inicioTs` |
| `valor`, `umbral` | número o null | lo observado y el umbral de la regla, en la misma `unidad` |
| `unidad` | enum | `%`, `h`, `cm`, `°C`, `lecturas` (fija por tipo) |
| `origenLecturas` | enum | `real` (`aqua-eventos-real`) o `aumentado` (validación con la flota extrapolada) |
| `detectadoTs` | int (epoch ms) | hora del reloj en que Spark lo detectó |
| `versionRegla` | string | versión de `reglas_servicio.yml` (`f4.1`), o `provisional` para `umbral_termico` |
| `detalle` | objeto (opcional) | datos propios del tipo: caudal de llenado, fuera de franja, litros, tasa de descenso, temperatura del agua estimada, etc. |

Dos reglas que JSON Schema no expresa (`eventoId` = `tipo-sensorId-inicioTs` y `finTs ≥ inicioTs`) las valida
`errores_servicio()` en `ingesta/comun.py`. La usan el validador y el consumidor.

`contratos/validar.py` agrega 12 ejemplos de servicio (3 válidos y 9 inválidos) a los 28 anteriores: **0 fallos**.

## Reglas

Los umbrales están en `streaming/reglas_servicio.yml` y la geometría en `ingesta/tanques.yml`. El detector **relee
ambos archivos cuando cambian**; no hace falta reiniciar Spark. El nivel se calcula con la geometría:

`nivel_cm = sensor_al_fondo − distancia compensada`, con `distancia compensada = distancia × c(T) / c(20 °C)` y la
temperatura del DHT11.

**Compensación en el firmware (f4.4).** El firmware del ESP32 (`firmware/esp32/MonitorAqua_esp32.ino`) ya convierte
el tiempo de vuelo con `vSonido = 331.3 + 0.606 · T` del DHT11, o con 20 °C si el DHT11 falla. La `distanciaCm` que
llega a Firebase **ya viene compensada**. Hasta f4.3 el detector la compensaba otra vez.

- **El efecto de la doble corrección:** de −0.48 a +0.26 cm en el historial, a ~13 cm de distancia. A 50-100 cm, con
  4-10 °C de enfriamiento nocturno, serían 0.35-1.76 cm por noche de cambio ficticio: el enfriamiento se ve como una
  subida que tapa una fuga, del orden del umbral de `alerta_fuga` (0.8 cm).
- **La corrección:** el tanque declara `compensacion_en_firmware: {desde}` en `ingesta/tanques.yml`. Desde esa fecha el
  detector no compensa la distancia (`compensacionOrigen = firmware` en la capa plata). Antes de esa fecha la
  compensación es `desconocida` y D6 excluye esas lecturas. La flota simulada (f2b.1/f2b.2) imita un sensor que **no**
  compensa, así que en ella el detector sigue compensando.
- **Supuesto documentado: "desde el inicio" (1-oct).** El equipo confirma la versión del firmware solo desde el
  28-oct. Antes no hay registro, y los datos no permiten distinguirlo:
  - **Sesiones con cambios de temperatura (15 y 22-oct, 4-8 °C):** miden 4-17 cm, donde compensar o no cambia ≤ 0.1
    cm, igual o menos que la resolución. El 22-oct, la distancia pasa de 16.3 a 16.2 cm con la temperatura bajando de
    28.9 a 25.0 °C, lo que es compatible con compensación, pero es un solo décimo.
  - **Sesiones a 137-215 cm (1 y 8-oct):** la temperatura casi no varía (18.0-18.7 °C), así que solo habría un desfase
    constante de ~0.3-0.6 cm, sin cambios ficticios.
  - **Consecuencia:** el supuesto no cambia ningún resultado.
- **Sensor:** el firmware usa un **JSN-SR04T con placa AJ-SR04M** (no un HC-SR04), con zona ciega de ~20-25 cm. Las
  lecturas reales de 4-17 cm caen dentro de esa zona: eso explica, al menos en parte, los ecos alternos (7 ↔ 17 cm)
  que motivaron la regla de intermitencia. **Montar la sonda al menos 25 cm sobre el nivel máximo** antes de la
  prueba real.

Antes de evaluar se descartan:

- las lecturas inválidas de F3;
- las distancias más allá del fondo;
- los sensores de prueba (`ingesta/sensores_prueba.yml`).

| Evento | Regla | Umbral (f4.4, congelado) |
|---|---|---|
| `llegada_agua` | La mediana móvil de 5 lecturas sube más de X % de la capacidad dentro de la ventana, y la subida se sostiene en 3 medianas seguidas (f4.2). Tras una falla del sensor la ventana se reinicia: el nivel de antes de la falla no sirve de referencia (f4.2). **Llenado lento (f4.3):** también hay llegada si la tendencia de 60 min (mediana de los primeros 10 min contra la de los últimos 5) sube 5 % o más, confirmada en 5 lecturas. **Caudal de llenado estimado** = subida × capacidad / Δt (L/min), fórmula de la adecuación de D4. `fueraDeFranja` si empieza fuera de la franja de la zona (± 1 h). **Subida sostenida (f4.4)**, solo en canales que informan nivel en % o m (ThingSpeak): la mediana de los últimos 15 min (por tiempo) supera en 10 pp su mínimo desde el último llenado, con al menos 2 lecturas; es el criterio de la adecuación de D4. **Sin llegadas** mientras el sensor está inestable o en la gracia tras una reconexión (f4.4) | 6 % en 15 min, o 5 % en 60 min (lento), o 10 pp sostenidos en 15 min (canales de nivel) |
| `corte_prolongado` | Horas desde la última `llegada_agua` (o desde el inicio del flujo). `detalle.posibleHuecoDeDatos` avisa si la lectura que dispara la regla llegó después de ≥ 1 h sin datos. **Reanudación (f4.4):** tras un hueco de más de 30 min se reinician las referencias y el reloj vuelve a contar desde la reconexión (`detalle.referencia = reanudacion`), salvo que ya hubiera un corte avisado, que sigue vigente | 30 h (intervalo típico ~24 h × 1.25); crítica desde 48 h |
| `alerta_fuga` | Cada noche (00:00-03:30 Lima, antes de la franja de llegada) sin llenado. El descenso de la distancia compensada debe superar dos cosas: (a) 3 × la incertidumbre del DHT11 (±0.36 % de la distancia, es decir ±0.18 cm a 50 cm) y un mínimo absoluto; (b) la tasa nocturna habitual del tanque más un margen. La tasa habitual es la media exponencial de las noches sin alerta. **No se evalúan (f4.4)** las noches con el sensor inestable o con una reconexión dentro de la ventana | 0.8 cm; base + 0.25 cm/h |
| `autonomia_baja` | El nivel cae bajo el umbral; se rearma al subir del nivel de rearme. La proyección en horas llega en F7 | 20 % (crítica bajo 10 %); rearme 30 % |
| `umbral_termico` | Temperatura del agua estimada con inercia de primer orden desde el aire de la cámara (τ = 25.2 h). Se proyecta 6 h con el ciclo horario de Juliaca más la anomalía actual del aire y se avisa si cruza un umbral. **`versionRegla = provisional`** hasta el modelo de F6 | 4 °C y 25 °C |
| `falla_sensor` | N lecturas de nivel inválidas seguidas (`-1`, fuera de rango o más allá del fondo). **Histéresis (f4.2):** no se rearma hasta tener 2 min de lecturas válidas seguidas, así un sensor intermitente es un solo episodio. **Intermitencia (f4.4)**, solo sensores de distancia: 4 lecturas inválidas o saltos de más de 3 cm entre lecturas seguidas en 5 min marcan el sensor **inestable** hasta 10 min de calma; mientras tanto no hay `llegada_agua` ni `autonomia_baja`, y al calmarse se reinician las referencias. Un llenado real cambia la distancia ~0.1 cm cada 5 s | 5 lecturas; rearme tras 2 min; inestable: 4 en 5 min, calma 10 min |

### Por qué τ = 25.2 h en `umbral_termico`

En ThingSpeak 3418755 el agua varía el 15 % de lo que varía el aire y va 5-7 h atrasada (adecuación de D8). Para un
sistema de primer orden con el ciclo diario (ω = 2π/24 h):

- la amplitud de 15 % exige `ωτ = √(1/0.15² − 1) ≈ 6.6`, o sea τ ≈ 25 h;
- ese mismo τ da un atraso de `atan(ωτ)/ω ≈ 5.4 h`, dentro de lo medido.

Los dos datos son coherentes con un solo parámetro.

### Por qué procesamiento con estado y no ventanas

Las reglas son máquinas de estado por tanque con horizontes largos y distintos entre sí:

- "horas desde la **última** llegada";
- "N inválidas **seguidas**";
- "esta noche frente a la tasa de las noches **anteriores**".

Con ventanas habría que fijar una ventana de días para el corte y otra para cada regla, y ni "desde la última" ni
"seguidas" se pueden expresar. `applyInPandasWithState` guarda por `sensorId` solo lo necesario (último llenado,
rachas, medianas por minuto de la noche en curso, tasa nocturna habitual) y procesa cada lectura una vez.

La lógica vive en `streaming/detector_servicio.py`, en Python puro y sin Spark: la misma clase corre en vivo, en la
validación con datos aumentados y en pruebas locales. Va detrás del mismo watermark de 7 min y del mismo
`dropDuplicatesWithinWatermark(eventId)` que `rapidas_agg`; Spark 4.2 acepta encadenar los dos operadores con estado.
El estado no usa timeouts: el corte se evalúa con cada lectura.

## Congelamiento de reglas

**Las reglas quedan congeladas en la versión f4.4 desde el 2026-10-29 a las 17:53:25, hora de Lima
(2026-10-29 22:53:25 UTC).** Ese es el momento en que `lambda26-aqua-streaming` arrancó con f4.4, y queda registrado
en `streaming/reglas_servicio.yml` → `congelamiento`.

**Commit de congelamiento: COMMIT_F44** (rama `u2-f44`). Fija `streaming/reglas_servicio.yml` (f4.4),
`streaming/detector_servicio.py` e `ingesta/tanques.yml` (`compensacion_en_firmware`) antes de cualquier prueba con
medidas reales. El hash no puede ir dentro de su propio commit, porque escribirlo cambiaría el contenido; por eso se
registra en un commit posterior que solo agrega estas líneas. Para verificarlo:

```bash
git diff COMMIT_CORTO -- streaming/reglas_servicio.yml streaming/detector_servicio.py
# solo debe mostrar la línea commit del bloque congelamiento; ninguna regla ni código del detector
```

**Congelamiento anterior (f4.3):** desde el 2026-10-28 23:37:40 (Lima), commit
[`b400e7d`](https://github.com/ClaudioB12/MonitorAqua/commit/b400e7d480363274665ece065db5ea07c66390db). Su periodo de
rendimiento real nunca empezó (geometría del ESP32 provisional), así que no hay cifras de f4.3 que conservar. Los
eventos que publicó (versionRegla f4.3) quedan como diseño.

**Partición p6.2** (`batch/particion_modelos_p6.2.yml`): el inicio de la prueba del ESP32 pasa a ser la más tardía
entre este congelamiento y `geometria_desde`. p6.1 se conserva sin cambios para reproducir d2.1-d2.2 y d4.1. Es
legítimo porque la prueba real del ESP32 nunca empezó: con p6.1 no había ningún dato en "prueba", así que mover su
inicio no reutiliza ni descarta nada evaluado. La regeneración de plata y oro con p6.2 y la compensación corregida
deja **idénticas** las tablas oro de flota y ThingSpeak (d2, d4, d6, d8), así que los resultados de D2 y D4 no
cambian.

- **Conjunto de diseño.** Todo lo real anterior a ese momento se usó para diseñar las reglas: el historial del ESP32
  del 1 al 29 de octubre, la falla intermitente del HC-SR04 de esa noche y las llegadas de las 21:46 y las 23:08. Con
  eso se hicieron f4.2, f4.3 y f4.4, así que **no se usa para reportar rendimiento real**. Se muestra aparte, como diseño.
- **Rendimiento real.** Se calcula **solo con lecturas y bitácora posteriores al congelamiento** y, para cada sensor,
  posteriores también al cambio a geometría real:
  - **Inicio:** la más tardía entre `congelamiento.desde` y `geometria_desde` del sensor en `ingesta/tanques.yml`.
  - **Geometría provisional:** mientras siga siendo provisional (`por_medir: true`), el sensor no tiene periodo de
    rendimiento real. Sus niveles (llegada, autonomía) no son medidas reales.
  - **Separación automática:** `streaming/comparar_bitacora.py` lee las dos fechas y separa las partes por sí mismo.
  - **Al cargar las medidas reales del recipiente:** se ponen las medidas y `por_medir: false` en `tanques.yml`, junto
    con la fecha y hora del cambio en `geometria_desde`.
- **Cambios futuros.** Cualquier cambio de reglas exige una versión nueva y un congelamiento nuevo, y el rendimiento
  real de esa versión vuelve a empezar desde su propio congelamiento.

Historia de versiones:

| Versión | Cambio | Motivo |
|---|---|---|
| f4.1 | reglas iniciales | — |
| f4.2 | `falla_sensor` con histéresis (rearme tras 2 min válidos); `llegada_agua` confirmada en 3 medianas y con ventana reiniciada tras una falla | la falla intermitente real del 28-oct (conjunto de diseño): tormenta de `falla_sensor` y llegada falsa al recuperarse el sensor |
| **f4.3** | `llegada_agua` también por **llenado lento**: tendencia de 60 min (mediana de los primeros 10 min contra la de los últimos 5) mayor o igual a 5 %, confirmada en 5 lecturas | en Juliaca, con baja presión de red, los llenados lentos son lo común; con f4.2 un tanque de 2500 L a 5 L/min no llegaba al 6 % en 15 min y se perdía |
| **f4.4** | (1) `llegada_agua` por **subida sostenida** en canales de nivel (%/m); (2) **intermitencia** del HC-SR04: sensor inestable, sin llegadas ni autonomía baja; (3) **reanudación** tras huecos de datos: referencias y reloj del corte reiniciados, 15 min de gracia sin llegadas; y noches perturbadas fuera de `alerta_fuga` | las tres limitaciones conocidas de f4.3: llenados no vistos en ThingSpeak 1233810 (publica cada ~10 min, en %), llegadas falsas por ecos alternos (23:55 y 00:27 del 28-29 oct) y llegadas y cortes falsos al reconectarse el ESP32 tras días apagado |

f4.3 compara medianas, no el mínimo de la ventana. Una primera versión usaba el mínimo de 60 medianas como base, y con
ruido ×2 la precisión de `llegada_atipica` bajaba de 0.92 a 0.75, porque el mínimo de una serie ruidosa es un extremo
bajo que infla la "subida". Con medianas vuelve a 0.92 (ruido ×2) y 1.00 (ruido ×3), y los llenados lentos se
detectan igual.

## Validación

Hay tres niveles de validación y **no se mezclan**:

1. **Consistencia detector-generador** (datos aumentados). El generador y el detector comparten definiciones: el
   `-1` de falla, la franja horaria y la escala de los llenados. Esta cifra mide si el detector reconoce lo que el
   generador inyectó con esas mismas definiciones: es **consistencia de implementación, no rendimiento real**.
2. **Sensibilidad**. Rompe a propósito esos supuestos compartidos y mide cuánto se degrada la consistencia.
3. **Rendimiento real**. Compara la bitácora de acciones en el tanque real con los eventos detectados, **solo después
   del congelamiento**.

Todas las proporciones llevan su intervalo de confianza de 95 % de Wilson. Con pocos episodios son muy anchos, y eso
es parte del resultado.

### 1. Consistencia detector-generador

**Corrida de referencia** (`aqua-eventos-aumentados`, generador f2b.1, semilla 2026, reglas f4.3):

- **Mismo detector:** `streaming/validar_detector.py` pasa las 172,800 lecturas por el mismo detector y el mismo
  Spark, con `trigger(availableNow)` y `origenLecturas = aumentado`.
- **Salida aislada:** publica en un tópico de prueba (`servicio-eventos-validacion-f4`, **borrado al terminar**) y en
  un JSONL, sin escribir en InfluxDB.
- **Evaluación:** `streaming/evaluar_detector.py` empareja eventos con episodios etiquetados del mismo sensor e
  intervalo superpuesto (± 30 min).

| Anomalía etiquetada | Evento | Episodios | Eventos | Consistencia: precisión [IC 95 %] | Consistencia: recall [IC 95 %] |
|---|---|---|---|---|---|
| `fuga_lenta` | `alerta_fuga` | 5 | 1 | 0.00 [0.00-0.79] | 0.00 [0.00-0.43] |
| `corte_prolongado` | `corte_prolongado` | 1 | 10 | 0.20 [0.06-0.51] | 1.00 [0.21-1.00] |
| `llegada_atipica` | `llegada_agua` fuera de franja | 4 | 4 | 1.00 [0.51-1.00] | 1.00 [0.51-1.00] |
| `falla_sensor` | `falla_sensor` | 25 | 25 | 1.00 [0.87-1.00] | 1.00 [0.87-1.00] |
| `deriva_termica` | `alerta_fuga` **falsa** | 3 | 0 | — | 0 de 3 episodios con alerta falsa [0.00-0.56] |

**Generador f2b.1 frente a f2b.2** (semilla 7, la misma en los dos; reglas f4.3; mismo detector, fuera de Spark):

f2b.2 (`ingesta/generador_f2b2.yml`) cambia dos supuestos del generador que, con f2b.1, castigaban al detector por
diferencias de definición:

- **Fugas persistentes:** duran de 1 a 4 días (1.5-4 L/h, un flotador o un goteo) y cruzan las noches, en lugar de
  4-10 h a cualquier hora. Hay al menos 4 por corrida.
- **Llegadas omitidas etiquetadas:** **toda** llegada que no ocurre se etiqueta como `corte_prolongado`, también el
  8 % de días sin agua que f2b.1 dejaba sin etiqueta.

| Anomalía → evento | f2b.1: episodios | f2b.1: precisión / recall | f2b.2: episodios | f2b.2: precisión / recall |
|---|---|---|---|---|
| `fuga_lenta` → `alerta_fuga` | 4 | 0.50 [0.10-0.91] / 0.25 [0.05-0.70] | 4 | **0.86 [0.49-0.97] / 0.75 [0.30-0.95]** |
| `corte_prolongado` → `corte_prolongado` | 1 | 0.06 [0.01-0.28] / 1.00 [0.21-1.00] | 7 | **0.58 [0.32-0.81] / 1.00 [0.65-1.00]** |
| `llegada_atipica` → `llegada_agua` fuera de franja | 4 | 1.00 [0.51-1.00] / 1.00 [0.51-1.00] | 4 | 1.00 [0.51-1.00] / 1.00 [0.51-1.00] |
| `falla_sensor` → `falla_sensor` | 26 | 1.00 [0.87-1.00] / 1.00 [0.87-1.00] | 27 | 1.00 [0.88-1.00] / 1.00 [0.88-1.00] |
| todas las llegadas efectivas → `llegada_agua` | 108 | recall 0.98 [0.94-0.99] | 110 | recall 1.00 [0.97-1.00] |
| `deriva_termica` → `alerta_fuga` falsa | 3 | 0 de 3 | 3 | 0 de 3 |

Runs: f2b.1 `r7-48fedbe6`, f2b.2 `r7-79a6b935`. En f2b.2, 2 llegadas omitidas cayeron dentro de una fuga larga y
quedaron sin etiquetar (los episodios no se superponen); explican parte de los cortes "falsos". La corrida f2b.2 tiene
16.2 % de eventos anómalos, porque las fugas largas y los cortes etiquetados ocupan mucho tiempo.

**Cómo leerlo:**

- **Los 1.00 de `falla_sensor` y `llegada_atipica` son consistencia por construcción.** El generador escribe `-1` y
  el detector busca `-1`; el generador pone las atípicas a las 12-18 h y el detector marca fuera de franja lo que
  está fuera de 4-8 h ± 1 h. Con 4 episodios, además, el IC va de 0.51 a 1.00.
- **`alerta_fuga` y `corte_prolongado` mejoran mucho con f2b.2.** Con f2b.1, buena parte de sus "errores" era
  desacuerdo de definiciones (fugas diurnas cortas, llegadas omitidas sin etiquetar), no fallas del detector. Aun
  así, son 4 y 7 episodios: los IC siguen siendo anchos.
- **Compensación térmica:** las corridas con compensación y sin ella dan las mismas métricas (`--sin-compensacion`).
  A 29-40 cm del sensor, la deriva en la ventana nocturna es de décimas de cm. Comparando con la distancia verdadera
  (`distanciaRealCm`), la compensación elimina **~70 %** del error térmico en las tres noches con helada: −0.47,
  −0.42 y −0.42 cm crudo, frente a −0.17, −0.09 y −0.14 cm compensado. Esto con el aire del generador (σ 0.2 °C), no
  con el ±2 °C real del DHT11.
- **`umbral_termico`:** ningún evento; con τ = 25 h el agua no baja de 4 °C en 6 h.

### 2. Sensibilidad (`streaming/sensibilidad.py`, generador f2b.1, 3 semillas: 2026, 7 y 11, reglas f4.3)

Conteos sumados de las tres semillas. "Llegadas (todas)" usa la agenda de suministro del generador: es el recall
sobre **todas** las llegadas que subieron el tanque al menos 3 cm, normales y atípicas.

| Variante | Qué rompe | Llegadas (todas), recall | `llegada_atipica` precisión / recall | `alerta_fuga` precisión (eventos) | `falla_sensor` |
|---|---|---|---|---|---|
| base | — | 0.99 [0.98-1.00] (333) | 1.00 / 1.00 (12 epis.) | 0.50 [0.19-0.81] (6) | 1.00 / 1.00 (76) |
| ruido ×2 | σ y frecuencia de saltos del perfil real × 2 | 0.99 [0.98-1.00] | 0.92 [0.67-0.99] / 1.00 | 0.33 [0.12-0.65] (9) | 1.00 / 1.00 |
| ruido ×3 | × 3 | 0.99 [0.98-1.00] | 1.00 / 1.00 | **0.17 [0.06-0.39] (18)** | 1.00 / 1.00 |
| llenado en el umbral | 5 L/min: un tanque de 2500 L sube solo 3 % en 15 min | **1.00 [0.99-1.00]** (f4.2: 0.73 [0.68-0.77]) | 1.00 / 1.00 (f4.2: 0.83) | 0.50 (6) | 1.00 / 1.00 |
| borde de franja | atípicas a las 8:00-9:30: fuera de tolerancia en la zona A, **dentro** en la zona B | 1.00 [0.98-1.00] | 1.00 / **0.75 [0.47-0.91]** | 0.50 (6) | 1.00 / 1.00 |

**Qué se aprende:**

- **Llenados lentos:** el criterio de f4.3 los recupera (0.73 → 1.00) sin perder precisión con ruido.
- **El ruido es el punto débil de `alerta_fuga`.** Con ruido ×3, dos de cada tres alertas son falsas.
- **"Atípica" depende de la definición de franja.** Si una llegada es atípica para el generador pero cae dentro de la
  tolerancia del detector, no se marca.
- **`falla_sensor` no se mueve en ninguna variante.** No es robustez: el generador escribe el mismo `-1` que el
  detector busca.

### 2b. f4.4 frente a f4.3 (`streaming/comparar_versiones.py`, mismos datos, 7 variantes × 3 semillas)

**Lo que se puede demostrar:** f4.4 elimina los falsos positivos **conocidos** del historial real (tabla siguiente).
**Lo que no:** en los datos aumentados, f4.4 **no es significativamente mejor ni peor** que f4.3. Todos los IC 95 %
de las dos versiones se solapan, en todas las variantes. Las diferencias de abajo son puntuales y se reportan para
transparencia, no como mejora.

Tablas completas en `docs/evidencias/f4/f4.4/tablas_f44.md`.

| Variante | Métrica | f4.3 | f4.4 |
|---|---|---|---|
| base | `alerta_fuga` precisión | 0.50 [0.19-0.81] | 0.75 [0.30-0.95] |
| base | `llegada_atipica` precisión | 1.00 [0.76-1.00] | 0.92 [0.67-0.99] |
| ruido ×2 | `alerta_fuga` precisión | 0.33 [0.12-0.65] | 0.60 [0.23-0.88] |
| ruido ×3 | llegadas (todas), recall | 0.99 [0.98-1.00] | 0.98 [0.96-0.99] |
| ruido ×3 | `llegada_atipica` precisión | 1.00 [0.76-1.00] | 0.86 [0.60-0.96] |
| ruido ×3 | `alerta_fuga` precisión / recall | 0.17 / 0.23 | 0.33 / 0.15 |
| f2b.2 | `alerta_fuga` precisión / recall | 0.80 [0.49-0.94] / 0.33 [0.14-0.61] | 0.90 [0.60-0.98] / 0.42 [0.19-0.68] |
| f2b.2 | `corte_prolongado` recall | 1.00 [0.83-1.00] | 0.95 [0.75-0.99] |
| **f2b.3** | `alerta_fuga` precisión / recall | 0.80 [0.49-0.94] / 0.42 [0.19-0.68] | 0.86 [0.49-0.97] / 0.33 [0.14-0.61] |
| **f2b.3** | `deriva_termica` → `alerta_fuga` falsa | 1 de 9 | 0 de 9 |

- **f2b.3 imita al ESP32:** el firmware compensa con un DHT11 de ±2 °C redondeado a grados enteros. f4.3 compensa por
  segunda vez; f4.4 no.
  - Con la doble compensación aparece 1 alerta de fuga falsa por deriva térmica (de 9 episodios), y con f4.4
    ninguna.
  - En `alerta_fuga` las dos versiones quedan dentro del mismo IC.
  - **Por qué el efecto es chico aquí:** la flota mide a 29-40 cm, donde la doble corrección es de décimas de cm. El
    daño estimado de 0.35-1.76 cm por noche corresponde a 50-100 cm.
  - **La razón para corregirla es física, no estadística:** el firmware ya compensa (`firmware/esp32`), así que
    compensar otra vez es un error de definición, aunque estos datos no alcancen para medirlo con significancia.
- **El costo observado:** ~1 llegada atípica falsa más y, con ruido ×3, el 1-2 % de las llegadas que la intermitencia
  confunde con ecos.
- **`alerta_fuga` sube en algunas variantes:** viene de no evaluar noches perturbadas, pero tampoco es significativo.
- **Por qué 4 eventos en la regla de intermitencia y no 3:** con 3, el recall de llegadas con ruido ×3 caía a
  0.92 [0.89-0.95], ya fuera del IC de f4.3, y la precisión de atípicas a 0.71. Con 4 el historial real queda igual
  de limpio.

**Historial real (conjunto de DISEÑO: con él se diseñó f4.4, no es rendimiento). Es la evidencia demostrable de f4.4:**

| Tanque | f4.3 | f4.4 |
|---|---|---|
| ESP32 | 13 `llegada_agua`, 4 `corte_prolongado`, 3 `autonomia_baja`, 16 `falla_sensor` | **solo los 16 `falla_sensor`** |
| ThingSpeak 1233810 (nivel en %) | 0 llegadas, 1 corte | 31 llegadas (subida sostenida, ~1 cada 2 días y casi siempre 16:30-17:00), 19 cortes |
| ThingSpeak 283977 | 0 llegadas, 1 corte | 3 llegadas, 3 cortes |
| ThingSpeak 3418755 (lluvia) | 49 llegadas, 16 cortes | 65 llegadas, 21 cortes |
| 1233815 y 480876 | — | sin cambios |

- **Desaparecen los falsos positivos conocidos del ESP32:** las llegadas de las 23:55 y las 00:27 (ecos alternos de
  7 ↔ 17 cm) y las de 00:44, 01:12, 01:46 y 09:23.
- **Desaparecen también las de reconexión:** 08-oct 22:33, 15-oct 20:48 y 22-oct 19:44, con sus cortes de 140-167 h.
- **Las de las 17:21 y 18:44 del 28-oct:** eran el sensor manipulado (146 cm, fuera del recipiente, y luego
  recolocado); suprimirlas es correcto.
- **Las de las 21:46 y 23:08 (`no_verificable` en la bitácora):** también desaparecen, porque llegan en plena
  inestabilidad: la recolocación desde 144 cm, y el regreso tras 10 min de `-1`, con lecturas de 4 a 18 cm alternadas
  con `-1`. Es coherente con los datos, pero siguen sin verificar y no se cuentan como aciertos.
- **1233810:** sus 31 llegadas usan el mismo criterio de llenado de D2 (`batch/modelo_d2.yml` → `llenado`).
- **La bitácora no tiene ninguna acción de llenado.** Su única acción es la `falla` de las 22:50:29, registrada
  a posteriori por el sistema. f4.4 la sigue detectando igual que f4.3: queda dentro del episodio `falla_sensor`
  iniciado a las 22:13. Las otras dos filas son las `no_verificable` de las 21:46 y 23:08.
- **Con la compensación en el firmware, el historial da los mismos eventos.** La corrección cambia distancias (y
  `alerta_fuga` a partir de la prueba real), no los eventos de diseño.

### 3. Rendimiento real: bitácora de pruebas con el ESP32

**Bitácora:** `docs/evidencias/f4/bitacora_real.csv` tiene una fila por acción sobre el tanque real:

- **Campos:** `timestamp` ISO con zona horaria, `accion` (llenado, vaciado, falla o no_verificable), `sensorId`,
  `timestamp_fin` opcional, `observador` y `nota`.
- **Script:** `streaming/comparar_bitacora.py` empareja cada acción con su evento (llenado → `llegada_agua`, vaciado →
  `autonomia_baja`, falla → `falla_sensor`) dentro de −5/+20 min. Una falla anotada tarde se empareja con un episodio
  `falla_sensor` ya activo, iniciado hasta 2 h antes.
- **Filas `no_verificable`:** nadie sabe qué pasó. La fila y los eventos a ±5 min de ella quedan fuera del cálculo.
  Los demás eventos del periodo sin acción anotada se listan como posibles falsos positivos.
- **Fuente de eventos:** lo publicado en vivo en `servicio-eventos`, o `--reproceso`, que corre las reglas vigentes
  sobre las lecturas reales de bronce.
- **Separación por congelamiento:** el script divide la salida en **conjunto de diseño** y **rendimiento real**.

**Estado al congelar (28-oct, 23:37:40):**

- **Conjunto de diseño:** 1 acción (la falla del HC-SR04, registrada desde los datos, `observador = sistema`, **no
  independiente**) y 2 filas `no_verificable`: las `llegada_agua` de las 21:46 y las 23:08. Por **decisión del
  equipo** quedan como no verificables y fuera del cálculo.

  Lo que mostró ese caso: el HC-SR04 estaba **intermitente**, no muerto. Desde las 22:13 alternaba `-1` con lecturas
  muy ruidosas (10-18 cm, picos de 383 cm, ~25 veces el σ del perfil real). Con f4.1 eso daba 47 `falla_sensor` en
  todo el historial real del ESP32 y una llegada falsa al recuperarse el sensor. Con f4.2 o f4.3 son 9, y la falla de
  esa noche es un solo episodio desde las 22:13. **Nada de esto es rendimiento real:** es el conjunto con el que se
  diseñaron las reglas.
- **Rendimiento real: todavía no hay cifra.** No hay acciones anotadas después del congelamiento. Sale de la prueba
  en vivo con el recipiente.

## Eventos reales detectados (historial de `aqua-eventos-real`)

Al arrancar, la consulta procesó todo el historial del tópico (7,468 lecturas) y publicó 51 eventos con las reglas
f4.1, todos válidos y con clave = `sensorId`:

| Evento | Cantidad | Qué son |
|---|---|---|
| `falla_sensor` | 43 (ESP32) + 1 (ThingSpeak 480876) | rachas de `-1` y periodos con el sensor fuera del tanque. Con f4.2 o f4.3 bajan a 9 en el ESP32 |
| `llegada_agua` | 5 (ESP32, 22 y 28-oct) | subidas de 6-10 % fuera de franja. Los caudales (14-111 L/min) usan la **geometría provisional** |
| `corte_prolongado` | 2 (ESP32, 140 y 167 h) | **huecos de datos**: el ESP32 midió en tandas; el `detalle` lo marca con `posibleHuecoDeDatos` |

Los eventos ya publicados no se reescriben: cada uno lleva su `versionRegla` (f4.1, f4.2 o f4.3). Todos son anteriores
al congelamiento, así que pertenecen al conjunto de diseño.

## Prueba en vivo con el ESP32

_Pendiente: cuando el HC-SR04 funcione, se cargan las medidas del recipiente en `ingesta/tanques.yml`, se llena y
vacía anotando cada acción en la bitácora, y se registran `llegada_agua` y `autonomia_baja` en Kafbat y en el
consumidor. Es el primer dato de rendimiento real de las reglas congeladas._

## Operación

```bash
# El detector corre dentro del job de Spark (5.ª consulta)
docker compose -f infra/compose.yml --env-file .env up -d --build aqua-streaming aqua-consumidor-eventos
docker logs -f lambda26-aqua-consumidor-eventos            # eventos en vivo, legibles

# Releer todo el tópico (otro grupo; termina tras 10 s sin eventos)
docker run --rm --network lambda26-kafka-net -e KAFKA_BOOTSTRAP=kafka:9092 monitoraqua-ingesta:latest \
  python consumidor_eventos.py --desde-inicio --salir-tras-s 10

# Validación con datos aumentados y evaluación
docker exec -w /app/streaming lambda26-aqua-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 validar_detector.py [--sin-compensacion]
.venv/Scripts/python streaming/evaluar_detector.py data/local/validacion/eventos_con_compensacion.jsonl
```

Al cambiar `ingesta/tanques.yml` (por ejemplo, con las medidas reales del tanque del ESP32) o
`streaming/reglas_servicio.yml`, el detector aplica los valores nuevos en el siguiente lote.

## Limitaciones conocidas

- **Corregida en f4.4 (se conserva como historia de f4.3).** `llegada_agua` falsas por el sensor intermitente,
  después del congelamiento de f4.3. Con las reglas f4.3 ya
  congeladas, el HC-SR04 intermitente del ESP32 (lecturas válidas muy ruidosas alternadas con `-1`) produjo dos
  `llegada_agua` falsas: 28-oct **23:55:04** (subida 9.8 %) y 29-oct **00:27:14** (subida 7.4 %), las dos por el
  criterio rápido, más un `falla_sensor` a las 00:00:39.
  - **Por qué pasan:** la mediana de 5 y la confirmación de 3 medianas no alcanzan cuando el ruido del sensor
    degradado es ~25 veces el del perfil real.
  - **Las reglas congeladas no se cambian.** Si esto se corrige, será con una versión **f4.4**, un congelamiento
    nuevo (fecha, hora y commit) y un periodo de evaluación nuevo que empieza en ese congelamiento. El rendimiento
    de f4.3 se sigue reportando con las reglas f4.3.
  - **Efecto en el rendimiento real:** ninguno por ahora. Mientras la geometría del ESP32 sea provisional
    (`por_medir`), no hay periodo de rendimiento real para ese sensor. Cuando lo haya, eventos como estos se
    cuentan como falsos positivos si la bitácora no registra una acción a esa hora.

- **f4.4: llenado durante la inestabilidad.** Si el HC-SR04 queda inestable justo mientras el tanque se llena,
  ese llenado no se detecta: al calmarse, las referencias se reinician con el tanque ya lleno. Con ruido ×3 son el
  1-2 % de las llegadas.
- **f4.4: tras un hueco de datos no se sabe qué pasó.** Una llegada ocurrida con el ESP32 apagado no se reporta: el
  corte cuenta desde la reconexión (`referencia = reanudacion`), y no desde la última llegada vista.
- **f4.4: subida sostenida en el tanque de lluvia.** En 3418755 las llegadas pasan de 49 a 65, y son lluvias. Está
  fuera del alcance de D4 (`batch/modelo_d4.yml` → `fuera_de_alcance`).

- **Zona ciega del sensor.** El JSN-SR04T/AJ-SR04M no mide bien a menos de ~20-25 cm, y la geometría provisional
  pone el sensor a ~4 cm del rebose. Con el tanque lleno, las lecturas caen en la zona ciega. Al cargar las medidas
  reales, la sonda debe quedar al menos 25 cm sobre el nivel máximo.
- **Eventos f4.3 de la mañana del 29-oct.** Entre las 11:05 y las 12:30, f4.3 publicó 5 eventos con el ESP32
  manipulado (nivel −7.5 %, subidas de 18-53 %). Son anteriores al congelamiento de f4.4: conjunto de diseño.
- **Las capas oro y la versión de reglas.** `batch/capas.py` recalcula `d4_llegadas` con las reglas vigentes. La
  regeneración del 29-oct se hizo antes del congelamiento (con f4.3) y es la entrada de d4.1; la próxima, con f4.4,
  será la entrada de d4.2.

## Pendientes

- **Medidas reales del tanque del ESP32.** `tanques.yml` tiene valores provisionales (`por_medir: true`); los
  caudales y litros reales dependen de ellas.
- **`alerta_fuga`:** con fugas persistentes (generador f2b.2) la consistencia sube a 0.86 / 0.75, pero con 4 episodios: hacen falta más semillas f2b.2 para un IC útil. Es además la regla más sensible al ruido: con ruido ×3, 2 de cada 3 alertas son falsas.
- **Rendimiento real:** todavía no hay ninguna acción en el periodo de rendimiento real. Empieza cuando se carguen
  las medidas reales del recipiente (`geometria_desde`). Las cifras con IC útil requieren varias pruebas anotadas por una persona.
- **`umbral_termico`:** se reemplaza el modelo provisional por el de F6.
