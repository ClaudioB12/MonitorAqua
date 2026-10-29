# Modelo D2: horas de autonomía del tanque

**Dimensión D2 (Claudio Bustinza).** Versión `d2.1`, entrenada el 2026-10-29 con la partición `p6.1`. Código:
`batch/modelo_d2.py` (configuración en `batch/modelo_d2.yml`) y `batch/holt.py`. Evidencia: `docs/evidencias/f6-d2/`.

## 1. Qué pregunta responde y qué decisión habilita

> **¿Cuántas horas le quedan al tanque antes de quedar casi vacío, si se sigue gastando como en las últimas horas?**

"Casi vacío" es un umbral configurable; por defecto, el **10 %** del nivel.

La respuesta sirve para decidir **a tiempo**:
- **Si la autonomía es baja y no se espera agua pronto** (eso lo dice D4): avisar a la familia o al administrador
  para que raciones, pidas una cisterna o llenes el tanque mientras hay servicio.
- **Si la autonomía es alta:** no hay que hacer nada.

En F7, este modelo correrá en vivo y alimentará el indicador del tablero y el evento `autonomia_baja`.

## 2. Datos

- **Tabla:** `d2_autonomia` de la capa oro de F6. Es el nivel en % cada 5 min (la mediana de cada bloque), cortado
  en tramos continuos y sin rellenar huecos.
- **Tres niveles de datos**, que se reportan siempre por separado:

| Nivel | Qué es | Para qué sirve aquí |
|---|---|---|
| Consistencia | Flota extrapolada (datos aumentados, f2b.1 y f2b.2): 4 tanques × 2 corridas × 30 días | Entrenar y comprobar que el modelo funciona donde la verdad se conoce. **No es métrica final** |
| ThingSpeak real | 5 tanques reales con nivel (canales 1233810, 1233815, 283977, 480876 y 3418755) | La métrica real con la que se juzga el modelo |
| ESP32 real | El tanque del equipo | Métrica final **pendiente** (sección 7) |

- **Snapshot:** las fuentes se descargaron el 2026-10-29 a las 10:35 de Lima, para incluir la noche del ESP32
  (`docs/evidencias/f6-d2/manifiesto_fuentes_d2.json`).

## 3. Cómo se calculan las horas de autonomía

**La idea es una regla de tres:** si el tanque está al 60 % y baja 4 puntos por hora, llegar al 10 % le toma
(60 − 10) / 4 = **12.5 horas**.

```
horas de autonomía = (nivel actual − umbral) / velocidad de bajada (% por hora)
```

- **Si el nivel no está bajando** (de noche, por ejemplo, nadie usa agua), la fórmula no tiene sentido. El modelo
  responde **"sin descenso previsto"**, que se trata como "más de 72 h".
- **Cada modelo estima la velocidad de bajada a su manera.** Esa es la diferencia entre ellos (sección 4).

### Qué es un caso censurado

Para saber si el modelo acertó hay que ver **cuándo llegó de verdad** el tanque al 10 %. Pero casi siempre llega
agua antes: el tanque se rellena y **nunca llegamos a ver cuándo se habría vaciado**. Ese caso está **censurado**:
sabemos que la autonomía era *al menos* hasta el llenado, pero no cuánto.

- **Cómo se detecta un llenado:** una subida sostenida de más de 10 puntos del nivel, sobre la mediana de 15 min.
  Con ese criterio, en la flota salen las mismas llegadas que detecta el detector congelado de F4 (27-29 por tanque
  en 30 días), y en ThingSpeak 1233810, ~1 por día.
- **Episodio de descenso:** el tramo entre un llenado y el siguiente.
- **Entrenamiento:** Holt y GBT aprenden a seguir el **nivel**, no la autonomía. Los casos censurados no ensucian el
  aprendizaje, porque el nivel sí se observa siempre. Solo la elección de los parámetros de Holt usa casos de
  autonomía, y únicamente los no censurados del entrenamiento.
- **Evaluación:** el error en horas se mide **solo en los casos no censurados**, donde el tanque realmente bajó
  hasta el umbral. Siempre se informa cuántos episodios hubo y cuántos fueron censurados.
- **¿Por qué no se cuentan los censurados como error?** Sería inventar una verdad que no se observó.

## 4. Los modelos

### Holt (principal)

Holt es un **promedio que se actualiza con cada lectura y recuerda dos cosas**: el nivel actual "limpio" (sin el
ruido del sensor) y la **velocidad** a la que viene cambiando. Tiene dos perillas:

- **alpha:** cuánto se cree en la lectura nueva frente a lo que ya se sabía.
- **beta:** qué tan rápido se cambia de opinión sobre la velocidad. Un beta chico = una velocidad con memoria larga.

**Por qué Holt:**
- **Da justo lo que necesita la autonomía:** un nivel y una velocidad. Con la regla de tres de arriba sale la
  respuesta.
- **Es liviano:** guarda dos números por tanque y se actualiza con cada lectura. Encaja con el streaming de F7, y
  `batch/holt.py` se usará allí tal cual.
- **Es explicable:** se puede mostrar en el tablero de dónde sale cada número.

**Un hallazgo del ajuste:** las perillas se eligen de dos formas, siempre solo con datos de entrenamiento, y las dos
chocan.

| Ajustado para… | Flota | ThingSpeak real | Qué pasa |
|---|---|---|---|
| Pronosticar el nivel a 15-60 min (`holt_pronostico`) | alpha 0.7 · beta 0.4 | alpha 1.0 · beta 0.0 | En ThingSpeak, **se convierte en persistencia**: velocidad siempre cero. Entonces nunca prevé una bajada y en autonomía dice "sin descenso" en el 95 % de los casos |
| Estimar la autonomía (`holt`, **el principal**) | alpha 1.0 · beta 0.03 | alpha 0.1 · beta 0.1 | Velocidad con memoria larga: pronostica peor el nivel a 15-60 min, pero sirve para la pregunta de D2 |

Por eso el modelo principal se ajusta con el objetivo de la dimensión, las horas de autonomía, y el otro queda solo
como comparación. Ese ajuste usa los casos no censurados del entrenamiento: 1,726 orígenes en la flota y 394 en
ThingSpeak.

### GBT de Spark MLlib (comparación)

`GBTRegressor` es un conjunto de **árboles de decisión** entrenados uno tras otro, cada uno corrigiendo los errores
del anterior. Pronostica **cuánto cambiará el nivel** en 15 y en 60 min a partir de:
- las lecturas recientes y sus diferencias;
- las medias móviles;
- la hora del día y el día de la semana;
- las horas desde el último llenado;
- la tendencia de las últimas 3 h.

**Por qué GBT:** ve cosas que Holt no ve, como que a cierta hora siempre se gasta más. Además es el modelo de
MLlib que exige la rúbrica (criterio 7). Para la autonomía se usa su pronóstico a 60 min como velocidad de bajada.

Variables más importantes según el propio modelo:
- **Flota:** la hora del día y el último cambio.
- **ThingSpeak:** el nivel actual, la variabilidad reciente y los rezagos.

### Líneas base (obligatorias, de `batch/evaluacion.py`)

| | Pronóstico del nivel | Autonomía |
|---|---|---|
| **Persistencia** | El nivel dentro de 15 o 60 min será el de ahora | La velocidad de bajada es la de la última hora |
| **Tendencia lineal** | Se prolonga la recta de la última hora | La velocidad es la pendiente de esa recta |

## 5. Cómo se evitó mirar la prueba

- **Partición `p6.1`, siempre temporal:**
  - **ThingSpeak:** la prueba son los últimos 7 días de cada canal, con un día de **embargo** antes.
  - **Flota:** la consistencia son los últimos ~8 días de cada corrida.
- **Ajuste de hiperparámetros:** solo con la partición de entrenamiento. Su último 20 % se separó como conjunto de
  ajuste para elegir. Después se reentrenó con todo el entrenamiento y **la prueba se miró una sola vez**, al final.
- **Intervalos de confianza:**
  - **Pronóstico:** bootstrap por **bloques de 1 h**, porque lecturas seguidas no son independientes.
  - **Autonomía:** bootstrap por **episodios completos**.

## 6. Resultados

Todas las cifras son del conjunto final (prueba de ThingSpeak y consistencia de la flota), con IC 95 %. Tablas
completas, con RMSE y MAPE, en `docs/evidencias/f6-d2/tablas_d2.md`.

### Pronóstico del nivel (MAE en puntos de %; menor es mejor)

| Nivel | Predictor | 15 min | Mejora vs persistencia | 60 min | Mejora vs persistencia |
|---|---|---|---|---|---|
| Consistencia (n ≈ 18,000) | persistencia | 0.72 [0.65, 0.80] | — | 2.66 [2.43, 2.94] | — |
| | tendencia | 0.99 [0.86, 1.13] | −37 % | 3.22 [2.83, 3.63] | −21 % |
| | holt (principal) | 0.81 [0.73, 0.88] | −12 % | 3.10 [2.84, 3.40] | −16 % |
| | holt_pronostico | 0.64 [0.56, 0.72] | **+12 %** | 2.93 [2.57, 3.30] | −10 % |
| | **gbt** | **0.41 [0.37, 0.45]** | **+43 %** | **1.84 [1.67, 2.06]** | **+31 %** |
| ThingSpeak real (n 3,468 / 4,290) | **persistencia** | **0.78 [0.58, 0.99]** | — | **1.13 [0.93, 1.36]** | — |
| | tendencia | 1.03 [0.80, 1.29] | −32 % | 1.99 [1.65, 2.39] | −76 % |
| | holt (principal) | 1.18 [0.96, 1.45] | −53 % | 1.77 [1.48, 2.10] | −57 % |
| | holt_pronostico | 0.78 [0.58, 0.99] | 0 % (es la persistencia) | 1.13 [0.93, 1.36] | 0 % |
| | gbt | 1.00 [0.77, 1.25] | −28 % | 1.30 [1.10, 1.53] | −15 % |
| ESP32 real | — | **pendiente** | | **pendiente** | |

### Horas de autonomía hasta el 10 % (MAE en horas; solo casos no censurados)

| Nivel | Episodios | Llegan al 10 % | Censurados | Predictor | MAE h | Sin descenso previsto |
|---|---|---|---|---|---|---|
| Consistencia | 83 | **3** | 80 | persistencia | 13.8 [8.1, 15.8] | 20 % |
| | | | | tendencia | 14.0 [8.3, 16.5] | 20 % |
| | | | | **holt (principal)** | **12.4 [8.7, 15.2]** | 22 % |
| | | | | holt_pronostico | 14.7 [8.3, 16.5] | 21 % |
| | | | | gbt | 23.6 [16.1, 25.6] | 37 % |
| ThingSpeak real | 137 | **2** | 135 | persistencia | **29.6 [24.4, 38.5]** | 38 % |
| | | | | tendencia | **29.4 [25.3, 36.5]** | 39 % |
| | | | | holt (principal) | 35.6 [26.6, 50.9] | 46 % |
| | | | | holt_pronostico | 64.1 [62.9, 66.2] | 95 % |
| | | | | gbt | 46.6 [38.8, 59.9] | 62 % |
| ESP32 real | — | — | — | **pendiente** | | |

**Con umbral del 25 %** (sensibilidad): en la flota se repite el orden (holt 13.2 h, persistencia 14.2 h, tendencia
14.2 h). En la prueba de ThingSpeak **ningún episodio llega al 25 %**, así que no hay casos no censurados.

### Cómo leer estos resultados

1. **En datos reales, nada le gana a la persistencia para pronosticar el nivel a 15-60 min.** En una hora el nivel
   de un tanque casi no cambia, salvo cuando llega agua, y eso ninguna tendencia lo anticipa. Es un resultado
   conocido en series de corto plazo, y por eso la persistencia es una línea base obligatoria.
2. **El GBT gana con holgura en la flota (+43 % y +31 %), pero pierde en ThingSpeak.** En los datos aumentados
   aprende el ciclo diario del generador. En los tanques reales ese patrón no se repite igual, y menos en la semana
   de prueba. Es la razón por la que la flota se llama "consistencia" y no se usa como métrica final.
3. **En autonomía, Holt es el mejor en la flota (12.4 h frente a 13.8 h de la persistencia), pero con solo 3
   episodios no censurados los intervalos se solapan.** No se puede afirmar que sea mejor. En ThingSpeak gana la
   persistencia de la velocidad (29.6 h frente a 35.6 h), sobre **2 episodios**.
4. **El error de autonomía es grande (12-35 h) por una razón clara: el consumo es intermitente.** En el 20-46 % de
   los momentos el tanque no está bajando (de noche, por ejemplo), y cualquier método de tendencia de corto plazo
   responde "sin descenso". Cuando se reanuda el consumo, el tanque se vacía mucho antes de lo que decía esa
   respuesta.
5. **El 10 % casi nunca se alcanza:** 3 de 83 episodios en la flota y 2 de 137 en ThingSpeak. Los tanques se
   rellenan antes. La evaluación de la autonomía descansa en muy pocos episodios, y eso es lo primero que hay que
   decir al presentarla.

**Conclusión:** el modelo queda **entrenado, evaluado sin trampas y listo para F7**, pero en datos reales **no supera
a la línea base**. Se reporta tal cual. La mejora natural para una versión `d2.2` es estimar la velocidad de
consumo con una memoria de un día o con el perfil de consumo por hora del día, en vez de la última hora. Así se
evita el "sin descenso" nocturno. No se implementó en esta fase.

## 7. ESP32: métrica final pendiente

- **Por qué está pendiente:** la geometría del tanque del ESP32 es **provisional** en `ingesta/tanques.yml`
  (`por_medir: true`, `geometria_desde: null`). La partición `p6.1` solo abre el periodo de prueba del ESP32 desde
  la hora en que rija la geometría medida (y después del congelamiento de F4). Sin periodo de prueba no hay
  métrica final, y no se inventa una.
- **El tramo nocturno del 29-oct** (01:40-08:30, 6.8 h continuas) es el primero del ESP32 que alcanza para un
  horizonte de 1 h. Sirve como **diseño**: con los parámetros de ThingSpeak, la persistencia da MAE 0.20 % a 15 min
  y 0.48 % a 60 min. El nivel casi no se movió, porque de noche no hubo consumo.
- **Para cerrar la métrica:**
  - Medir el tanque y fijar `geometria_desde`.
  - Acumular sesiones del ESP32 con consumo y llenados.
  - Volver a correr `batch/modelo_d2.py` para obtener la métrica final.

## 8. Limitaciones

- **ESP32:**
  - Geometría provisional, así que los % y litros son aproximados.
  - Sesiones cortas e intermitentes: hasta el 29-oct, ningún tramo superaba las 2.4 h.
  - Sin periodo de prueba.
- **DHT11 (±2 °C):** la compensación térmica de la distancia tiene una incertidumbre de ~±0.36 % de la distancia.
  En pronósticos de pocas décimas de punto, eso es del mismo orden que el error.
- **ThingSpeak es de otro país:** son tanques de terceros, con otros hábitos de consumo, tamaños y sensores. Sirven
  para probar el **método** con datos reales, no para afirmar cuánto acertará en Juliaca.
- **Muestreo de ThingSpeak 1233810 (cada ~10 min):** en la rejilla de 5 min no tiene observación a 15 min exactos.
  Entra en la evaluación a 60 min y en autonomía, pero no a 15 min.
- **Pocos episodios no censurados:** 3 en la flota y 2 en ThingSpeak. Los intervalos son anchos y un solo
  episodio pesa mucho.
- **Snapshot:** volver a descargar las fuentes mueve la semana de prueba de ThingSpeak. Las cifras valen para el
  snapshot guardado en la evidencia.

## 9. Uso en F7 (streaming)

- **Holt:**
  - `batch/holt.py`: la clase `Holt`, con `actualizar(ts, nivel)`, `pronostico(minutos)` y
    `horas_autonomia(umbral)`. Su `estado()` es serializable para guardarlo entre micro-lotes.
  - Parámetros en `batch/modelos/d2/d2.1-20261029/holt.json`. Los del ESP32 son los de ThingSpeak real.
  - Se alimenta con la mediana del nivel en bloques de 5 min, como la tabla oro.
- **GBT:** `batch/modelos/d2/d2.1-20261029/gbt_h15_*` y `gbt_h60_*`. Son modelos de Spark que no se versionan y se
  regeneran con el script. Las variables y su orden están en `manifiesto.json`.

## 10. Preguntas probables del jurado

**1. "Su modelo principal pierde contra la persistencia en datos reales. ¿Para qué sirve?"**
A 15-60 min, el nivel de un tanque casi no cambia, y la persistencia es muy difícil de vencer: por eso es una línea
base obligatoria. Lo reportamos tal cual. La pregunta de D2 no es el nivel en una hora, sino **cuántas horas
quedan**. Para eso se necesita una velocidad de bajada, que la persistencia del nivel no da. Lo que mostramos es
que el método funciona de punta a punta, que no hicimos trampa con la prueba, y dónde falla: el consumo
intermitente. Ese es el siguiente paso, con una velocidad de consumo de memoria más larga.

**2. "¿Qué es un caso censurado y por qué lo dejan fuera?"**
Es cuando llega agua antes de que el tanque se vacíe. Sabemos que el tanque aguantó al menos hasta el llenado,
pero no cuándo se habría vaciado. Si lo contáramos como error, estaríamos inventando una verdad. Por eso medimos
el error solo donde el tanque llegó de verdad al umbral, e informamos siempre cuántos casos fueron censurados:
80 de 83 en la flota y 135 de 137 en ThingSpeak.

**3. "Si el GBT pronostica mejor en la flota, ¿por qué el principal es Holt?"**
Porque el GBT gana en los datos aumentados, que tienen un ciclo diario que aprende, pero pierde en los tanques
reales. Para la autonomía es el peor (23.6 h y 46.6 h), porque su velocidad a 1 h es ruidosa. Holt guarda dos
números por tanque, se actualiza con cada lectura en streaming y se puede explicar en el tablero.

**4. "¿Cómo saben que no hay fuga de información entre entrenamiento y prueba?"**
La separación es temporal, nunca aleatoria, y se define una sola vez para los cuatro modelos (`p6.1`).
- Hay un día de embargo entre el entrenamiento y la prueba.
- Los hiperparámetros se eligieron solo con el último 20 % del entrenamiento.
- La prueba se usó una vez, al final.
- La prueba del ESP32 ni siquiera existe todavía.

Los intervalos usan bootstrap por bloques y por episodios, porque las lecturas seguidas no son independientes.

**5. "¿Qué tan confiable es esto para su propio tanque?"**
Todavía no lo sabemos, y no lo inventamos. La geometría del tanque es provisional, así que no hay periodo de prueba
del ESP32. El tramo nocturno de 6.8 h ya permite evaluar a 1 hora, pero como diseño. La métrica final llega cuando
midamos el tanque y juntemos sesiones con consumo y llenados.

## 11. Reproducir

```bash
# Fuentes y capas (si se quiere un snapshot nuevo; mueve la prueba de ThingSpeak)
docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/batch monitoraqua-ingesta:latest python descargar_fuentes.py
docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/streaming:/repo/batch monitoraqua-streaming:latest \
    spark-submit --driver-memory 3g batch/capas.py
# Modelo D2 (~12 min)
docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming:latest \
    spark-submit --driver-memory 3g batch/modelo_d2.py
```
