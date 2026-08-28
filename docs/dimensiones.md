# Dimensiones del proyecto

## Regla de asignación

Según la plantilla del curso, cada integrante desarrolla **dos dimensiones** del proyecto, registradas en **dos filas consecutivas** de la tabla de asignación:

- **U1 — Predictiva sin tiempo real (batch):** la pregunta se responde sobre el histórico acumulado, con un modelo de MLlib que corre una sola vez al ejecutar el pipeline y entrega su salida en un notebook.
- **U2 — Predictiva con tiempo real (streaming):** la misma línea de pregunta se responde en vivo, dato por dato, con inferencia de series de tiempo dentro de Spark Structured Streaming y salida en un panel de Grafana.

La pregunta central contiene dos variables de interés: **disponibilidad** (cuánta agua hay, cuándo llega, cuánta se pierde) y **calidad** (en qué estado está el agua almacenada). Cada integrante toma un aspecto de una de esas dos variables y lo trabaja en sus dos dimensiones, U1 y U2. Todas convergen en un solo tablero de Grafana: **"Salud del servicio de agua — Juliaca"**, con cuatro grupos de paneles, uno por integrante.

## Tabla de asignación

| Integrante | Tipo | Dimensión (como pregunta) | Indicador | Fuente/instrumento batch | Fuente/instrumento streaming |
|---|---|---|---|---|---|
| Integrante A | Predictiva (U1, batch) | ¿Cuál fue el consumo diario por hogar y por zona, y cuántos días de autonomía se pueden esperar según tamaño del hogar, capacidad del tanque y zona? | Litros/día por hogar; días de autonomía estimados por regresión | Histórico de nivel de los nodos + INEI (tamaño de hogar) + SUNASS (continuidad) | (mismo instrumento, aquí no se procesa en vivo) |
| Integrante A | Predictiva (U2, streaming) | ¿Cuántas horas de agua le quedan a este tanque según la tendencia de consumo de las últimas horas? | Horas de autonomía proyectadas, por tanque | (mismo histórico, para entrenar) | Nivel compensado en vivo de cada nodo |
| Integrante B | Predictiva (U1, batch) | ¿En qué franjas horarias llegó el agua a cada zona, y qué probabilidad hay de que una zona reciba agua en una franja dada según el día de la semana? | Horas de servicio por día y zona; probabilidad de llegada por franja (clasificación) | SUNASS continuidad histórica + eventos de llenado detectados en el histórico de nodos | (mismo instrumento, aquí no se procesa en vivo) |
| Integrante B | Predictiva (U2, streaming) | ¿A qué hora se espera la próxima llegada de agua a esta zona, según la presión de línea y el patrón reciente? | Hora estimada de próxima llegada; presión de línea actual | (mismo histórico, para entrenar) | Presión y caudal en vivo del nodo maestro |
| Integrante C | Predictiva (U1, batch) | ¿Qué viviendas presentan pérdida por fuga y de qué severidad, según su caudal mínimo nocturno y su perfil de consumo? | Litros perdidos/mes; clase de fuga (sin fuga / leve / severa) | Histórico de nodos + SUNASS agua no facturada como tasa base | (mismo instrumento, aquí no se procesa en vivo) |
| Integrante C | Predictiva (U2, streaming) | ¿Cuánto volumen se habrá perdido al amanecer si la deriva actual del nivel continúa? | Pérdida acumulada proyectada, próximas horas | (mismo histórico, para entrenar) | Divergencia en vivo entre nivel esperado sin consumo y nivel observado |
| Integrante D | Predictiva (U1, batch) | ¿Cómo evolucionó la temperatura del agua almacenada y su tiempo de residencia, y qué temperatura mínima nocturna se puede esperar según la temperatura ambiente y la capacidad del tanque? | Temperatura mínima del agua; horas de residencia; temperatura proyectada por regresión | Histórico de nodos + SENAMHI Juliaca (mínimas y heladas) | (mismo instrumento, aquí no se procesa en vivo) |
| Integrante D | Predictiva (U2, streaming) | ¿Cuándo se espera que la temperatura del agua cruce el umbral de riesgo y cuánto tiempo lleva almacenada el agua actual? | Temperatura proyectada próximas horas; horas de residencia acumuladas; ORP actual | (mismo histórico, para entrenar) | Temperatura de agua y ORP en vivo del nodo maestro |

## Fichas por dimensión

### Dimensión 1 (U1) — Consumo y autonomía del hogar (Integrante A)

- **Tipo:** Predictiva sin tiempo real (U1, batch)
- **Dimensión:** ¿Cuál fue el consumo diario de agua por hogar y por zona de Juliaca, y cuántos días de autonomía se pueden esperar en función del tamaño del hogar, la capacidad del tanque y la zona? Aborda directamente el componente de *disponibilidad* de la pregunta central: cuánta agua realmente usa un hogar y cuánto le dura.
- **Indicadores:** litros/día por hogar; litros/día per cápita por zona; **días de autonomía estimados por un modelo de regresión** (variables: tamaño del hogar, capacidad del tanque, zona, continuidad del sector, mes).
- **Decisión que habilita:** la familia sabe si su tanque está bien dimensionado para su hogar; la EPS identifica qué zonas tienen menor autonomía y priorizarlas.
- **Instrumento/fuente batch:** acumulación histórica de las series de nivel compensado de los 8 nodos (`device_id`, `timestamp`, `zona`, `nivel_cm_comp`, `litros`), cruzada con INEI (tamaño de hogar por distrito de Juliaca) y SUNASS (continuidad de Seda Juliaca por año).
- **Instrumento/fuente streaming:** mismo instrumento que su U2, aquí en modo acumulación histórica, no procesado en vivo.
- **Modelo predictivo:** regresión (Random Forest Regressor o GBT en Spark MLlib). Corre una sola vez al ejecutar el pipeline.
- **Salida:** notebook Jupyter/PySpark con tabla de consumo por zona, gráfico de distribución de autonomía y la predicción del modelo con su métrica de error.
- **Se combina con:** dimensión 3 (llegada del agua determina cuánto puede reponer un hogar) y dimensión 5 (una fuga reduce la autonomía real).
- **Requisitos:**
    1. El sistema debe calcular el consumo diario por hogar descontando los eventos de llenado.
    2. El sistema debe estimar los días de autonomía de un hogar a partir de sus atributos y su zona.
    3. El sistema debe reportar el error del modelo sobre un conjunto de prueba separado.

---

### Dimensión 2 (U2) — Autonomía proyectada en vivo (Integrante A)

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión:** ¿Cuántas horas de agua le quedan a este tanque según la tendencia de consumo observada en las últimas horas? Es la versión en vivo de la dimensión 1: la misma pregunta de autonomía, pero respondida dato por dato.
- **Indicadores:** nivel actual en litros y porcentaje; **horas de autonomía proyectadas**, actualizadas con cada lectura.
- **Decisión que habilita:** la familia decide si racionar o esperar; recibe aviso con horas de anticipación en lugar de descubrirlo al abrir el caño.
- **Instrumento/fuente batch:** el mismo histórico de la dimensión 1, usado para entrenar el modelo.
- **Instrumento/fuente streaming:** tópico Kafka `tanques.nivel`, alimentado vía MQTT desde los nodos ESP32. Esquema: `device_id`, `timestamp`, `zona`, `nivel_cm_comp`, `litros`, `bateria_pct`.
- **Modelo predictivo:** series de tiempo sobre el nivel (suavizado exponencial o regresión sobre ventana deslizante), corriendo **en vivo** dentro de Spark Structured Streaming, aplicando inferencia a cada lectura que llega.
- **Salida:** panel en el Grafana común con dos series superpuestas, nivel actual y horas restantes proyectadas, más alerta cuando la proyección baje de 12 horas.
- **Se combina con:** dimensión 4 (si se sabe cuándo llega el agua, la autonomía proyectada cambia de significado).
- **Requisitos:**
    1. El sistema debe actualizar la proyección de autonomía con cada mensaje recibido.
    2. El sistema debe emitir una alerta cuando la autonomía proyectada caiga bajo un umbral configurable.
    3. El sistema debe descartar lecturas físicamente imposibles antes de alimentar el modelo.

---

### Dimensión 3 (U1) — Franjas de llegada del agua por zona (Integrante B)

- **Tipo:** Predictiva sin tiempo real (U1, batch)
- **Dimensión:** ¿En qué franjas horarias y con qué frecuencia llegó el agua a cada zona de Juliaca, y qué probabilidad hay de que una zona reciba agua en una franja dada según el día de la semana y el histórico de continuidad? Responde al componente de *cuándo* de la disponibilidad.
- **Indicadores:** horas de servicio por día y por zona; número de eventos de llenado por semana; **probabilidad de llegada por franja horaria estimada por un modelo de clasificación**.
- **Decisión que habilita:** la familia sabe en qué franja conviene estar disponible para abrir válvulas o llenar recipientes; la EPS contrasta la continuidad declarada contra la observada en campo.
- **Instrumento/fuente batch:** SUNASS Benchmarking Regulatorio de Seda Juliaca (continuidad en h/día, presión, agua no facturada, series anuales), cruzado con los eventos de llenado detectados en el histórico de nodos (todo aumento de nivel superior a un umbral marca un evento de llegada, con su hora y duración).
- **Instrumento/fuente streaming:** mismo instrumento que su U2, aquí en modo acumulación histórica.
- **Modelo predictivo:** clasificación (regresión logística o Random Forest Classifier en MLlib), etiqueta = llegó agua en la franja sí/no. Corre una sola vez.
- **Salida:** notebook con mapa de calor zona × franja horaria, comparación entre continuidad declarada por SUNASS y observada, y matriz de confusión del clasificador.
- **Se combina con:** dimensión 1 (la continuidad de la zona es variable predictora de la autonomía) y dimensión 7 (más horas sin reposición implican mayor tiempo de residencia).
- **Requisitos:**
    1. El sistema debe detectar automáticamente los eventos de llegada de agua a partir de la serie de nivel.
    2. El sistema debe estimar la probabilidad de llegada por zona y franja horaria.
    3. El sistema debe contrastar la continuidad observada contra la reportada por SUNASS.

---

### Dimensión 4 (U2) — Próxima llegada de agua en vivo (Integrante B)

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión:** ¿A qué hora se espera la próxima llegada de agua a esta zona, según la presión de línea actual y el patrón reciente de suministro? Versión en vivo de la dimensión 3.
- **Indicadores:** presión de línea actual en kPa; caudal de entrada en L/min; **hora estimada de próxima llegada de agua**, actualizada continuamente.
- **Decisión que habilita:** la familia se organiza sabiendo si el agua llega en dos horas o en diez; la EPS detecta un sector que no recibió agua cuando debía.
- **Instrumento/fuente batch:** el mismo histórico de presión y eventos de llenado, usado para entrenar.
- **Instrumento/fuente streaming:** tópico Kafka `tanques.red`, con `device_id`, `timestamp`, `zona`, `presion_linea_kpa`, `caudal_lpm`. Proviene del nodo maestro; los nodos simples aportan la señal indirecta del aumento de nivel.
- **Modelo predictivo:** series de tiempo sobre la señal de presión y el intervalo entre llegadas, corriendo **en vivo** en Structured Streaming.
- **Salida:** panel en el Grafana común con presión de línea en vivo y cuenta regresiva estimada hasta la próxima llegada, con alerta si el sector supera su intervalo típico sin recibir agua.
- **Se combina con:** dimensión 2 (autonomía proyectada contra llegada proyectada es la información que realmente le sirve a la familia).
- **Requisitos:**
    1. El sistema debe registrar presión de línea de forma continua en el nodo maestro.
    2. El sistema debe proyectar en vivo la hora de la próxima llegada por zona.
    3. El sistema debe alertar cuando un sector exceda su intervalo típico entre llegadas.

---

### Dimensión 5 (U1) — Detección y severidad de fugas (Integrante C)

- **Tipo:** Predictiva sin tiempo real (U1, batch)
- **Dimensión:** ¿Qué viviendas presentan pérdida de agua por fuga y de qué severidad, según su caudal mínimo nocturno y su perfil de consumo? Es el componente de *cuánta agua se pierde sin que nadie lo note* dentro de la disponibilidad.
- **Indicadores:** caudal mínimo nocturno por vivienda (L/h entre 2:00 y 5:00); litros perdidos por mes; **clase de fuga estimada por un modelo de clasificación**: sin fuga, fuga leve, fuga severa.
- **Decisión que habilita:** la familia repara un flotador o una tubería antes de que el recibo suba; la EPS focaliza inspección de conexiones clandestinas por sector.
- **Instrumento/fuente batch:** histórico de nivel compensado de los nodos, del cual se deriva el caudal mínimo nocturno como etiqueta (método de mínimo caudal nocturno, estándar en gestión de pérdidas), validado contra el caudalímetro del nodo maestro. Se cruza con el indicador de agua no facturada de SUNASS para Seda Juliaca como tasa base de referencia.
- **Instrumento/fuente streaming:** mismo instrumento que su U2, aquí en modo acumulación histórica.
- **Modelo predictivo:** clasificación multiclase en MLlib. Corre una sola vez.
- **Salida:** notebook con distribución del caudal mínimo nocturno, clasificación por vivienda, volumen perdido estimado por zona y comparación contra el agua no facturada declarada por SUNASS.
- **Se combina con:** dimensión 1 (la fuga reduce la autonomía real) y dimensión 3 (la pérdida se agrava en zonas con menor continuidad).
- **Requisitos:**
    1. El sistema debe calcular el caudal mínimo nocturno por vivienda a partir de la serie compensada.
    2. El sistema debe clasificar cada vivienda en una categoría de severidad de fuga.
    3. El sistema debe estimar el volumen mensual perdido por zona y contrastarlo con el agua no facturada oficial.

---

### Dimensión 6 (U2) — Pérdida proyectada por fuga en vivo (Integrante C)

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión:** ¿Cuánto volumen se habrá perdido al amanecer si la deriva actual del nivel continúa? Versión en vivo de la dimensión 5: no solo detectar que hay fuga, sino proyectar cuánta agua se irá si nadie interviene.
- **Indicadores:** divergencia actual entre nivel esperado sin consumo y nivel observado; **pérdida acumulada proyectada para las próximas horas**.
- **Decisión que habilita:** la familia cierra la llave de paso esa misma noche en lugar de descubrir la pérdida días después.
- **Instrumento/fuente batch:** el mismo histórico, usado para entrenar el modelo de deriva esperada.
- **Instrumento/fuente streaming:** tópico Kafka `tanques.nivel`, con el campo `nivel_cm_comp` ya compensado térmicamente. La compensación es requisito para esta dimensión: sin ella, la contracción térmica nocturna es indistinguible de una fuga lenta.
- **Modelo predictivo:** series de tiempo sobre la señal de divergencia, corriendo **en vivo** en Structured Streaming.
- **Salida:** panel en el Grafana común con la curva de pérdida proyectada y alerta inmediata cuando la proyección supere un umbral en litros.
- **Se combina con:** dimensión 2 (una fuga activa recorta la autonomía proyectada) y dimensión 8 (la compensación térmica es la que hace válida esta señal).
- **Requisitos:**
    1. El sistema debe comparar en vivo el nivel observado contra el nivel esperado sin consumo.
    2. El sistema debe proyectar la pérdida acumulada para las próximas horas.
    3. El sistema debe emitir alerta de fuga solo sobre nivel compensado, nunca sobre la lectura cruda.

---

### Dimensión 7 (U1) — Temperatura y tiempo de residencia del agua (Integrante D)

- **Tipo:** Predictiva sin tiempo real (U1, batch)
- **Dimensión:** ¿Cómo evolucionó la temperatura del agua almacenada y su tiempo de residencia en el tanque, y qué temperatura mínima nocturna se puede esperar según la temperatura ambiente, la hora y la capacidad del tanque? Aborda el componente de *calidad* de la pregunta central.
- **Indicadores:** temperatura mínima diaria del agua; horas de residencia acumuladas por lote de agua; horas al día en rango de riesgo; **temperatura mínima proyectada por un modelo de regresión**.
- **Decisión que habilita:** la familia protege la línea antes de una noche de helada; se identifica qué tanques mantienen agua almacenada demasiado tiempo, lo que compromete el cloro residual.
- **Instrumento/fuente batch:** histórico del nodo maestro (`temp_agua_c`, `temp_aire_c`, `orp_mv`, `nivel_cm_comp`) cruzado con SENAMHI, estación Juliaca: temperatura mínima y máxima diaria, humedad relativa y ocurrencia de heladas, con histórico de varios años.
- **Instrumento/fuente streaming:** mismo instrumento que su U2, aquí en modo acumulación histórica.
- **Modelo predictivo:** regresión en MLlib, variable objetivo temperatura mínima nocturna del agua. Corre una sola vez.
- **Salida:** notebook con series de temperatura de agua contra temperatura ambiente, distribución de tiempos de residencia, decaimiento observado del ORP contra tiempo de residencia, y predicción del modelo.
- **Se combina con:** dimensión 3 (menor continuidad implica mayor tiempo de residencia) y dimensión 8.
- **Requisitos:**
    1. El sistema debe registrar la temperatura del agua y del aire interno de forma continua.
    2. El sistema debe calcular el tiempo de residencia de cada lote de agua a partir de los eventos de llenado.
    3. El sistema debe predecir la temperatura mínima nocturna del agua a partir de la temperatura ambiente pronosticada.

---

### Dimensión 8 (U2) — Riesgo térmico y agua estancada en vivo (Integrante D)

- **Tipo:** Predictiva con tiempo real (U2, streaming)
- **Dimensión:** ¿Cuándo se espera que la temperatura del agua cruce el umbral de riesgo, y cuánto tiempo lleva almacenada el agua que hay ahora en el tanque? Versión en vivo de la dimensión 7.
- **Indicadores:** temperatura actual del agua; ORP actual como proxy de cloro residual; horas de residencia acumuladas; **hora estimada de cruce del umbral de riesgo**.
- **Decisión que habilita:** aviso con anticipación para proteger la línea ante helada, y aviso de que el agua almacenada lleva demasiadas horas sin renovarse.
- **Instrumento/fuente batch:** el mismo histórico, usado para entrenar.
- **Instrumento/fuente streaming:** tópico Kafka `tanques.calidad`, con `device_id`, `timestamp`, `temp_agua_c`, `temp_aire_c`, `hum_aire_pct`, `orp_mv`.
- **Modelo predictivo:** series de tiempo sobre la temperatura del agua, corriendo **en vivo** en Structured Streaming, proyectando el instante de cruce del umbral.
- **Salida:** panel en el Grafana común con temperatura del agua en vivo, curva proyectada, horas de residencia y alerta de riesgo por helada o por agua estancada.
- **Se combina con:** dimensión 6 (la instrumentación térmica que valida la detección de fugas es la misma) y dimensión 4 (una llegada de agua reinicia el contador de residencia).
- **Requisitos:**
    1. El sistema debe proyectar en vivo el instante de cruce del umbral térmico.
    2. El sistema debe reiniciar el contador de residencia al detectar un evento de llenado.
    3. El sistema debe compensar la velocidad del sonido por temperatura y humedad en el propio cabezal de medición, antes de publicar el nivel.

## Alcance

**Qué SÍ cubre este proyecto en conjunto:**

Un observatorio del servicio de agua potable de Juliaca construido desde el punto de almacenamiento domiciliario, que responde en cuatro frentes convergentes: cuánta agua tiene y cuánto le dura a un hogar, cuándo llega el agua a cada zona, cuánta se pierde sin que nadie lo note, y en qué estado está el agua ya almacenada. Cada frente entrega una respuesta histórica con modelo predictivo y una respuesta en vivo con inferencia de series de tiempo, todas visibles en un solo tablero.

**Qué NO cubre, fuera de alcance explícito:**

- No mide calidad microbiológica del agua. El ORP es un proxy de poder oxidante, no un conteo de coliformes ni una medición de cloro libre en mg/L. Su validación contra kit DPD manual queda como trabajo futuro.
- No modela la red de distribución aguas arriba del domicilio. El sistema observa el punto de entrega, no las cámaras, reservorios ni líneas matrices de la EPS.
- No cubre viviendas sin conexión a red que se abastecen por cisterna o pozo.
- No es un sistema de facturación ni sustituye al micromedidor de la EPS.
- No publica datos individualizados por vivienda. La serie de nivel de un tanque revela patrones de ocupación del hogar; toda salida agregada se reporta por zona, y los datos individuales requieren consentimiento firmado del propietario.

## Trazabilidad del simulador

Cada parámetro de los 92 tanques sintéticos debe rastrearse a una medición propia o a un dataset público:

| Parámetro simulado | Origen |
|---|---|
| Perfil de consumo horario | Ajustado sobre los 8 nodos reales |
| Horario de llegada por zona | Continuidad SUNASS, Seda Juliaca |
| Tamaño del hogar | Censo INEI, distrito de Juliaca |
| Capacidad del tanque | Distribución observada en la muestra |
| Ruido del sensor y deriva térmica | Ensayo térmico del nodo maestro |
| Tasa base de fugas | Agua no facturada SUNASS |
| Temperatura ambiente | Serie SENAMHI estación Juliaca |

**Sobre propiedad intelectual.** La protección considerada para el proyecto se limita al registro de software y a la publicación del repositorio.
