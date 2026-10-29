# Adecuación de las dimensiones U2 al hardware disponible

Este documento **complementa** el Brief S2 (`docs/brief.md`, `docs/dimensiones.md`, `dimensiones_equipo_horeb.md`),
que no se modifica. Registra cómo se adecúan las dimensiones de streaming (U2) al hardware que realmente tiene el
equipo. Se tomó antes de F4, porque los eventos de servicio dependen de estas señales.

## Hardware real

El Brief S2 preveía un nodo maestro con JSN-SR04T, BME280, transductor de presión, caudalímetro YF-S201, DS18B20
sumergible y sonda ORP. **El hardware disponible es solo:**

| Sensor | Mide | Rango | Precisión declarada |
|---|---|---|---|
| HC-SR04 | distancia al agua (`distanciaCm`) | 2 a 400 cm | ±3 mm nominal, más la dependencia de la velocidad del sonido con la temperatura (~0.18 %/°C) |
| DHT11 | temperatura y humedad del aire de la cámara (`temperaturaC`, `humedadPct`) | 0 a 50 °C, 20 a 90 % HR | **±2 °C, ±5 % HR**, resolución 1 °C / 1 % |

**No habrá** caudalímetro, transductor de presión, sonda de temperatura del agua ni sonda ORP.

## Tabla Brief S2 → U2

| Dim. | Integrante | Brief S2 (U2) | Adecuación U2 | Indicador U2 | Fuentes | Fuera de alcance |
|---|---|---|---|---|---|---|
| **D2** | Claudio Bustinza | Horas de autonomía según la tendencia del nivel (nivel compensado en vivo) | **Sin cambios** | Horas de autonomía proyectadas | ESP32 · ThingSpeak · flota extrapolada | — |
| **D4** | Wilbert Mayta | Hora de la próxima llegada según la presión y el caudal de línea (YF-S201) | Presión y caudal de línea se reemplazan por **llenados detectados en la serie de nivel** (ESP32 y ThingSpeak) y por el **caudal de llenado estimado** (L/min), calculado con la velocidad de subida del nivel y la geometría del tanque | Hora estimada de la próxima llegada | ESP32 · ThingSpeak · flota extrapolada | Presión de línea |
| **D6** | Joel Huillca | Pérdida proyectada por fuga (divergencia nivel esperado vs. observado, sobre nivel compensado) | **Sin cambios**; la compensación térmica usa el DHT11 | Pérdida acumulada proyectada (L) | ESP32 · ThingSpeak · flota extrapolada (`fuga_lenta`, `deriva_termica`) | — |
| **D8** | Josué Ochoa | Cruce del umbral térmico del agua y residencia, con DS18B20 y ORP | La **temperatura del agua se estima** a partir del aire de la cámara (DHT11) y de Open-Meteo Juliaca, y el modelo se valida con ThingSpeak 3418755. La **residencia** se calcula desde los llenados del ESP32 | Temperatura del agua estimada · hora de cruce del umbral · horas de residencia | ESP32 · Open-Meteo · ThingSpeak 3418755 · flota extrapolada | ORP (proxy de cloro) |

**Environment Agency UK y USGS no sostienen ninguna dimensión.** Sirven para:

- aportar volumen real al pipeline;
- demostrar el watermark de fuentes lentas (llegan con 25 a 54 min de atraso; consulta `lentas_agg` de F3);
- mostrar el escalado a decenas de sensores.

## Detalle por dimensión

### D2: autonomía (sin cambios)

Las horas de autonomía se proyectan con la tendencia de consumo de las últimas horas sobre el nivel.

- **ESP32:** es la serie en vivo.
- **ThingSpeak:** aporta tanques reales con llenados y consumo; el canal 1233810 tiene 67 llenados en dos meses.
- **Flota extrapolada:** sirve para evaluar contra episodios conocidos (`corte_prolongado`).
- **Pendiente:** para pasar de cm a litros hace falta la geometría del tanque del ESP32, que hoy es un supuesto (ver
  `docs/datos-aumentados.md`). El equipo debe medir la altura útil y la capacidad.

### D4: próxima llegada (sin presión ni caudalímetro)

- **Detección del llenado:** una subida sostenida del nivel (la distancia baja), medida sobre medianas por bloque para
  no confundir saltos del sensor con agua que llega. Es el mismo criterio de `batch/perfilar.py`.
- **Caudal de llenado estimado:**

  `Q [L/min] = (Δnivel [cm] / Δt [min]) × (capacidad [L] / altura útil [cm])`

  Esto supone un tanque de sección constante, como los cilíndricos y los rectangulares. Necesita la geometría del
  tanque, igual que D2.
- **Hora de la próxima llegada:** se estima con el patrón de las llegadas detectadas (hora del día, intervalo entre
  llegadas).
- **Limitación:** en todo su historial el ESP32 tiene **una sola subida de nivel** real. El patrón de llegadas se
  aprende con ThingSpeak (1233810 llena cada ~22 h) y con la flota extrapolada (`llegada_atipica`,
  `corte_prolongado`). Para el ESP32 se reporta lo que se observe en vivo.
- **Fuera de alcance:** la presión de línea. Con la serie de nivel solo se ve el agua que entra al tanque, no la que
  llega a la red cuando el tanque ya está lleno.

### D6: fuga (sin cambios)

La compensación de la velocidad del sonido usa la temperatura del aire del DHT11:

`distancia compensada = distanciaCm × c(T) / c(20 °C)`, con `c(T) = 331.3 + 0.606·T`

El DHT11 tiene una precisión de **±2 °C**. Eso deja una incertidumbre de la compensación de ~±0.36 % de la
distancia: ±0.18 cm a 50 cm y ±0.36 cm a 100 cm. Es menor que la deriva que corrige (~0.7 cm en una noche normal de
Juliaca a 50-100 cm), pero del mismo orden que una fuga muy lenta en pocas horas. Hay que declararlo al reportar el
umbral de detección.

Las alertas de fuga se emiten solo sobre la distancia compensada, nunca sobre la cruda, como exige el Brief.

### D8: temperatura del agua y residencia (sin DS18B20 ni ORP)

- **Temperatura del agua estimada:** un modelo de inercia térmica toma la temperatura del aire de la cámara (DHT11)
  como entrada. Open-Meteo Juliaca cubre lo que el DHT11 no ve: el ESP32 solo tiene datos de 15 a 23 h, y Open-Meteo da
  el ciclo completo con las mínimas nocturnas.
- **Validación del modelo:** con ThingSpeak **3418755**, el único canal disponible que mide a la vez el aire dentro
  del tanque y la temperatura real del agua (verificado el 2026-10-29):

  | | |
  |---|---|
  | Historial | **70 días** (2026-08-20 → 2026-10-29), 20,054 lecturas, una cada ~4.6 min; un solo hueco (11-13 sep) |
  | Campos | `field2` aire en el tanque, `field3` temperatura del agua (DS18B20), `field1` nivel del agua (cm); 100 % de cobertura, sin códigos de falla del DS18B20 (`-127`, `85`) |
  | Rangos | aire 7.6-36.8 °C · agua 7.8-20.0 °C · nivel 13-100 cm |
  | Relación aire → agua | correlación 0.70 instantánea y **0.77 con el agua atrasada 5-7 h** respecto del aire |
  | Amplitud diaria (mediana) | aire 8.2 °C · agua 1.2 °C: el agua varía ~15 % de lo que varía el aire |

  Esto permite ajustar un modelo de primer orden: el agua sigue al aire con una constante de tiempo de horas, y el
  nivel del agua entra como volumen térmico. El modelo se valida en días separados del mismo canal.
- **Limitación de la validación:** el canal 3418755 es un tanque de agua de lluvia en el noroeste de Inglaterra
  (53.45, -2.51), con otro clima, otro tamaño y otro aislamiento. La validación muestra que **el método** funciona
  con datos reales. Al aplicarlo en Juliaca, el error se declara como el error de transferencia de ese modelo, no
  como una medición del agua.
- **Residencia:** son las horas desde el último llenado detectado del ESP32 (mismo detector que D4). Con un solo
  llenado real en el historial, la residencia se mide en vivo cuando haya llenados y se evalúa con la flota
  extrapolada.
- **Fuera de alcance:** el ORP como proxy del cloro residual. La calidad se aborda solo por temperatura y residencia.

## Efecto en las fases siguientes

- **F4, eventos de servicio:**
  - `llegada_agua` sale de los llenados detectados.
  - `alerta_fuga` sale de la distancia compensada.
  - `autonomia_baja`, igual que antes.
  - `umbral_termico` usa la **temperatura del agua estimada**.
- **F6, modelos:** se agrega el modelo aire → agua entrenado con ThingSpeak 3418755. Los modelos se evalúan con la
  flota extrapolada, y la métrica final se reporta sobre datos reales (`docs/datos-aumentados.md`).
- **Pendiente para todas las dimensiones:** medir la geometría real del tanque del ESP32.
