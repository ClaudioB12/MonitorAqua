# Datos aumentados: flota extrapolada (F2b)

!!! important "Para qué se usan, y para qué no"
    Los **datos aumentados** son eventos generados por la **flota extrapolada**: tanques simulados con parámetros
    sacados de las lecturas reales. Se usan para dos cosas:

    1. **Volumen**: practicar particionado, procesamiento por lotes y streaming con cientos de miles de eventos.
    2. **Evaluar modelos**: entrenar y medir detectores con anomalías de etiqueta conocida, algo que los datos
       reales no tienen.

    Y tienen dos límites:

    - **La demo en vivo usa solo datos reales** (`aqua-eventos-real`: ESP32 y APIs públicas).
    - **La métrica final de cada modelo se reporta sobre datos reales.** Lo aumentado sirve para desarrollar y
      comparar; la cifra que se sustenta sale de los datos reales.

Los datos aumentados nunca entran a Firebase ni a `aqua-eventos-real`. Viajan por su propio tópico
(`aqua-eventos-aumentados`) y se guardan en Parquet bajo `batch/datos/aumentados/`.

## Identificación en el contrato

`contratos/evento_aqua.schema.json` se amplió de forma compatible: los eventos reales siguen siendo válidos
sin cambios.

| Campo | Datos reales | Datos aumentados |
|---|---|---|
| `origen` | `esp32-fisico`, `thingspeak`, `ea-uk`, `usgs`, `open-meteo` | `flota-extrapolada` |
| `fuenteTipo` | `iot-propio`, `api-publica` (sin cambios) | `aumentado` |
| `runId` | — | obligatorio: ejecución del generador |
| `esAnomalia` | — | obligatorio (booleano) |
| `tipoAnomalia` | — | obligatorio: tipo del catálogo, o `null` |
| `episodioId` | — | agrupa los eventos de un episodio; `null` si es normal o una falla puntual |
| `distanciaRealCm` | — | distancia verdadera, sin ruido ni deriva ni falla (referencia) |

Reglas que valida el schema:

- `flota-extrapolada` ⇔ `fuenteTipo = aumentado`, siempre con `tipoEvento = aqua.tanque.lectura`.
- Un evento real no puede marcarse como `aumentado`.
- `esAnomalia = true` exige un `tipoAnomalia` del catálogo: `fuga_lenta`, `corte_prolongado`, `falla_sensor`,
  `deriva_termica`, `llegada_atipica`.
- `esAnomalia = false` exige `tipoAnomalia = null` y `episodioId = null`.
- `timestamp` sigue en milisegundos epoch (UTC).

`contratos/validar.py` cubre 22 ejemplos (12 nuevos, de aumentados) y los 6 chequeos del puente: 0 fallos.

## Perfil de los datos reales

`batch/perfilar.py` → `batch/perfiles/perfil_real.json`. Calculado el 2026-10-29 con tres fuentes:

| Fuente | Lecturas | Periodo | Qué aporta |
|---|---|---|---|
| ESP32 (Firebase `historial`, entradas del firmware) | 7,349 | 2026-10-02 → 2026-10-29 | ruido, saltos, tasa de `-1`, cadencia |
| ThingSpeak, 4 canales con `nivelPct` | 28,763 | hasta 2 meses | llenados y consumo de tanques reales |
| Open-Meteo, Juliaca, horario | 720 | 2026-09-28 → 2026-10-27 | ciclo térmico día/noche de Juliaca |

Resultados principales:

- **Distancia del ESP32** (5,090 lecturas válidas dentro del tanque): mediana 6.6 cm, p5 3.9, p95 16.3, máx. 91.7.
  El tanque estuvo casi siempre lleno.
- **Ruido del sensor**: diferencias entre lecturas seguidas con σ = 0.29 cm (p1/p99 ±1.4 cm). Los saltos de más de
  2 cm ocurren en el 1.9 % de los pares.
- **Lecturas inválidas**:
  - `distanciaCm = -1`: 225 (3.1 % del total; 0.8 % en la operación continua del 28-oct).
  - DHT11 en `-1`: 57.
  - Sensor fuera del tanque (más de 120 cm): 2,027, sobre todo los días 1 y 2 de octubre.
- **Cadencia**: mediana de 5 s y ningún timestamp duplicado. Hay 6 huecos de más de 60 s, que son los días sin medir
  entre tandas.
- **Llenados en el ESP32**: solo **1** subida real de nivel en todo el historial. Con eso no se puede estimar el
  suministro.
- **ThingSpeak 1233810** (tanque de 24,000 L): 67 llenados en dos meses, uno cada ~22 h.
  - Cada llenado dura una mediana de 71 min y sube 7.9 puntos porcentuales.
  - Empiezan sobre todo entre las 4 y las 5 UTC.
  - El consumo tiene una mediana de 1.3 pp/h, con mucha dispersión (p95/p50 ≈ 5).
- **Juliaca** (Open-Meteo): mínima diaria con mediana de 1.3 °C (la más baja, -3 °C) hacia las 5 h; máxima diaria
  con mediana de 19.4 °C hacia las 14 h.

### Limitaciones (también en el JSON)

- **Un solo ESP32**: no hay perfiles por zona ni por tanque propio.
- **Geometría desconocida**: el ESP32 mide distancia, no nivel, así que la altura útil y la capacidad del tanque son
  supuestos.
- **Temperatura y humedad del ESP32**: son del aire (DHT11), no del agua, y solo cubren de 15 a 23 h. El ciclo
  nocturno de Juliaca sale de Open-Meteo, que es un modelo meteorológico y no una medición dentro del tanque.
- **Suministro y consumo**: salen de tanques de ThingSpeak de otros países y climas. Sus horas están en UTC y la zona
  horaria de cada canal no se conoce.
- **Franja de llegada en Juliaca**: no se observa. Es un **supuesto declarado** a partir de la continuidad que SUNASS
  reporta para Seda Juliaca (servicio de pocas horas, de madrugada) y se reemplaza con el dato del Benchmarking en F6.

## Modelo del generador

`ingesta/generador.py` con `ingesta/generador.yml`. En el YAML cada parámetro dice si viene del `[perfil]` o es un
`[supuesto]`. Por cada tanque y paso de tiempo:

1. **Volumen**: se resta el consumo, se resta la fuga si la hay y se suma el llenado si hay agua en la red. El
   flotador corta al llenarse y el tanque no baja de 0.
   - **Consumo** `[supuesto]`: personas × 100 L/día × peso de la hora (picos de mañana y noche) × un multiplicador
     lognormal por hora (σ 0.5).
   - **Llegadas de agua** `[supuesto SUNASS]`: una por día con probabilidad 0.92, empezando dentro de la franja de la
     zona (zona A de 4 a 7 h, zona B de 5 a 8 h), con 3 h de servicio a 12 L/min.
2. **Distancia verdadera**: `distanciaRealCm = sensor_sobre_lleno + altura_útil × (1 − volumen / capacidad)`.
3. **Lo que mide el HC-SR04**: `distanciaCm = distanciaRealCm × c(20 °C) / c(T)`, con `c(T) = 331.3 + 0.606·T`,
   más ruido N(0, 0.29) y saltos de 2 a 6 cm en el 1.9 % de las lecturas. Se redondea a 0.1 cm, como el ESP32.
4. **Temperatura de la cámara de aire**: el ciclo horario real de Juliaca, amortiguado (0.6), con un desvío diario.
5. **Fallas puntuales con la tasa real**: `-1` de distancia (0.8 %) y del DHT11 (0.78 %). Cada una se etiqueta
   `falla_sensor`, sin episodio.

La **deriva térmica normal** es parte del modelo físico: las noches frías de Juliaca alargan la distancia medida en
~0.7 cm de promedio, contra ~0.2 cm de día, y esas lecturas **no** se etiquetan como anomalía, porque al sensor real
le pasa lo mismo todas las noches. El efecto es proporcional a la distancia: a 30 cm, 5-8 °C extra suman ~0.5 cm; a
100 cm, ~3 cm.

### Anomalías inyectadas

Política de etiquetado: se etiqueta **todo el intervalo del episodio**. Todos los eventos de un episodio comparten
`episodioId`. Las fallas puntuales llevan `episodioId = null`. Los episodios de un mismo tanque no se superponen.

| Tipo | Mecanismo | Intervalo etiquetado |
|---|---|---|
| `fuga_lenta` | salida extra de 4-10 L/h durante 4-10 h; nunca encima de un llenado, que la taparía | todo el episodio |
| `corte_prolongado` | no ocurren 1-2 llegadas esperadas y el tanque se vacía | desde la llegada que no ocurrió hasta la siguiente |
| `deriva_termica` | helada atípica: la cámara se enfría 5-8 °C extra desde las 20 h, durante 6-8 h, con rampa; termina antes de la franja de llegada | toda la noche del episodio |
| `llegada_atipica` | la llegada del día ocurre entre las 12 y las 18 h en vez de en la franja | servicio de 3 h + 30 min |
| `falla_sensor` | ráfagas de 10-60 min de `-1` o de distancias imposibles (más allá del fondo), más las fallas puntuales | ráfaga o lectura |

El presupuesto de anómalos (`proporcion_anomalias`) se reparte entre los tipos (`anomalias.reparto`). Un tipo no
toma un episodio que lo pase más de 10 % de su cuota.

### Cómo medir los falsos positivos de la deriva térmica

Cada evento lleva `distanciaRealCm`, `distanciaCm` y `temperaturaC`, así que un detector de fugas se puede evaluar
dos veces:

1. **Sobre la distancia cruda.**
2. **Sobre la distancia compensada**: `distanciaCm × c(T) / c(20 °C)`.

Los avisos de fuga en noches con `tipoAnomalia = deriva_termica` (o en noches normales frías) son falsos positivos
que la compensación debería eliminar.

## Corrida de referencia (2026-10-29)

```bash
.venv/Scripts/python ingesta/generador.py --parquet --kafka --crear-topico --tasa 2000
```

| | Resultado |
|---|---|
| `runId` | `r2026-45facae2` (semilla 2026 + hash de configuración y versión del generador) |
| Volumen | **172,800 eventos** = 4 tanques × 30 días × 1,440 lecturas/día (intervalo de 60 s) |
| Periodo simulado | 2026-09-28 00:00 → 2026-10-27 23:59 (hora de Lima) |
| Reproducibilidad | dos corridas con la misma configuración dan la misma huella de contenido (`c67c485ac9253935`) |
| Anómalos | **8,410 (4.87 %)**: falla_sensor 3,434 (de ellas, ~2,500 puntuales), fuga_lenta 1,538, corte_prolongado 1,358, deriva_termica 1,240, llegada_atipica 840 |
| Episodios | fuga_lenta 5, corte_prolongado 1, deriva_termica 3, llegada_atipica 4, falla_sensor 25 |
| Contrato | muestra de 1,000 eventos validada en el generador y otra de 1,000 leída de Kafka: **0 inválidos** |

### Kafka: `aqua-eventos-aumentados`

- **Configuración:** 3 particiones, clave `sensorId`, `retention.ms = 604800000` (7 días),
  `message.timestamp.type = CreateTime`. Lo crea `--crear-topico`, nunca la auto-creación, que lo dejaría con 1
  partición.
- **Publicación:** 172,800 mensajes en 86 s a ~2,000 ev/s, con **0 errores** y 172,800 `eventId` únicos.
- **Particiones:** 43,200 / 43,200 / 86,400. Con solo 4 claves, el hash manda dos tanques a la misma partición; con
  más tanques el reparto se equilibra.
- **Timestamps:** el del mensaje es la hora de publicación (2026-10-29 02:02-02:04 UTC); el `timestamp` del JSON es
  la fecha simulada (28-sep a 28-oct). Así la retención de 7 días no borra mensajes por tener fechas viejas.

### Parquet: `batch/datos/aumentados/runId=<id>/fecha=<YYYY-MM-DD>/`

- **Contenido:** 172,800 filas en 30 archivos, una partición por día. Los mismos 172,800 `eventId` que en Kafka.
- **Columnas:** las del contrato más `fecha`, que es la partición. Ni secretos ni credenciales.
- **Particionado:** por fecha y no por sensor. Con 4 tanques, partir por sensor daría 120 archivos de 1,440 filas.
- **Sin duplicados:** volver a correr la misma configuración reescribe las particiones de esa corrida
  (`delete_matching`).
- **Archivos extra en la carpeta de la corrida:** `_episodios.json` (cada episodio con su inicio, fin y eventos) y
  `_resumen.json`.

### Integridad del flujo real

- **Offsets:** los de `aqua-eventos-real` antes (`0:5924, 1:183, 2:61`) y después de publicar (`0:5924, 1:187, 2:61`)
  solo difieren en 4 lecturas de ThingSpeak del colector.
- **Contenido:** ningún mensaje del tópico real tiene `fuenteTipo = aumentado` ni `eventId` con prefijo `aum-`.
- **Firebase:** el generador no tiene código que escriba en él.

## Grafana

El tablero **"F2b - Datos aumentados en Kafka (flota extrapolada)"** (`dashboards/aumentados_kafka.json`) tiene solo
métricas a nivel Kafka, de Prometheus/kafka-exporter:

- eventos retenidos por tópico;
- eventos por segundo;
- distribución y tasa por partición;
- particiones y réplicas.

La variable `topico` permite ver el tópico real, el aumentado o ambos, siempre separados.

Se verificó que las consultas devuelven datos en Prometheus: 172,800 retenidos, 43,200/43,200/86,400 por partición y
un pico de 1,790 ev/s. Los paneles de contenido (nivel, anomalías por tipo, episodios) quedan para **F3/F5**,
alimentados por Spark.

## Comandos

```bash
# Perfil real (descarga ESP32, ThingSpeak y Open-Meteo a batch/datos/fuentes/)
docker run --rm --env-file .env -v "$PWD:/repo" -w /repo/batch monitoraqua-ingesta:latest python perfilar.py

# Generar y validar sin escribir nada
.venv/Scripts/python ingesta/generador.py --seco

# Generar, guardar en Parquet y publicar (la primera vez, con --crear-topico)
.venv/Scripts/python ingesta/generador.py --parquet --kafka --crear-topico --tasa 2000

# Otras configuraciones (cambian el runId)
.venv/Scripts/python ingesta/generador.py --tanques 8 --dias 15 --parquet                     # 172,800 eventos
.venv/Scripts/python ingesta/generador.py --intervalo-s 300 --tanques 12 --dias 30 --parquet  # 103,680 eventos
.venv/Scripts/python ingesta/generador.py --semilla 7 --anomalias 0.08 --seco
.venv/Scripts/python ingesta/generador.py --intervalo-s 300 --permitir-menos --seco           # 34,560: exige el permiso explícito

# Reanudar una publicación cortada en el evento 90,000 (misma configuración = mismos eventos)
.venv/Scripts/python ingesta/generador.py --kafka --desde 90000
```

Si la configuración produce menos de `minimo_eventos` (100,000), el generador se detiene y lo dice, salvo que se
pase `--permitir-menos`. Firebase no tiene integración: está fuera de esta fase por decisión del equipo.

## Modelo f2b.2 (`ingesta/generador_f2b2.yml`)

Mismo modelo físico que f2b.1; cambian dos supuestos que hacían que la validación de F4 castigara al detector por
diferencias de definición, no por errores:

| | f2b.1 (`generador.yml`, corrida de referencia) | f2b.2 (`generador_f2b2.yml`) |
|---|---|---|
| Fuga lenta | 4-10 h, 4-10 L/h, a cualquier hora, nunca encima de un llenado | **1-4 días, 1.5-4 L/h** (un flotador o un goteo), cruza las noches y convive con los llenados; al menos 4 episodios por corrida |
| Llegadas que no ocurren | el 8 % de los días sin agua **no** se etiqueta; solo los cortes inyectados | **toda** llegada omitida es `corte_prolongado`, desde el inicio de la franja de ese día hasta la siguiente llegada |
| Eventos anómalos (semilla 7) | 5.7 % | 16.2 % (los episodios largos ocupan más tiempo) |

- **Activación:** f2b.2 se activa con `version_modelo: f2b.2` en su YAML. Sin esa clave el generador produce
  exactamente f2b.1: la corrida de referencia `r2026-45facae2` conserva su huella `c67c485ac9253935`.
- **Superposición con fugas:** en f2b.2 los cortes se etiquetan después de las fugas. Una llegada omitida que cae
  dentro de una fuga larga queda sin etiquetar (los episodios no se superponen) y se cuenta
  (`omitidas_sin_etiqueta`: 2 en la semilla 7).
- **Efecto en la consistencia:** la tabla f2b.1 frente a f2b.2 de la validación está en `docs/eventos-servicio.md`.
- **Uso:**

  ```bash
  .venv/Scripts/python ingesta/generador.py --config ingesta/generador_f2b2.yml --semilla 7 --seco
  ```

## Pendientes y límites

- **Republicar duplica en el tópico:** volver a publicar la misma corrida con `--kafka` sin `--desde` repite los
  mensajes. Los `eventId` son los mismos, así que Spark los deduplica (F3), pero el tópico crece.
- **Altura y capacidad:** siguen siendo supuestos hasta que el equipo mida el tanque real.
- **Franja de llegada:** sigue siendo un supuesto hasta que F6 cargue el Benchmarking de SUNASS.
- **Grafana:** la carga del tablero no se pudo comprobar por la API, porque la contraseña de admin cambió y `.env`
  no está actualizado. El archivo está montado en Grafana y sus consultas devuelven datos en Prometheus.
