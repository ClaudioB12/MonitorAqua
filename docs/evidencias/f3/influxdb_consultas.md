# F3: consultas a InfluxDB y resultados

Ejecutadas con `docker exec lambda26-influxdb influx query --org horeb --raw "<flux>"`.

## Ventanas: puntos de lecturas_agg por origen y tamaño de ventana

```flux
from(bucket:"monitoraqua") |> range(start:-40d) |> filter(fn:(r)=> r._measurement=="lecturas_agg" and r._field=="n_total") |> group(columns:["origen","ventana"]) |> count()
```

Resultado (2026-10-29T03:36:23Z):

```text
_start,_stop,_value,origen,ventana
2026-09-19T03:36:24.126309315Z,2026-10-29T03:36:24.126309315Z,231,ea-uk,15m
2026-09-19T03:36:24.126309315Z,2026-10-29T03:36:24.126309315Z,1124,esp32-fisico,1m
2026-09-19T03:36:24.126309315Z,2026-10-29T03:36:24.126309315Z,12,open-meteo,15m
2026-09-19T03:36:24.126309315Z,2026-10-29T03:36:24.126309315Z,362,thingspeak,1m
2026-09-19T03:36:24.126309315Z,2026-10-29T03:36:24.126309315Z,61,usgs,15m
```

## Ventanas: ventana ESP32 de las 03:25 UTC (válidas e inválidas)

```flux
from(bucket:"monitoraqua") |> range(start:2026-10-29T03:25:00Z, stop:2026-10-29T03:25:01Z) |> filter(fn:(r)=> r._measurement=="lecturas_agg" and r.origen=="esp32-fisico") |> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value") |> keep(columns:["_time","sensorId","n_total","n_validas","n_invalidas","distanciaCm_media","distanciaCm_min","distanciaCm_max","latencia_ingesta_s_media"])
```

Resultado (2026-10-29T03:36:24Z):

```text
_time,sensorId,distanciaCm_max,distanciaCm_media,distanciaCm_min,latencia_ingesta_s_media,n_invalidas,n_total,n_validas
2026-10-29T03:25:00Z,esp32-tanque-real-01,17.1,17,16.4,0.2050833333333333,4,12,8
```

## Watermark: el evento tardío (esp32-prueba-f3, timestamp 01:28 UTC) no llegó a InfluxDB

```flux
from(bucket:"monitoraqua") |> range(start:2026-10-29T01:00:00Z, stop:2026-10-29T02:00:00Z) |> filter(fn:(r)=> r.sensorId=="esp32-prueba-f3") |> count()
```

Resultado (2026-10-29T03:36:24Z):

```text
```

## Deduplicación: el eventId publicado dos veces cuenta una sola vez

```flux
from(bucket:"monitoraqua") |> range(start:2026-10-29T03:28:00Z, stop:2026-10-29T03:30:00Z) |> filter(fn:(r)=> r.sensorId=="esp32-prueba-f3" and (r._field=="n_total" or r._field=="distanciaCm")) |> keep(columns:["_time","_measurement","_field","_value"])
```

Resultado (2026-10-29T03:36:24Z):

```text
_time,_value,_field,_measurement
2026-10-29T03:28:48.859Z,42,distanciaCm,lecturas_esp32
_time,_value,_field,_measurement
2026-10-29T03:28:00Z,1,n_total,lecturas_agg
2026-10-29T03:28:30Z,1,n_total,lecturas_agg
```

## Métricas de Spark: máximo de numRowsDroppedByWatermark por consulta (spark_progreso)

```flux
from(bucket:"monitoraqua") |> range(start:2026-10-29T03:00:00Z, stop:2026-10-29T04:00:00Z) |> filter(fn:(r)=> r._measurement=="spark_progreso" and r._field=="numRowsDroppedByWatermark") |> max() |> keep(columns:["consulta","_value"])
```

Resultado (2026-10-29T03:36:25Z):

```text
_value,consulta
0,bronce
1,esp32_detalle
0,lentas_agg
1,rapidas_agg
```

## Métricas de Spark: máximo de numDroppedDuplicateRows por consulta (spark_progreso)

```flux
from(bucket:"monitoraqua") |> range(start:2026-10-29T03:00:00Z, stop:2026-10-29T04:00:00Z) |> filter(fn:(r)=> r._measurement=="spark_progreso" and r._field=="numDroppedDuplicateRows") |> max() |> keep(columns:["consulta","_value"])
```

Resultado (2026-10-29T03:36:25Z):

```text
_value,consulta
0,bronce
54,esp32_detalle
0,lentas_agg
54,rapidas_agg
```

