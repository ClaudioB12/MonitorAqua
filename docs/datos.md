# Datos

## Ficha del dataset de muestra

**Archivo:** [`data/tanques_horeb_muestra.csv`](https://github.com/ClaudioB12/MonitorAqua/blob/main/data/tanques_horeb_muestra.csv)

| Característica | Valor |
|---|---|
| Registros | 34,560 |
| Tanques | 8 (1 nodo maestro + 7 nodos simples) |
| Zonas | 3 |
| Periodo | 30 días |
| Frecuencia de muestreo | Cada 10 minutos |

### Columnas

`device_id`, `timestamp`, `zona`, `distancia_cm`, `temp_aire_c`, `hum_aire_pct`, `nivel_cm_comp`, `litros`, `capacidad_litros`, `altura_tanque_cm`, `presion_linea_kpa`, `caudal_lpm`, `temp_agua_c`, `orp_mv`, `horas_residencia`, `bateria_pct`

### Suciedad realista

El dataset incluye imperfecciones deliberadas, del tipo que produce la instrumentación real:

- **207 valores nulos** repartidos en las series.
- **Ecos espurios** del sensor ultrasónico (lecturas de distancia físicamente inconsistentes).

### Fugas configuradas

Tres nodos tienen fugas inyectadas con tasas conocidas, para validar las dimensiones 5 y 6:

| Nodo | Tasa de fuga |
|---|---|
| `n03` | 2.5 L/h |
| `n05` | 7.0 L/h |
| `n07` | 1.2 L/h |

### Hallazgo: deriva térmica

La lectura cruda del ultrasonido deriva con la temperatura de la cámara de aire porque la velocidad del sonido depende de ella: `c = 331.3·sqrt(1 + T/273.15)`.
En las noches del altiplano esa deriva produce una caída aparente de nivel comparable a una fuga lenta real, por eso el dataset trae tanto la distancia cruda (`distancia_cm`) como el nivel compensado (`nivel_cm_comp`).
