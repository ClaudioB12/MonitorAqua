# Tablas D6 (pérdida proyectada por fuga)

Modelo `d6.1`. Umbral de P(fuga) elegido en el ajuste: 0.7 (F1 de ajuste 0.63). Factor de persistencia: 24 h = 0.61, 168 h = 0.23.

Control: la reconstrucción de f2b.2 semilla 7 reproduce la tabla oro d6_noches (120/120 noches, diferencia máxima 0.0 cm, etiquetas iguales: True).

## Flota aumentado_f2b2 (consistencia, 14 episodios de fuga, evidencia suficiente)

Noches evaluables 384 de 384; con fuga 25; deriva térmica 12.

| Detector | Recall por episodio | Recall por noche | Precisión por noche | Alarmas en deriva | Falsas / 100 noches sin fuga |
|---|---|---|---|---|---|
| modelo | 0.93 [0.69-0.99] (13/14) | 0.88 [0.70-0.96] (22/25) | 0.55 [0.40-0.69] (23/42) | 0.00 [0.00-0.24] (0/12) | 5.32 |
| base_c_alerta_f44 | 0.50 [0.27-0.73] (7/14) | 0.32 [0.17-0.52] (8/25) | 0.80 [0.49-0.94] (8/10) | 0.00 [0.00-0.24] (0/12) | 0.56 |
| base_a_sin_fuga | 0.00 [0.00-0.21] (0/14) | 0.00 [0.00-0.13] (0/25) | — | 0.00 [0.00-0.24] (0/12) | 0.0 |

| Tasa real de la fuga | Recall por noche: modelo | Recall por noche: alerta_fuga f4.4 |
|---|---|---|
| 1.5-2.5 L/h | 0.62 [0.31-0.86] (5/8) | 0.25 [0.07-0.59] (2/8) |
| 2.5-4 L/h | 1.00 [0.82-1.00] (17/17) | 0.35 [0.17-0.59] (6/17) |

Error de la tasa en las noches con fuga (L/h, IC por episodio):

| Predictor | MAE [IC 95 %] · sesgo |
|---|---|
| modelo | 0.91 [0.55, 1.25] · sesgo +0.34 |
| base_a_sin_fuga | 2.71 [2.43, 3.01] · sesgo -2.71 |
| base_b_noche_anterior | 1.71 [1.22, 2.28] · sesgo -0.90 |
| base_c_alerta_f44 | 2.13 [1.51, 2.68] · sesgo -1.51 |

Pérdida proyectada a 24h (L, IC por tanque y corrida):

| Predictor | Todas las noches | Solo noches con fuga |
|---|---|---|
| modelo | 4.6 [3.1, 6.4] · sesgo +0.4 | 23.4 [18.0, 29.7] · sesgo -3.1 |
| base_a_sin_fuga | 4.5 [2.1, 7.1] · sesgo -4.5 | 45.6 [36.1, 54.8] · sesgo -45.6 |
| base_b_noche_anterior | 12.0 [9.0, 15.2] · sesgo +6.8 | 39.6 [27.5, 53.0] · sesgo -5.1 |
| base_c_alerta_f44 | 4.8 [2.2, 7.2] · sesgo -2.5 | 47.1 [37.2, 57.2] · sesgo -18.4 |

Banda P10-P90: cobertura 0.90 [0.86-0.92] (301/336); en noches con fuga 0.75 [0.55-0.88] (18/24); ancho mediano 0.0 L.

Pérdida proyectada a 168h (L, IC por tanque y corrida):

| Predictor | Todas las noches | Solo noches con fuga |
|---|---|---|
| modelo | 22.7 [11.5, 35.6] · sesgo -17.4 | 41.8 [14.1, 73.8] · sesgo -30.8 |
| base_a_sin_fuga | 31.7 [14.9, 49.5] · sesgo -31.7 | 133.6 [92.4, 175.4] · sesgo -133.6 |
| base_b_noche_anterior | 56.9 [36.7, 80.8] · sesgo +5.4 | 167.1 [92.4, 261.3] · sesgo -25.1 |
| base_c_alerta_f44 | 41.9 [19.1, 67.2] · sesgo -15.3 | 195.2 [113.4, 303.6] · sesgo -40.8 |

Banda P10-P90: cobertura 0.88 [0.75-0.94] (42/48); en noches con fuga 0.80 [0.38-0.96] (4/5); ancho mediano 111.0 L.

Sesgo de selección: evaluables 1.00 [0.87-1.00] (25/25) de las noches con fuga y 1.00 [0.99-1.00] (359/359) de las demás; episodios sin noche evaluable: 0.

## Flota aumentado_f2b3 (consistencia, 14 episodios de fuga, evidencia suficiente)

Noches evaluables 384 de 384; con fuga 25; deriva térmica 12.

| Detector | Recall por episodio | Recall por noche | Precisión por noche | Alarmas en deriva | Falsas / 100 noches sin fuga |
|---|---|---|---|---|---|
| modelo | 0.86 [0.60-0.96] (12/14) | 0.80 [0.61-0.91] (20/25) | 0.55 [0.40-0.70] (21/38) | 0.00 [0.00-0.24] (0/12) | 4.76 |
| base_c_alerta_f44 | 0.36 [0.16-0.61] (5/14) | 0.24 [0.12-0.43] (6/25) | 1.00 [0.61-1.00] (6/6) | 0.00 [0.00-0.24] (0/12) | 0.0 |
| base_a_sin_fuga | 0.00 [0.00-0.21] (0/14) | 0.00 [0.00-0.13] (0/25) | — | 0.00 [0.00-0.24] (0/12) | 0.0 |

| Tasa real de la fuga | Recall por noche: modelo | Recall por noche: alerta_fuga f4.4 |
|---|---|---|
| 1.5-2.5 L/h | 0.38 [0.14-0.69] (3/8) | 0.12 [0.02-0.47] (1/8) |
| 2.5-4 L/h | 1.00 [0.82-1.00] (17/17) | 0.29 [0.13-0.53] (5/17) |

Error de la tasa en las noches con fuga (L/h, IC por episodio):

| Predictor | MAE [IC 95 %] · sesgo |
|---|---|
| modelo | 0.58 [0.43, 0.73] · sesgo -0.05 |
| base_a_sin_fuga | 2.71 [2.43, 3.01] · sesgo -2.71 |
| base_b_noche_anterior | 1.76 [1.47, 2.19] · sesgo -1.50 |
| base_c_alerta_f44 | 2.02 [1.43, 2.48] · sesgo -1.94 |

Pérdida proyectada a 24h (L, IC por tanque y corrida):

| Predictor | Todas las noches | Solo noches con fuga |
|---|---|---|
| modelo | 5.4 [3.7, 7.3] · sesgo -0.1 | 30.5 [24.2, 38.2] · sesgo -11.1 |
| base_a_sin_fuga | 4.5 [2.1, 7.1] · sesgo -4.5 | 45.6 [36.1, 54.8] · sesgo -45.6 |
| base_b_noche_anterior | 12.4 [9.9, 15.3] · sesgo +6.3 | 38.9 [31.5, 49.0] · sesgo -20.1 |
| base_c_alerta_f44 | 4.5 [2.2, 6.8] · sesgo -3.4 | 45.3 [36.5, 55.1] · sesgo -30.0 |

Banda P10-P90: cobertura 0.87 [0.82-0.90] (291/336); en noches con fuga 0.58 [0.39-0.76] (14/24); ancho mediano 0.0 L.

Pérdida proyectada a 168h (L, IC por tanque y corrida):

| Predictor | Todas las noches | Solo noches con fuga |
|---|---|---|
| modelo | 26.9 [15.0, 41.1] · sesgo -20.7 | 70.2 [40.4, 103.5] · sesgo -70.2 |
| base_a_sin_fuga | 31.7 [14.9, 49.5] · sesgo -31.7 | 133.6 [92.4, 175.4] · sesgo -133.6 |
| base_b_noche_anterior | 60.8 [39.0, 82.4] · sesgo +12.0 | 105.0 [62.2, 160.0] · sesgo -105.0 |
| base_c_alerta_f44 | 31.7 [14.9, 49.5] · sesgo -31.7 | 133.6 [92.4, 175.4] · sesgo -133.6 |

Banda P10-P90: cobertura 0.90 [0.78-0.95] (43/48); en noches con fuga 0.80 [0.38-0.96] (4/5); ancho mediano 111.0 L.

Sesgo de selección: evaluables 1.00 [0.87-1.00] (25/25) de las noches con fuga y 1.00 [0.99-1.00] (359/359) de las demás; episodios sin noche evaluable: 0.

## ThingSpeak real: descenso de la noche siguiente

Noches anteriores para la mediana (elegido en el diseño): k = 10.

| Conjunto | Noches | Tanques | Unidad | Mediana de k noches | Sin fuga (0) | Noche anterior |
|---|---|---|---|---|---|---|
| diseno | 16 | thingspeak-1233810, thingspeak-1233815, thingspeak-283977 | pp | 0.171 [0.148, 0.233] · sesgo +0.001 | 0.201 [0.172, 0.275] · sesgo +0.085 | 0.299 [0.266, 0.353] · sesgo -0.095 |
| diseno (litros) | 12 | con capacidad | L | 30.6 [22.5, 35.8] · sesgo -8.8 | 40.3 [29.8, 48.7] · sesgo +18.1 | 24.7 [20.1, 27.9] · sesgo -7.5 |
| prueba | 11 | thingspeak-1233815, thingspeak-283977, thingspeak-480876 | pp | 0.164 [0.158, 0.175] · sesgo +0.073 | 0.060 [0.041, 0.069] · sesgo -0.013 | 0.137 [0.123, 0.153] · sesgo +0.025 |
| prueba (litros) | 7 | con capacidad | L | 10.6 [8.3, 14.7] · sesgo +2.1 | 9.8 [2.4, 13.1] · sesgo +8.1 | 13.2 [8.0, 19.2] · sesgo -3.2 |
| fuera_de_alcance_prueba | 5 | thingspeak-3418755 | cm | 0.000 (sin IC) · sesgo +0.000 | 0.000 (sin IC) · sesgo +0.000 | 0.000 (sin IC) · sesgo +0.000 |

## Sensibilidad mínima detectable

| Fuente de incertidumbre | Mínima detectable |
|---|---|
| ruido hc_sr04 a 17 cm (280 min) | 0.029 cm/h = 0.31 L/h |
| ruido jsn_sr04t | sin un tramo de 30 min válido: pendiente |
| DHT11 a 25_cm (cambio de 1 °C no visto; peor caso 2 °C) | 0.43 L/h (peor caso 0.86) |
| DHT11 a 50_cm (cambio de 1 °C no visto; peor caso 2 °C) | 0.86 L/h (peor caso 1.72) |
| DHT11 a 100_cm (cambio de 1 °C no visto; peor caso 2 °C) | 1.72 L/h (peor caso 3.45) |
| consumo nocturno aumentado_f2b2 (P95 del exceso sin fuga) | 1.55 L/h |
| consumo nocturno aumentado_f2b3 (P95 del exceso sin fuga) | 1.59 L/h |
