# Resultados D4 d4.1, 20261029

## Llegadas: detector_f43

### Hora de la próxima llegada (conjunto final)

| Nivel | Episodios (observados / censurados) | Evidencia | Predictor | Error h [IC 95 % por episodio] | C-index global | C-index por tanque |
|---|---|---|---|---|---|---|
| consistencia | 60 / 8 | suficiente | km_moda | 2.3 [1.0, 4.0] | 0.855 [0.778, 0.931] | 0.871 [0.793, 0.952] |
| consistencia | 60 / 8 | suficiente | km_mediana | 2.3 [1.0, 4.0] | 0.851 [0.775, 0.924] | 0.866 [0.794, 0.942] |
| consistencia | 60 / 8 | suficiente | weibull_moda | 1.6 [0.9, 2.5] | 0.867 [0.803, 0.934] | 0.881 [0.830, 0.947] |
| consistencia | 60 / 8 | suficiente | base_misma_hora | 2.1 [1.6, 2.9] | 0.830 [0.774, 0.887] | 0.843 [0.787, 0.913] |
| consistencia | 60 / 8 | suficiente | base_mediana_intervalo | 2.1 [1.5, 2.8] | 0.852 [0.790, 0.915] | 0.867 [0.799, 0.935] |
| consistencia | ventana P10-P90 | | cobertura antes 78% (ancho 25.0 h) → después 78% (ancho 25.0 h) | | | |
| consistencia | sesgo de selección | | intervalo mediano: observados 23.3 h vs Kaplan-Meier (todos) 23.5 h; censurados 12% | | | |
| consistencia | por tanque: aumentado_f2b1|flota-zonaA-01|r2026-45facae2 | 8 episodios | km_moda 0.6, km_mediana 0.7, weibull_moda 0.4, base_misma_hora 1.1, base_mediana_intervalo 1.3 | | | |
| consistencia | por tanque: aumentado_f2b1|flota-zonaA-02|r2026-45facae2 | 8 episodios | km_moda 0.8, km_mediana 0.8, weibull_moda 0.6, base_misma_hora 0.6, base_mediana_intervalo 1.2 | | | |
| consistencia | por tanque: aumentado_f2b1|flota-zonaB-01|r2026-45facae2 | 6 episodios | km_moda 1.5, km_mediana 0.9, weibull_moda 0.8, base_misma_hora 1.7, base_mediana_intervalo 1.2 | | | |
| consistencia | por tanque: aumentado_f2b1|flota-zonaB-02|r2026-45facae2 | 8 episodios | km_moda 1.8, km_mediana 1.9, weibull_moda 3.9, base_misma_hora 3.3, base_mediana_intervalo 3.4 | | | |
| consistencia | por tanque: aumentado_f2b2|flota-zonaA-01|r7-79a6b935 | 8 episodios | km_moda 6.8, km_mediana 6.6, weibull_moda 4.3, base_misma_hora 3.0, base_mediana_intervalo 2.9 | | | |
| consistencia | por tanque: aumentado_f2b2|flota-zonaA-02|r7-79a6b935 | 6 episodios | km_moda 5.3, km_mediana 5.3, weibull_moda 0.7, base_misma_hora 3.9, base_mediana_intervalo 3.8 | | | |
| consistencia | por tanque: aumentado_f2b2|flota-zonaB-01|r7-79a6b935 | 8 episodios | km_moda 0.8, km_mediana 1.2, weibull_moda 0.8, base_misma_hora 1.9, base_mediana_intervalo 1.6 | | | |
| consistencia | por tanque: aumentado_f2b2|flota-zonaB-02|r7-79a6b935 | 8 episodios | km_moda 0.8, km_mediana 0.9, weibull_moda 0.8, base_misma_hora 1.8, base_mediana_intervalo 1.3 | | | |
| thingspeak_real | — | sin datos evaluables  | | | | |
| esp32_real | — | pendiente sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | | |
| fuera_de_alcance | 12 / 1 | suficiente | km_moda | 22.3 [6.4, 31.1] | 0.511 [0.397, 0.750] | 0.511 [0.397, 0.750] |
| fuera_de_alcance | 12 / 1 | suficiente | km_mediana | 38.0 [28.9, 42.8] | 0.397 [0.259, 0.708] | 0.397 [0.259, 0.708] |
| fuera_de_alcance | 12 / 1 | suficiente | weibull_moda | 7.0 [3.4, 9.7] | 0.559 [0.490, 0.660] | 0.559 [0.490, 0.660] |
| fuera_de_alcance | 12 / 1 | suficiente | base_misma_hora | 10.8 [7.6, 19.1] | 0.390 [0.202, 0.584] | 0.390 [0.202, 0.584] |
| fuera_de_alcance | 12 / 1 | suficiente | base_mediana_intervalo | 7.2 [5.0, 9.5] | 0.423 [0.279, 0.525] | 0.423 [0.279, 0.525] |
| fuera_de_alcance | ventana P10-P90 | | cobertura antes 52% (ancho 90.0 h) → después sin calibrar | | | |
| fuera_de_alcance | sesgo de selección | | intervalo mediano: observados 3.9 h vs Kaplan-Meier (todos) 4.0 h; censurados 8% | | | |
| fuera_de_alcance | por tanque: thingspeak|thingspeak-3418755| | 12 episodios | km_moda 22.3, km_mediana 38.0, weibull_moda 7.0, base_misma_hora 10.8, base_mediana_intervalo 7.2 | | | |

### Probabilidad de llegada en las próximas H horas (ingrediente observable; conjunto final)

| Nivel | H | Ventanas | Frecuencia | Predictor | Brier [IC 95 %] | Skill vs frecuencia del entrenamiento [IC 95 %] | Calibración global |
|---|---|---|---|---|---|---|---|
| consistencia | 6 h | 1496 | 0.24 | km | 0.064 [0.047, 0.081] | 0.65 [0.56, 0.74] | -0.05 |
| consistencia | 6 h | 1496 | 0.24 | weibull | 0.066 [0.051, 0.081] | 0.64 [0.55, 0.72] | -0.06 |
| consistencia | 6 h | 1496 | 0.24 | base_misma_hora | 0.093 [0.072, 0.116] | 0.49 [0.37, 0.61] | -0.01 |
| consistencia | 6 h | 1496 | 0.24 | base_mediana_intervalo | 0.091 [0.066, 0.112] | 0.50 [0.39, 0.64] | -0.04 |
| consistencia | 12 h | 1448 | 0.47 | km | 0.091 [0.065, 0.119] | 0.64 [0.53, 0.74] | -0.09 |
| consistencia | 12 h | 1448 | 0.47 | weibull | 0.093 [0.071, 0.115] | 0.63 [0.54, 0.72] | -0.11 |
| consistencia | 12 h | 1448 | 0.47 | base_misma_hora | 0.115 [0.087, 0.149] | 0.54 [0.40, 0.66] | -0.00 |
| consistencia | 12 h | 1448 | 0.47 | base_mediana_intervalo | 0.111 [0.080, 0.146] | 0.56 [0.42, 0.68] | -0.03 |
| consistencia | 24 h | 1385 | 0.93 | km | 0.107 [0.060, 0.140] | -0.54 [-2.09, -0.20] | -0.15 |
| consistencia | 24 h | 1385 | 0.93 | weibull | 0.107 [0.074, 0.130] | -0.53 [-2.03, -0.34] | -0.18 |
| consistencia | 24 h | 1385 | 0.93 | base_misma_hora | 0.069 [0.018, 0.095] | 0.00 [-0.03, 0.29] | +0.07 |
| consistencia | 24 h | 1385 | 0.93 | base_mediana_intervalo | 0.083 [0.039, 0.109] | -0.19 [-0.60, -0.06] | +0.02 |
| fuera_de_alcance | 6 h | 153 | 0.30 | km | 0.219 [0.118, 0.315] | 0.09 [-0.18, 0.29] | -0.12 |
| fuera_de_alcance | 6 h | 153 | 0.30 | weibull | 0.223 [0.117, 0.318] | 0.08 [-0.05, 0.19] | -0.15 |
| fuera_de_alcance | 6 h | 153 | 0.30 | base_misma_hora | 0.386 [0.242, 0.533] | -0.59 [-1.46, -0.22] | -0.16 |
| fuera_de_alcance | 6 h | 153 | 0.30 | base_mediana_intervalo | 0.549 [0.431, 0.654] | -1.27 [-3.31, -0.52] | +0.12 |
| fuera_de_alcance | 12 h | 147 | 0.44 | km | 0.324 [0.180, 0.471] | -0.07 [-0.38, 0.21] | -0.20 |
| fuera_de_alcance | 12 h | 147 | 0.44 | weibull | 0.267 [0.160, 0.366] | 0.12 [-0.05, 0.26] | -0.18 |
| fuera_de_alcance | 12 h | 147 | 0.44 | base_misma_hora | 0.524 [0.364, 0.673] | -0.73 [-1.88, -0.19] | -0.13 |
| fuera_de_alcance | 12 h | 147 | 0.44 | base_mediana_intervalo | 0.558 [0.327, 0.789] | -0.84 [-3.74, 0.26] | +0.56 |
| fuera_de_alcance | 24 h | 135 | 0.64 | km | 0.423 [0.225, 0.629] | -0.32 [-0.60, 0.02] | -0.35 |
| fuera_de_alcance | 24 h | 135 | 0.64 | weibull | 0.252 [0.216, 0.296] | 0.21 [0.01, 0.31] | -0.20 |
| fuera_de_alcance | 24 h | 135 | 0.64 | base_misma_hora | 0.356 [0.022, 0.667] | -0.11 [-2.00, 0.95] | +0.36 |
| fuera_de_alcance | 24 h | 135 | 0.64 | base_mediana_intervalo | 0.356 [0.022, 0.667] | -0.11 [-2.00, 0.95] | +0.36 |

## Llegadas: subida_sostenida

### Hora de la próxima llegada (conjunto final)

| Nivel | Episodios (observados / censurados) | Evidencia | Predictor | Error h [IC 95 % por episodio] | C-index global | C-index por tanque |
|---|---|---|---|---|---|---|
| thingspeak_real | 5 / 2 | indicativa | km_moda | 27.5 [16.3, 62.8] | 0.526 [0.232, 0.709] | 0.526 [0.232, 0.709] |
| thingspeak_real | 5 / 2 | indicativa | km_mediana | 34.9 [22.4, 86.3] | 0.395 [0.132, 0.616] | 0.395 [0.132, 0.616] |
| thingspeak_real | 5 / 2 | indicativa | weibull_moda | 20.0 [4.3, 25.5] | 0.604 [0.379, 0.758] | 0.604 [0.379, 0.758] |
| thingspeak_real | 5 / 2 | indicativa | base_misma_hora | 22.0 [6.9, 27.9] | 0.513 [0.437, 0.749] | 0.513 [0.437, 0.749] |
| thingspeak_real | 5 / 2 | indicativa | base_mediana_intervalo | 22.8 [11.4, 27.1] | 0.505 [0.399, 0.749] | 0.505 [0.399, 0.749] |
| thingspeak_real | ventana P10-P90 | | cobertura antes 77% (ancho 72.0 h) → después 77% (ancho 72.5 h) | | | |
| thingspeak_real | sesgo de selección | | intervalo mediano: observados 9.6 h vs Kaplan-Meier (todos) 14.8 h; censurados 29% | | | |
| thingspeak_real | por tanque: thingspeak|thingspeak-1233810| | 5 episodios | km_moda 27.5, km_mediana 34.9, weibull_moda 20.0, base_misma_hora 22.0, base_mediana_intervalo 22.8 | | | |
| fuera_de_alcance | 14 / 1 | suficiente | km_moda | 17.7 [10.6, 25.2] | 0.505 [0.407, 0.695] | 0.505 [0.407, 0.695] |
| fuera_de_alcance | 14 / 1 | suficiente | km_mediana | 20.8 [15.6, 25.9] | 0.498 [0.429, 0.660] | 0.498 [0.429, 0.660] |
| fuera_de_alcance | 14 / 1 | suficiente | weibull_moda | 7.0 [2.4, 10.0] | 0.573 [0.454, 0.666] | 0.573 [0.454, 0.666] |
| fuera_de_alcance | 14 / 1 | suficiente | base_misma_hora | 10.7 [7.9, 15.5] | 0.399 [0.274, 0.611] | 0.399 [0.274, 0.611] |
| fuera_de_alcance | 14 / 1 | suficiente | base_mediana_intervalo | 6.8 [2.6, 9.5] | 0.422 [0.352, 0.501] | 0.422 [0.352, 0.501] |
| fuera_de_alcance | ventana P10-P90 | | cobertura antes 67% (ancho 92.5 h) → después sin calibrar | | | |
| fuera_de_alcance | sesgo de selección | | intervalo mediano: observados 4.3 h vs Kaplan-Meier (todos) 4.8 h; censurados 7% | | | |
| fuera_de_alcance | por tanque: thingspeak|thingspeak-3418755| | 14 episodios | km_moda 17.7, km_mediana 20.8, weibull_moda 7.0, base_misma_hora 10.7, base_mediana_intervalo 6.8 | | | |

### Probabilidad de llegada en las próximas H horas (ingrediente observable; conjunto final)

| Nivel | H | Ventanas | Frecuencia | Predictor | Brier [IC 95 %] | Skill vs frecuencia del entrenamiento [IC 95 %] | Calibración global |
|---|---|---|---|---|---|---|---|
| thingspeak_real | 6 h | 146 | 0.21 | km | 0.182 [0.097, 0.306] | -0.08 [-0.47, 0.19] | -0.09 |
| thingspeak_real | 6 h | 146 | 0.21 | weibull | 0.153 [0.080, 0.259] | 0.09 [-0.20, 0.34] | -0.09 |
| thingspeak_real | 6 h | 146 | 0.21 | base_misma_hora | 0.315 [0.192, 0.473] | -0.88 [-2.29, -0.20] | +0.00 |
| thingspeak_real | 6 h | 146 | 0.21 | base_mediana_intervalo | 0.288 [0.168, 0.432] | -0.72 [-1.65, -0.27] | -0.11 |
| thingspeak_real | 12 h | 136 | 0.37 | km | 0.300 [0.134, 0.538] | -0.24 [-0.83, 0.17] | -0.17 |
| thingspeak_real | 12 h | 136 | 0.37 | weibull | 0.241 [0.115, 0.435] | 0.01 [-0.42, 0.31] | -0.15 |
| thingspeak_real | 12 h | 136 | 0.37 | base_misma_hora | 0.485 [0.294, 0.691] | -1.00 [-2.75, -0.25] | +0.03 |
| thingspeak_real | 12 h | 136 | 0.37 | base_mediana_intervalo | 0.471 [0.287, 0.684] | -0.94 [-2.47, -0.31] | -0.10 |
| thingspeak_real | 24 h | 124 | 0.52 | km | 0.382 [0.200, 0.608] | -0.53 [-1.47, 0.23] | -0.15 |
| thingspeak_real | 24 h | 124 | 0.52 | weibull | 0.274 [0.202, 0.350] | -0.10 [-0.43, 0.25] | -0.10 |
| thingspeak_real | 24 h | 124 | 0.52 | base_misma_hora | 0.476 [0.262, 0.932] | -0.91 [-2.41, -0.10] | +0.48 |
| thingspeak_real | 24 h | 124 | 0.52 | base_mediana_intervalo | 0.508 [0.444, 0.806] | -1.04 [-1.97, -0.86] | +0.22 |
| fuera_de_alcance | 6 h | 163 | 0.33 | km | 0.245 [0.163, 0.354] | 0.04 [-0.16, 0.20] | -0.13 |
| fuera_de_alcance | 6 h | 163 | 0.33 | weibull | 0.229 [0.143, 0.337] | 0.10 [-0.01, 0.19] | -0.14 |
| fuera_de_alcance | 6 h | 163 | 0.33 | base_misma_hora | 0.411 [0.264, 0.562] | -0.62 [-1.29, -0.31] | -0.20 |
| fuera_de_alcance | 6 h | 163 | 0.33 | base_mediana_intervalo | 0.675 [0.509, 0.816] | -1.66 [-4.36, -0.36] | +0.67 |
| fuera_de_alcance | 12 h | 157 | 0.49 | km | 0.322 [0.196, 0.436] | -0.01 [-0.23, 0.22] | -0.20 |
| fuera_de_alcance | 12 h | 157 | 0.49 | weibull | 0.263 [0.170, 0.361] | 0.18 [-0.02, 0.28] | -0.17 |
| fuera_de_alcance | 12 h | 157 | 0.49 | base_misma_hora | 0.541 [0.414, 0.701] | -0.69 [-1.65, -0.23] | -0.17 |
| fuera_de_alcance | 12 h | 157 | 0.49 | base_mediana_intervalo | 0.510 [0.267, 0.758] | -0.59 [-3.13, 0.41] | +0.51 |
| fuera_de_alcance | 24 h | 145 | 0.68 | km | 0.323 [0.211, 0.408] | -0.04 [-0.17, 0.20] | -0.26 |
| fuera_de_alcance | 24 h | 145 | 0.68 | weibull | 0.229 [0.214, 0.253] | 0.26 [0.06, 0.39] | -0.17 |
| fuera_de_alcance | 24 h | 145 | 0.68 | base_misma_hora | 0.324 [0.028, 0.624] | -0.04 [-1.66, 0.93] | +0.32 |
| fuera_de_alcance | 24 h | 145 | 0.68 | base_mediana_intervalo | 0.324 [0.028, 0.624] | -0.04 [-1.66, 0.93] | +0.32 |

## Caudal de llenado estimado (L/min, descriptivo)

| Fuente de llegadas | Tanque | Llenados | P10 | Mediana | P90 |
|---|---|---|---|---|---|
| detector_f43 | aumentado_f2b1|flota-zonaA-01|r2026-45facae2 | 27 | 6.5 | 8.1 | 9.4 |
| detector_f43 | aumentado_f2b1|flota-zonaA-02|r2026-45facae2 | 29 | 4.3 | 6.2 | 8.4 |
| detector_f43 | aumentado_f2b1|flota-zonaB-01|r2026-45facae2 | 28 | 6.6 | 7.9 | 9.5 |
| detector_f43 | aumentado_f2b1|flota-zonaB-02|r2026-45facae2 | 29 | 10.4 | 10.9 | 11.8 |
| detector_f43 | aumentado_f2b2|flota-zonaA-01|r7-79a6b935 | 28 | 6.4 | 7.9 | 9.1 |
| detector_f43 | aumentado_f2b2|flota-zonaA-02|r7-79a6b935 | 26 | 4.6 | 7.8 | 9.0 |
| detector_f43 | aumentado_f2b2|flota-zonaB-01|r7-79a6b935 | 28 | 6.1 | 8.1 | 9.9 |
| detector_f43 | aumentado_f2b2|flota-zonaB-02|r7-79a6b935 | 28 | 10.8 | 11.3 | 11.9 |
| detector_f43 | esp32|esp32-tanque-real-01| | 6 | 43.2 | 68.2 | 96.1 |
| subida_sostenida | thingspeak|thingspeak-1233810| | 36 | 20.8 | 33.0 | 50.8 |

## Llegadas excluidas (detector f4.3)

| Tanque | Hora (UTC) | Motivo |
|---|---|---|
| esp32|esp32-tanque-real-01 | 2026-10-09 03:33 | artefacto_reanudacion |
| esp32|esp32-tanque-real-01 | 2026-10-16 01:48 | artefacto_reanudacion |
| esp32|esp32-tanque-real-01 | 2026-10-23 00:44 | artefacto_reanudacion |
| esp32|esp32-tanque-real-01 | 2026-10-29 02:46 | no_verificable |
| esp32|esp32-tanque-real-01 | 2026-10-29 04:08 | no_verificable |
| esp32|esp32-tanque-real-01 | 2026-10-29 06:46 | artefacto_reanudacion |
| esp32|esp32-tanque-real-01 | 2026-10-29 14:23 | artefacto_reanudacion |
| thingspeak|thingspeak-3418755 | 2026-09-16 11:39 | artefacto_reanudacion |