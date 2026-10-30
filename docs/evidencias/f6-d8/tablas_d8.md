# Tablas D8 (temperatura del agua y residencia)

Modelo `d8.1`; principal (elegido en el ajuste): **aire_y_om**. Canal thingspeak-3418755: 1606 h con agua medida ({'entrenamiento': 1131, 'ajuste': 283, 'embargo': 24, 'prueba': 168}).

| Modelo | τ (h) | Ganancias | Constante (°C) |
|---|---|---|---|
| aire_y_om | 24.3 | aire 0.451, om 0.584 | -0.49 |
| solo_aire | 27.9 | aire 0.989 | -0.45 |
| solo_om | 22.7 | om 0.899 | +2.05 |

τ del principal por bootstrap de días: P10 22.7 h, mediana 24.3 h, P90 26.3 h.

## Temperatura del agua: Diseño (ajuste) (MAE en °C, IC por bloques de 24 h)

| Predictor | 0h (13 días, suficiente) | 6h (13 días, suficiente) | 12h (13 días, suficiente) |
|---|---|---|---|
| aire_y_om | 0.29 [0.21, 0.39] · sesgo +0.11 | 0.38 [0.27, 0.49] · sesgo +0.16 | 0.57 [0.42, 0.73] · sesgo +0.20 |
| solo_aire | 0.46 [0.38, 0.61] · sesgo -0.16 | 0.48 [0.41, 0.62] · sesgo -0.12 | 0.57 [0.47, 0.70] · sesgo -0.09 |
| solo_om | 0.92 [0.64, 1.17] · sesgo +0.92 | 1.00 [0.76, 1.22] · sesgo +0.96 | 1.12 [0.83, 1.35] · sesgo +1.00 |
| base_a_aire_actual | 0.98 [0.67, 1.32] · sesgo +0.17 | 0.86 [0.66, 1.12] · sesgo +0.24 | 1.06 [0.82, 1.31] · sesgo +0.33 |
| base_b_media_aire_24h | 0.51 [0.37, 0.66] · sesgo +0.31 | 0.63 [0.46, 0.82] · sesgo +0.36 | 0.81 [0.60, 1.03] · sesgo +0.40 |
| base_c_provisional_f44 | 0.48 [0.29, 0.70] · sesgo +0.39 | 0.54 [0.36, 0.75] · sesgo +0.43 | 0.68 [0.51, 0.87] · sesgo +0.47 |

## Temperatura del agua: Prueba (MAE en °C, IC por bloques de 24 h)

| Predictor | 0h (8 días, suficiente) | 6h (8 días, suficiente) | 12h (8 días, suficiente) |
|---|---|---|---|
| aire_y_om | 0.16 [0.10, 0.23] · sesgo +0.13 | 0.24 [0.17, 0.33] · sesgo +0.12 | 0.41 [0.31, 0.55] · sesgo +0.11 |
| solo_aire | 0.91 [0.57, 1.36] · sesgo -0.87 | 0.91 [0.57, 1.36] · sesgo -0.84 | 0.95 [0.61, 1.39] · sesgo -0.83 |
| solo_om | 1.79 [1.70, 2.03] · sesgo +1.79 | 1.75 [1.64, 2.04] · sesgo +1.75 | 1.73 [1.48, 2.09] · sesgo +1.73 |
| base_a_aire_actual | 0.22 [0.18, 0.26] · sesgo -0.21 | 0.35 [0.26, 0.48] · sesgo -0.28 | 0.47 [0.33, 0.70] · sesgo -0.34 |
| base_b_media_aire_24h | 0.41 [0.28, 0.63] · sesgo -0.33 | 0.52 [0.36, 0.82] · sesgo -0.38 | 0.64 [0.41, 0.98] · sesgo -0.42 |
| base_c_provisional_f44 | 0.61 [0.46, 0.82] · sesgo -0.33 | 0.63 [0.49, 0.83] · sesgo -0.30 | 0.68 [0.51, 0.90] · sesgo -0.29 |

## Cobertura de la banda P10-P90 (nominal 80 %)

| Modelo | Conjunto | 0 h | 6 h | 12 h |
|---|---|---|---|---|
| aire_y_om | ajuste | 81% (ancho 0.92 °C) | 81% (ancho 1.17 °C) | 81% (ancho 1.70 °C) |
| aire_y_om | prueba | 100% (ancho 0.92 °C) | 95% (ancho 1.17 °C) | 89% (ancho 1.70 °C) |
| solo_aire | ajuste | 81% (ancho 1.46 °C) | 81% (ancho 1.58 °C) | 81% (ancho 1.83 °C) |
| solo_aire | prueba | 39% (ancho 1.46 °C) | 43% (ancho 1.58 °C) | 57% (ancho 1.83 °C) |
| solo_om | ajuste | 81% (ancho 1.48 °C) | 81% (ancho 1.59 °C) | 81% (ancho 2.16 °C) |
| solo_om | prueba | 24% (ancho 1.48 °C) | 42% (ancho 1.59 °C) | 68% (ancho 2.16 °C) |

## Cruce de umbral en las próximas 12 h

Umbrales configurados {'bajo_c': 4.0, 'alto_c': 25.0}: horas del canal bajo 4 °C = 0, sobre 25 °C = 0 (no evaluables). Rejilla de evaluación: {'desde_c': 8.0, 'hasta_c': 18.5, 'paso_c': 0.5}.

| Lado | Conjunto | Orígenes (con cruce) | Episodios | Evidencia | Modelo: Brier | (a) aire actual | (b) media 24 h | (c) provisional f4.4 |
|---|---|---|---|---|---|---|---|---|
| bajo | ajuste | 2182 (258) | 23 | suficiente | 0.066 (skill +0.42; calib. +0.016) | 0.120 (skill -0.06; calib. -0.010) | 0.126 (skill -0.12; calib. -0.092) | 0.116 (skill -0.02; calib. -0.078) |
| bajo | prueba | 492 (65) | 6 | indicativa | 0.076 (skill +0.43; calib. +0.049) | 0.134 (skill -0.02; calib. -0.008) | 0.240 (skill -0.82; calib. +0.146) | 0.264 (skill -1.00; calib. +0.163) |
| alto | ajuste | 4039 (175) | 15 | suficiente | 0.027 (skill +0.80; calib. +0.012) | 0.070 (skill +0.47; calib. +0.035) | 0.056 (skill +0.58; calib. +0.013) | 0.065 (skill +0.51; calib. +0.046) |
| alto | prueba | 2938 (97) | 7 | indicativa | 0.019 (skill +0.88; calib. +0.007) | 0.033 (skill +0.79; calib. -0.033) | 0.028 (skill +0.82; calib. -0.027) | 0.036 (skill +0.76; calib. -0.016) |

## Precisión limitante del DHT11 (prueba, estimación actual)

| Modelo | MAE base | +2 °C: cambio / MAE | −2 °C: cambio / MAE | Redondeo a 1 °C: cambio / MAE |
|---|---|---|---|---|
| aire_y_om | 0.16 | +0.90 / 1.03 | -0.90 / 0.78 | +0.02 / 0.17 |
| solo_aire | 0.91 | +1.98 / 1.12 | -1.98 / 2.85 | +0.04 / 0.89 |

## Transferencia a Juliaca (NO es una medición: no hay agua medida)

| Fuente | Horas | Principal P05 / P50 / P95 (°C) | Provisional P50 | Solo OM P50 | Diferencia absoluta principal vs provisional | Incertidumbre por parámetros (ancho P10-P90 mediano) | Horas < 4 °C (principal / provisional) |
|---|---|---|---|---|---|---|---|
| aumentado_f2b1 | 2592 | 8.7 / 10.3 / 11.7 | 10.5 | 11.4 | 0.86 °C | 0.50 °C | 0 / 0 |
| aumentado_f2b2 | 2592 | 8.0 / 9.9 / 11.6 | 9.7 | 11.4 | 0.72 °C | 0.55 °C | 0 / 0 |
| esp32 | 34 | 9.8 / 10.4 / 12.6 | 12.7 | 10.4 | 1.89 °C | 0.63 °C | 0 / 0 |

## Residencia (edad media del agua, tanque bien mezclado)

| Tanque | Pasos de 5 min | Con edad conocida | Mediana (h) | P90 (h) |
|---|---|---|---|---|
| aumentado_f2b1 · flota-zonaA-01 | 8608 | 37% | 50 | 68 |
| aumentado_f2b1 · flota-zonaA-02 | 8585 | 16% | 24 | 37 |
| aumentado_f2b1 · flota-zonaB-01 | 8613 | 48% | 59 | 87 |
| aumentado_f2b1 · flota-zonaB-02 | 8593 | 6% | 68 | 77 |
| aumentado_f2b2 · flota-zonaA-01 | 8606 | 26% | 39 | 53 |
| aumentado_f2b2 · flota-zonaA-02 | 8605 | 57% | 29 | 47 |
| aumentado_f2b2 · flota-zonaB-01 | 8602 | 7% | 31 | 40 |
| aumentado_f2b2 · flota-zonaB-02 | 8587 | 16% | 77 | 102 |
| esp32 · esp32-tanque-real-01 | 252 | 0% | — | — |
| thingspeak · thingspeak-1233810 | 8000 | 22% | 41 | 56 |
| thingspeak · thingspeak-1233815 | 8000 | 0% | — | — |
| thingspeak · thingspeak-283977 | 6333 | 0% | — | — |
| thingspeak · thingspeak-3418755 | 18655 | 28% | 29 | 62 |
| thingspeak · thingspeak-480876 | 1316 | 0% | — | — |

Residencia y temperatura, canal_real_agua_medida: 459 h, correlación de Spearman -0.08.

| Edad (h) | Horas | Temperatura media (°C) |
|---|---|---|
| [0, 12) | 71 | 15.19 |
| [12, 24) | 119 | 13.55 |
| [24, 48) | 166 | 13.27 |
| [48, 96) | 103 | 14.37 |

Residencia y temperatura, flota_agua_estimada: 1545 h, correlación de Spearman +0.27.

| Edad (h) | Horas | Temperatura media (°C) |
|---|---|---|
| [0, 12) | 73 | 9.81 |
| [12, 24) | 192 | 9.56 |
| [24, 48) | 601 | 10.15 |
| [48, 96) | 635 | 10.44 |
| [96, 1000) | 44 | 10.65 |
