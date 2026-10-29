# Resultados D2 (d2.1, 20261029)

## Pronóstico de nivel a 15 min (conjunto final)

| Nivel | Predictor | n | MAE % [IC 95 %] | RMSE % [IC 95 %] | MAPE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |
|---|---|---|---|---|---|---|---|
| consistencia | persistencia | 18094 | 0.72 [0.65, 0.80] | 2.29 [1.96, 2.60] | 1.02 [0.91, 1.14] |  |  |
| consistencia | tendencia | 18094 | 0.99 [0.86, 1.13] | 3.50 [3.10, 3.91] | 1.23 [1.07, 1.38] |  |  |
| consistencia | holt | 18094 | 0.81 [0.73, 0.88] | 2.26 [1.96, 2.56] | 1.04 [0.94, 1.14] | -11.6% | +18.7% |
| consistencia | holt_pronostico | 18094 | 0.64 [0.56, 0.72] | 2.27 [1.97, 2.56] | 0.84 [0.74, 0.94] | +11.7% | +35.6% |
| consistencia | gbt | 18094 | 0.41 [0.37, 0.45] | 1.28 [1.10, 1.45] | 0.66 [0.59, 0.76] | +43.3% | +58.7% |
| thingspeak_real | persistencia | 3468 | 0.78 [0.58, 0.99] | 2.78 [2.25, 3.36] | 0.85 [0.62, 1.08] |  |  |
| thingspeak_real | tendencia | 3468 | 1.03 [0.80, 1.29] | 2.96 [2.41, 3.51] | 1.12 [0.86, 1.40] |  |  |
| thingspeak_real | holt | 3468 | 1.18 [0.96, 1.45] | 2.80 [2.34, 3.32] | 1.28 [1.02, 1.55] | -52.7% | -14.5% |
| thingspeak_real | holt_pronostico | 3468 | 0.78 [0.58, 0.99] | 2.78 [2.25, 3.36] | 0.85 [0.62, 1.08] | +0.0% | +25.0% |
| thingspeak_real | gbt | 3468 | 1.00 [0.77, 1.25] | 2.91 [2.34, 3.49] | 1.07 [0.82, 1.35] | -28.4% | +3.6% |
| esp32_real | — | 0 | pendiente: sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | | |

ESP32, tramo nocturno 29-oct (diseño, informativo, no es métrica final): n = 68

- persistencia: MAE 0.20 [0.00, 0.40]
- tendencia: MAE 0.58 [0.03, 1.21]
- holt: MAE 1.05 [0.44, 1.64]
- holt_pronostico: MAE 0.20 [0.00, 0.40]
- gbt: MAE 0.57 [0.12, 1.10]

## Pronóstico de nivel a 60 min (conjunto final)

| Nivel | Predictor | n | MAE % [IC 95 %] | RMSE % [IC 95 %] | MAPE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |
|---|---|---|---|---|---|---|---|
| consistencia | persistencia | 17887 | 2.66 [2.43, 2.94] | 6.53 [5.73, 7.42] | 3.67 [3.31, 4.08] |  |  |
| consistencia | tendencia | 17887 | 3.22 [2.83, 3.63] | 9.68 [8.69, 10.71] | 3.86 [3.43, 4.33] |  |  |
| consistencia | holt | 17887 | 3.10 [2.84, 3.40] | 6.93 [6.18, 7.77] | 3.84 [3.54, 4.21] | -16.3% | +3.8% |
| consistencia | holt_pronostico | 17887 | 2.93 [2.57, 3.30] | 9.45 [8.43, 10.53] | 3.57 [3.18, 4.02] | -10.0% | +9.0% |
| consistencia | gbt | 17887 | 1.84 [1.67, 2.06] | 4.97 [4.43, 5.54] | 2.90 [2.53, 3.32] | +30.7% | +42.7% |
| thingspeak_real | persistencia | 4290 | 1.13 [0.93, 1.36] | 3.08 [2.59, 3.58] | 1.92 [1.53, 2.37] |  |  |
| thingspeak_real | tendencia | 4290 | 1.99 [1.65, 2.39] | 4.91 [4.15, 5.70] | 3.17 [2.59, 3.88] |  |  |
| thingspeak_real | holt | 4290 | 1.77 [1.48, 2.10] | 3.82 [3.15, 4.49] | 2.77 [2.31, 3.36] | -56.8% | +11.1% |
| thingspeak_real | holt_pronostico | 4290 | 1.13 [0.93, 1.36] | 3.08 [2.59, 3.58] | 1.92 [1.53, 2.37] | +0.0% | +43.3% |
| thingspeak_real | gbt | 4290 | 1.30 [1.10, 1.53] | 3.02 [2.54, 3.48] | 2.06 [1.69, 2.50] | -14.7% | +35.0% |
| esp32_real | — | 0 | pendiente: sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | | |

ESP32, tramo nocturno 29-oct (diseño, informativo, no es métrica final): n = 59

- persistencia: MAE 0.48 [0.02, 0.76]
- tendencia: MAE 1.42 [0.11, 3.06]
- holt: MAE 1.96 [0.88, 2.76]
- holt_pronostico: MAE 0.48 [0.02, 0.76]
- gbt: MAE 0.94 [0.37, 1.33]

## Horas de autonomía hasta 10 % (conjunto final, solo casos no censurados)

| Nivel | Episodios | Llegan al umbral | Censurados | Orígenes evaluados | Predictor | MAE h [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|---|
| consistencia | 83 | 3 | 80 | 1368 | persistencia | 13.78 [8.13, 15.81] | 20% |
| consistencia | 83 | 3 | 80 | 1368 | tendencia | 13.97 [8.26, 16.46] | 20% |
| consistencia | 83 | 3 | 80 | 1368 | holt | 12.40 [8.66, 15.24] | 22% |
| consistencia | 83 | 3 | 80 | 1368 | holt_pronostico | 14.65 [8.25, 16.53] | 21% |
| consistencia | 83 | 3 | 80 | 1368 | gbt | 23.59 [16.09, 25.61] | 37% |
| thingspeak_real | 137 | 2 | 135 | 138 | persistencia | 29.64 [24.44, 38.50] | 38% |
| thingspeak_real | 137 | 2 | 135 | 138 | tendencia | 29.43 [25.30, 36.48] | 39% |
| thingspeak_real | 137 | 2 | 135 | 138 | holt | 35.57 [26.57, 50.92] | 46% |
| thingspeak_real | 137 | 2 | 135 | 138 | holt_pronostico | 64.11 [62.91, 66.16] | 95% |
| thingspeak_real | 137 | 2 | 135 | 138 | gbt | 46.56 [38.76, 59.86] | 62% |
| esp32_real | — | — | — | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | |

## Horas de autonomía hasta 25 % (conjunto final, solo casos no censurados)

| Nivel | Episodios | Llegan al umbral | Censurados | Orígenes evaluados | Predictor | MAE h [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|---|
| consistencia | 83 | 3 | 80 | 1183 | persistencia | 14.24 [8.48, 16.70] | 17% |
| consistencia | 83 | 3 | 80 | 1183 | tendencia | 14.23 [8.66, 17.41] | 17% |
| consistencia | 83 | 3 | 80 | 1183 | holt | 13.23 [9.58, 17.05] | 23% |
| consistencia | 83 | 3 | 80 | 1183 | holt_pronostico | 15.35 [8.05, 17.92] | 19% |
| consistencia | 83 | 3 | 80 | 1183 | gbt | 24.67 [16.56, 26.86] | 38% |
| thingspeak_real | 133 | 0 | 133 | 0 | — | sin casos no censurados | |
| esp32_real | — | — | — | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | |
