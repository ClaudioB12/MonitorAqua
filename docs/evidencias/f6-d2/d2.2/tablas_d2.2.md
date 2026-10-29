# Resultados D2 d2.2 (con d2.1 lado a lado), 20261029

## Horas de autonomía hasta 10 % (conjunto final)

| Nivel | Predictor | Versión / rol | MAE h, no censurados [IC 95 %] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|
| **consistencia** | casos | episodios 83 (llegan 3, censurados 80) | no censurados: 1368 orígenes de 3 episodios | C-index: 1463 orígenes (114 no censurados, 1349 censurados) | | |
| consistencia | perfil_p50 | d22 | 7.3 [5.1, 12.5] | 0.989 [0.978, 0.999] | 0.958 [0.928, 0.992] | 0% |
| consistencia | consumo_medio | base d2.2 | 5.5 [4.7, 9.4] | 0.994 [0.989, 1.000] | 0.984 [0.955, 1.000] | 0% |
| consistencia | holt_d21 | base d2.2 (d2.1 principal) | 12.4 [8.7, 15.2] | 0.969 [0.945, 0.986] | 0.923 [0.879, 0.970] | 22% |
| consistencia | persistencia_vel_d21 | d2.1 | 13.8 [8.1, 15.8] | 0.912 [0.875, 0.962] | 0.844 [0.791, 0.914] | 20% |
| consistencia | tendencia_d21 | d2.1 | 14.0 [8.3, 16.5] | 0.906 [0.867, 0.960] | 0.842 [0.785, 0.908] | 20% |
| consistencia | gbt_d21 | d2.1 | 23.6 [16.1, 25.6] | 0.775 [0.699, 0.928] | 0.708 [0.638, 0.872] | 37% |
| consistencia | banda P10-P90 del perfil | d2.2 | antes: cobertura 89%, ancho 25.7 h | después (conforme): cobertura 91%, ancho 27.3 h | | |
| **thingspeak_real** | casos | episodios 137 (llegan 2, censurados 135) | no censurados: 138 orígenes de 2 episodios | C-index: 304 orígenes (12 no censurados, 292 censurados) | | |
| thingspeak_real | perfil_p50 | d22 | 4.5 [3.7, 5.9] | 0.991 [0.972, 1.000] | 0.955 [0.807, 1.000] | 0% |
| thingspeak_real | consumo_medio | base d2.2 | 2.4 [1.9, 3.1] | 0.987 [0.960, 1.000] | 0.935 [0.719, 1.000] | 0% |
| thingspeak_real | holt_d21 | base d2.2 (d2.1 principal) | 35.6 [26.6, 50.9] | 0.779 [0.699, 0.837] | 0.677 [0.533, 0.863] | 46% |
| thingspeak_real | persistencia_vel_d21 | d2.1 | 29.6 [24.4, 38.5] | 0.926 [0.860, 0.985] | 0.822 [0.600, 0.907] | 38% |
| thingspeak_real | tendencia_d21 | d2.1 | 29.4 [25.3, 36.5] | 0.870 [0.727, 0.986] | 0.776 [0.496, 0.938] | 39% |
| thingspeak_real | gbt_d21 | d2.1 | 46.6 [38.8, 59.9] | 0.671 [0.474, 0.839] | 0.605 [0.325, 0.795] | 62% |
| thingspeak_real | banda P10-P90 del perfil | d2.2 | antes: cobertura 59%, ancho 24.5 h | después (conforme): cobertura 77%, ancho 49.6 h | | |
| esp32_real | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | |

## Horas de autonomía hasta 25 % (conjunto final)

| Nivel | Predictor | Versión / rol | MAE h, no censurados [IC 95 %] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|
| **consistencia** | casos | episodios 83 (llegan 3, censurados 80) | no censurados: 1183 orígenes de 3 episodios | C-index: 1448 orígenes (99 no censurados, 1349 censurados) | | |
| consistencia | perfil_p50 | d22 | 14.1 [11.9, 17.5] | 0.990 [0.980, 0.998] | 0.951 [0.900, 0.986] | 0% |
| consistencia | consumo_medio | base d2.2 | 11.9 [11.4, 13.9] | 0.995 [0.992, 1.000] | 0.983 [0.958, 1.000] | 0% |
| consistencia | holt_d21 | base d2.2 (d2.1 principal) | 17.7 [13.1, 21.9] | 0.930 [0.907, 0.954] | 0.872 [0.825, 0.928] | 25% |
| consistencia | persistencia_vel_d21 | d2.1 | 18.3 [11.2, 21.7] | 0.825 [0.772, 0.932] | 0.750 [0.660, 0.856] | 23% |
| consistencia | tendencia_d21 | d2.1 | 18.7 [11.4, 22.6] | 0.813 [0.749, 0.929] | 0.745 [0.640, 0.854] | 23% |
| consistencia | gbt_d21 | d2.1 | 28.8 [21.8, 31.8] | 0.668 [0.520, 0.878] | 0.615 [0.468, 0.797] | 42% |
| consistencia | banda P10-P90 del perfil | d2.2 | antes: cobertura 34%, ancho 29.0 h | después (conforme): cobertura 35%, ancho 30.8 h | | |
| **thingspeak_real** | casos | episodios 133 (llegan 0, censurados 133) | no censurados: 0 orígenes de 0 episodios | C-index: 290 orígenes (0 no censurados, 290 censurados) | | |
| thingspeak_real | perfil_p50 | d22 | — | — | — | — |
| thingspeak_real | consumo_medio | base d2.2 | — | — | — | — |
| thingspeak_real | holt_d21 | base d2.2 (d2.1 principal) | — | — | — | — |
| thingspeak_real | persistencia_vel_d21 | d2.1 | — | — | — | — |
| thingspeak_real | tendencia_d21 | d2.1 | — | — | — | — |
| thingspeak_real | gbt_d21 | d2.1 | — | — | — | — |
| esp32_real | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | |

## Pronóstico de nivel a 15 min (conjunto final; mismas filas para todos)

| Nivel | Predictor | n | MAE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |
|---|---|---|---|---|---|
| consistencia | persistencia | 18094 | 0.72 [0.65, 0.80] |  |  |
| consistencia | tendencia | 18094 | 0.99 [0.86, 1.13] |  |  |
| consistencia | perfil | 18094 | 0.60 [0.52, 0.68] | +16.9% | +39.4% |
| consistencia | holt_d21 | 18094 | 0.81 [0.73, 0.88] | -11.6% | +18.7% |
| consistencia | gbt_d21 | 18094 | 0.41 [0.37, 0.45] | +43.3% | +58.7% |
| thingspeak_real | persistencia | 2485 | 1.02 [0.76, 1.33] |  |  |
| thingspeak_real | tendencia | 2485 | 1.28 [0.99, 1.60] |  |  |
| thingspeak_real | perfil | 2485 | 1.04 [0.77, 1.35] | -1.3% | +19.0% |
| thingspeak_real | holt_d21 | 2485 | 1.31 [1.03, 1.61] | -27.7% | -2.1% |
| thingspeak_real | gbt_d21 | 2485 | 1.25 [0.95, 1.59] | -22.1% | +2.3% |
| esp32_real | — | 0 | pendiente | | |

## Pronóstico de nivel a 60 min (conjunto final; mismas filas para todos)

| Nivel | Predictor | n | MAE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |
|---|---|---|---|---|---|
| consistencia | persistencia | 17887 | 2.66 [2.43, 2.94] |  |  |
| consistencia | tendencia | 17887 | 3.22 [2.83, 3.63] |  |  |
| consistencia | perfil | 17887 | 1.99 [1.75, 2.27] | +25.3% | +38.2% |
| consistencia | holt_d21 | 17887 | 3.10 [2.84, 3.40] | -16.3% | +3.8% |
| consistencia | gbt_d21 | 17887 | 1.84 [1.67, 2.06] | +30.7% | +42.7% |
| thingspeak_real | persistencia | 3392 | 1.30 [1.08, 1.56] |  |  |
| thingspeak_real | tendencia | 3392 | 2.19 [1.78, 2.65] |  |  |
| thingspeak_real | perfil | 3392 | 1.35 [1.12, 1.60] | -3.8% | +38.4% |
| thingspeak_real | holt_d21 | 3392 | 1.77 [1.48, 2.09] | -36.3% | +19.1% |
| thingspeak_real | gbt_d21 | 3392 | 1.43 [1.21, 1.68] | -10.1% | +34.6% |
| esp32_real | — | 0 | pendiente | | |
