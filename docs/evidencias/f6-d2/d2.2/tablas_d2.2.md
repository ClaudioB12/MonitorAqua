# Resultados D2 d2.2 (con d2.1 lado a lado), 20261029

## Consumo previsto vs real (evidencia principal; conjunto final)

Ventanas desde un origen por hora; pasos dentro de un llenado excluidos (real y previsto sobre los mismos pasos). IC 95 % por bootstrap de bloques por tanque. Sesgo = previsto - real (> 0 sobreestima el consumo, es decir, subestima la autonomía).

| Nivel | Horizonte | Ventanas (con llenado; tanques) | Consumo real medio | Predictor | MAE % cap. [IC 95 %] | MAE litros [IC 95 %] | Sesgo % cap. [IC 95 %] |
|---|---|---|---|---|---|---|---|
| consistencia | 6 h | 1367 (331; 8) | 6.8 %, 71 L | consumo_medio | 5.7 [5.2, 6.1] | 68 [62, 74] | 1.9 [1.2, 2.7] |
| consistencia | 6 h | 1367 (331; 8) | 6.8 %, 71 L | perfil_p50 | 3.8 [3.4, 4.1] | 47 [42, 51] | 1.3 [0.8, 1.8] |
| consistencia | 6 h | 1367 (331; 8) | 6.8 %, 71 L | holt_d21 | 8.0 [7.7, 8.5] | 95 [91, 100] | -0.6 [-1.6, 0.4] |
| consistencia | 12 h | 1245 (588; 8) | 13.6 %, 142 L | consumo_medio | 8.5 [7.9, 9.1] | 103 [95, 112] | 3.8 [3.0, 5.3] |
| consistencia | 12 h | 1245 (588; 8) | 13.6 %, 142 L | perfil_p50 | 5.9 [5.4, 6.3] | 74 [67, 80] | 2.5 [1.8, 3.5] |
| consistencia | 12 h | 1245 (588; 8) | 13.6 %, 142 L | holt_d21 | 14.3 [13.7, 15.1] | 165 [160, 174] | -1.7 [-3.2, 0.9] |
| consistencia | 24 h | 1028 (953; 8) | 26.6 %, 272 L | consumo_medio | 9.9 [9.1, 11.0] | 126 [118, 138] | 8.6 [8.1, 10.4] |
| consistencia | 24 h | 1028 (953; 8) | 26.6 %, 272 L | perfil_p50 | 8.0 [7.0, 8.6] | 99 [90, 107] | 5.2 [4.6, 7.2] |
| consistencia | 24 h | 1028 (953; 8) | 26.6 %, 272 L | holt_d21 | 18.6 [17.7, 19.8] | 201 [193, 215] | -1.0 [-2.1, 1.1] |
| thingspeak_real | 6 h | 278 (57; 4) | 2.3 %, -61 L | consumo_medio | 4.2 [2.9, 5.8] | 786 [597, 1029] | -0.1 [-2.3, 1.4] |
| thingspeak_real | 6 h | 278 (57; 4) | 2.3 %, -61 L | perfil_p50 | 3.8 [2.3, 5.7] | 695 [512, 1004] | -1.4 [-3.5, 0.1] |
| thingspeak_real | 6 h | 278 (57; 4) | 2.3 %, -61 L | holt_d21 | 4.4 [3.1, 5.8] | 852 [618, 1099] | -0.3 [-1.9, 1.2] |
| thingspeak_real | 12 h | 245 (90; 4) | 6.0 %, 194 L | consumo_medio | 7.9 [5.5, 12.0] | 1279 [1036, 1599] | -1.4 [-6.6, 1.9] |
| thingspeak_real | 12 h | 245 (90; 4) | 6.0 %, 194 L | perfil_p50 | 7.4 [4.3, 12.2] | 1060 [863, 1436] | -4.2 [-9.2, -0.9] |
| thingspeak_real | 12 h | 245 (90; 4) | 6.0 %, 194 L | holt_d21 | 8.8 [6.3, 12.7] | 1486 [1172, 1897] | -1.4 [-5.4, 1.8] |
| thingspeak_real | 24 h | 170 (92; 4) | 14.5 %, 232 L | consumo_medio | 15.7 [12.6, 24.2] | 2710 [2550, 3296] | -5.2 [-16.1, -1.9] |
| thingspeak_real | 24 h | 170 (92; 4) | 14.5 %, 232 L | perfil_p50 | 15.7 [12.7, 27.0] | 1886 [1835, 2693] | -10.5 [-21.2, -6.4] |
| thingspeak_real | 24 h | 170 (92; 4) | 14.5 %, 232 L | holt_d21 | 19.5 [16.9, 28.8] | 2580 [2329, 3590] | -3.8 [-14.3, 1.5] |
| esp32_real | — | — | — | pendiente | | | |

## Autonomía: evidencia por umbral (conjunto final)

Evidencia suficiente: al menos 10 episodios no censurados. IC 95 % del MAE por bootstrap de episodios; con menos de 5 episodios se marca como no informativo.

| Umbral | Nivel | Episodios | No censurados: episodios (orígenes) | Censurados: episodios | Evidencia | Mejor MAE h | Banda calibrada: cobertura antes → después |
|---|---|---|---|---|---|---|---|
| 10 % | consistencia | 83 | 3 (1368) | 80 | **indicativa** | consumo_medio: 5.5 | 89% → 91% |
| 10 % | thingspeak_real | 137 | 2 (138) | 135 | **indicativa** | consumo_medio: 2.4 | 59% → 77% |
| 10 % | esp32_real | — | — | — | pendiente | | |
| 25 % | consistencia | 83 | 3 (1183) | 80 | **indicativa** | consumo_medio: 11.9 | 34% → 35% |
| 25 % | thingspeak_real | 133 | 0 (0) | 133 | **indicativa** | — | — |
| 25 % | esp32_real | — | — | — | pendiente | | |
| 50 % | consistencia | 83 | 5 (1182) | 78 | **indicativa** | consumo_medio: 25.9 | 0% → 0% |
| 50 % | thingspeak_real | 130 | 4 (39) | 126 | **indicativa** | persistencia_vel_d21: 57.7 | 0% → 0% |
| 50 % | esp32_real | — | — | — | pendiente | | |

## Horas de autonomía hasta 10 % (conjunto final)

| Nivel | Predictor | Versión / rol | MAE h [IC 95 % por episodio] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|
| **consistencia** | evidencia **indicativa** | episodios 83: 3 no censurados (1368 orígenes), 80 censurados | C-index: 1463 orígenes (114 no censurados, 1349 censurados) | | | |
| consistencia | perfil_p50 | d22 | 7.3 (IC no informativo: 3 episodios (< 5)) | 0.989 [0.978, 0.999] | 0.958 [0.928, 0.992] | 0% |
| consistencia | consumo_medio | base d2.2 | 5.5 (IC no informativo: 3 episodios (< 5)) | 0.994 [0.989, 1.000] | 0.984 [0.955, 1.000] | 0% |
| consistencia | holt_d21 | base d2.2 (d2.1 principal) | 12.4 (IC no informativo: 3 episodios (< 5)) | 0.969 [0.945, 0.986] | 0.923 [0.879, 0.970] | 22% |
| consistencia | persistencia_vel_d21 | d2.1 | 13.8 (IC no informativo: 3 episodios (< 5)) | 0.912 [0.875, 0.962] | 0.844 [0.791, 0.914] | 20% |
| consistencia | tendencia_d21 | d2.1 | 14.0 (IC no informativo: 3 episodios (< 5)) | 0.906 [0.867, 0.960] | 0.842 [0.785, 0.908] | 20% |
| consistencia | gbt_d21 | d2.1 | 23.6 (IC no informativo: 3 episodios (< 5)) | 0.775 [0.699, 0.928] | 0.708 [0.638, 0.872] | 37% |
| consistencia | banda P10-P90 del perfil | d2.2 | antes: cobertura 89%, ancho 25.7 h | después (conforme): cobertura 91%, ancho 27.3 h | | |
| **thingspeak_real** | evidencia **indicativa** | episodios 137: 2 no censurados (138 orígenes), 135 censurados | C-index: 304 orígenes (12 no censurados, 292 censurados) | | | |
| thingspeak_real | perfil_p50 | d22 | 4.5 (IC no informativo: 2 episodios (< 5)) | 0.991 [0.972, 1.000] | 0.955 [0.807, 1.000] | 0% |
| thingspeak_real | consumo_medio | base d2.2 | 2.4 (IC no informativo: 2 episodios (< 5)) | 0.987 [0.960, 1.000] | 0.935 [0.719, 1.000] | 0% |
| thingspeak_real | holt_d21 | base d2.2 (d2.1 principal) | 35.6 (IC no informativo: 2 episodios (< 5)) | 0.779 [0.699, 0.837] | 0.677 [0.533, 0.863] | 46% |
| thingspeak_real | persistencia_vel_d21 | d2.1 | 29.6 (IC no informativo: 2 episodios (< 5)) | 0.926 [0.860, 0.985] | 0.822 [0.600, 0.907] | 38% |
| thingspeak_real | tendencia_d21 | d2.1 | 29.4 (IC no informativo: 2 episodios (< 5)) | 0.870 [0.727, 0.986] | 0.776 [0.496, 0.938] | 39% |
| thingspeak_real | gbt_d21 | d2.1 | 46.6 (IC no informativo: 2 episodios (< 5)) | 0.671 [0.474, 0.839] | 0.605 [0.325, 0.795] | 62% |
| thingspeak_real | banda P10-P90 del perfil | d2.2 | antes: cobertura 59%, ancho 24.5 h | después (conforme): cobertura 77%, ancho 49.6 h | | |
| esp32_real | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | |

## Horas de autonomía hasta 25 % (conjunto final)

| Nivel | Predictor | Versión / rol | MAE h [IC 95 % por episodio] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|
| **consistencia** | evidencia **indicativa** | episodios 83: 3 no censurados (1183 orígenes), 80 censurados | C-index: 1448 orígenes (99 no censurados, 1349 censurados) | | | |
| consistencia | perfil_p50 | d22 | 14.1 (IC no informativo: 3 episodios (< 5)) | 0.990 [0.980, 0.998] | 0.951 [0.900, 0.986] | 0% |
| consistencia | consumo_medio | base d2.2 | 11.9 (IC no informativo: 3 episodios (< 5)) | 0.995 [0.992, 1.000] | 0.983 [0.958, 1.000] | 0% |
| consistencia | holt_d21 | base d2.2 (d2.1 principal) | 17.7 (IC no informativo: 3 episodios (< 5)) | 0.930 [0.907, 0.954] | 0.872 [0.825, 0.928] | 25% |
| consistencia | persistencia_vel_d21 | d2.1 | 18.3 (IC no informativo: 3 episodios (< 5)) | 0.825 [0.772, 0.932] | 0.750 [0.660, 0.856] | 23% |
| consistencia | tendencia_d21 | d2.1 | 18.7 (IC no informativo: 3 episodios (< 5)) | 0.813 [0.749, 0.929] | 0.745 [0.640, 0.854] | 23% |
| consistencia | gbt_d21 | d2.1 | 28.8 (IC no informativo: 3 episodios (< 5)) | 0.668 [0.520, 0.878] | 0.615 [0.468, 0.797] | 42% |
| consistencia | banda P10-P90 del perfil | d2.2 | antes: cobertura 34%, ancho 29.0 h | después (conforme): cobertura 35%, ancho 30.8 h | | |
| **thingspeak_real** | evidencia **indicativa** | episodios 133: 0 no censurados (0 orígenes), 133 censurados | C-index: 290 orígenes (0 no censurados, 290 censurados) | | | |
| thingspeak_real | perfil_p50 | d22 | — | — | — | — |
| thingspeak_real | consumo_medio | base d2.2 | — | — | — | — |
| thingspeak_real | holt_d21 | base d2.2 (d2.1 principal) | — | — | — | — |
| thingspeak_real | persistencia_vel_d21 | d2.1 | — | — | — | — |
| thingspeak_real | tendencia_d21 | d2.1 | — | — | — | — |
| thingspeak_real | gbt_d21 | d2.1 | — | — | — | — |
| esp32_real | — | pendiente | sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional (por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento f4.3, geometria_desde) | | | |

## Horas de autonomía hasta 50 % (conjunto final)

| Nivel | Predictor | Versión / rol | MAE h [IC 95 % por episodio] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |
|---|---|---|---|---|---|---|
| **consistencia** | evidencia **indicativa** | episodios 83: 5 no censurados (1182 orígenes), 78 censurados | C-index: 1403 orígenes (99 no censurados, 1304 censurados) | | | |
| consistencia | perfil_p50 | d22 | 28.6 [22.3, 33.1] | 0.945 [0.895, 0.981] | 0.774 [0.556, 0.949] | 0% |
| consistencia | consumo_medio | base d2.2 | 25.9 [20.7, 30.0] | 0.944 [0.894, 0.987] | 0.836 [0.677, 0.992] | 0% |
| consistencia | holt_d21 | base d2.2 (d2.1 principal) | 34.0 [29.7, 37.3] | 0.836 [0.782, 0.881] | 0.779 [0.705, 0.857] | 39% |
| consistencia | persistencia_vel_d21 | d2.1 | 28.8 [24.9, 31.1] | 0.775 [0.714, 0.829] | 0.678 [0.565, 0.768] | 24% |
| consistencia | tendencia_d21 | d2.1 | 29.0 [25.3, 30.9] | 0.769 [0.702, 0.825] | 0.673 [0.564, 0.764] | 24% |
| consistencia | gbt_d21 | d2.1 | 35.9 [31.5, 40.2] | 0.637 [0.462, 0.767] | 0.539 [0.420, 0.674] | 35% |
| consistencia | banda P10-P90 del perfil | d2.2 | antes: cobertura 0%, ancho 32.2 h | después (conforme): cobertura 0%, ancho 34.4 h | | |
| **thingspeak_real** | evidencia **indicativa** | episodios 130: 4 no censurados (39 orígenes), 126 censurados | C-index: 247 orígenes (3 no censurados, 244 censurados) | | | |
| thingspeak_real | perfil_p50 | d22 | 71.0 (IC no informativo: 4 episodios (< 5)) | 0.500 [0.500, 0.500] | 0.500 [0.500, 0.500] | 100% |
| thingspeak_real | consumo_medio | base d2.2 | 59.7 (IC no informativo: 4 episodios (< 5)) | 0.999 [0.992, 0.999] | 0.667 [0.667, 0.667] | 0% |
| thingspeak_real | holt_d21 | base d2.2 (d2.1 principal) | 60.1 (IC no informativo: 4 episodios (< 5)) | 0.812 [0.779, 0.828] | 0.667 [0.667, 0.667] | 77% |
| thingspeak_real | persistencia_vel_d21 | d2.1 | 57.7 (IC no informativo: 4 episodios (< 5)) | 0.810 [0.775, 0.824] | 0.333 [0.333, 0.333] | 77% |
| thingspeak_real | tendencia_d21 | d2.1 | 57.9 (IC no informativo: 4 episodios (< 5)) | 0.803 [0.759, 0.819] | 0.333 [0.333, 0.333] | 74% |
| thingspeak_real | gbt_d21 | d2.1 | 67.4 (IC no informativo: 4 episodios (< 5)) | 0.641 [0.598, 0.660] | 0.500 [0.500, 0.500] | 85% |
| thingspeak_real | banda P10-P90 del perfil | d2.2 | antes: cobertura 0%, ancho 45.1 h | después (conforme): cobertura 0%, ancho 57.0 h | | |
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
