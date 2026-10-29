# Tablas f4.4 frente a f4.3

Semillas: 2026, 7, 11. Versión vieja tomada de git: `b400e7d`.

## 1. Consistencia y sensibilidad (datos aumentados, conteos sumados de las semillas)

| Variante | Anomalía → evento | Episodios | f4.3 precisión | f4.3 recall | f4.4 precisión | f4.4 recall |
|---|---|---|---|---|---|---|
| base | fuga_lenta → alerta_fuga | 13 | 0.50 [0.19-0.81] | 0.23 [0.08-0.50] | 0.75 [0.30-0.95] | 0.23 [0.08-0.50] |
| base | corte_prolongado → corte_prolongado | 3 | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] |
| base | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 0.92 [0.67-0.99] | 1.00 [0.76-1.00] |
| base | falla_sensor → falla_sensor | 76 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| base | todas las llegadas → llegada_agua | 333 | — | 0.99 [0.98-1.00] | — | 0.99 [0.98-1.00] |
| base | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| ruido_x2 | fuga_lenta → alerta_fuga | 13 | 0.33 [0.12-0.65] | 0.23 [0.08-0.50] | 0.60 [0.23-0.88] | 0.23 [0.08-0.50] |
| ruido_x2 | corte_prolongado → corte_prolongado | 3 | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] |
| ruido_x2 | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 0.92 [0.67-0.99] | 1.00 [0.76-1.00] | 0.92 [0.67-0.99] | 1.00 [0.76-1.00] |
| ruido_x2 | falla_sensor → falla_sensor | 76 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| ruido_x2 | todas las llegadas → llegada_agua | 333 | — | 0.99 [0.98-1.00] | — | 0.99 [0.98-1.00] |
| ruido_x2 | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| ruido_x3 | fuga_lenta → alerta_fuga | 13 | 0.17 [0.06-0.39] | 0.23 [0.08-0.50] | 0.33 [0.10-0.70] | 0.15 [0.04-0.42] |
| ruido_x3 | corte_prolongado → corte_prolongado | 3 | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] | 0.10 [0.04-0.23] | 1.00 [0.44-1.00] |
| ruido_x3 | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 0.86 [0.60-0.96] | 1.00 [0.76-1.00] |
| ruido_x3 | falla_sensor → falla_sensor | 76 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| ruido_x3 | todas las llegadas → llegada_agua | 333 | — | 0.99 [0.98-1.00] | — | 0.98 [0.96-0.99] |
| ruido_x3 | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| llenado_umbral | fuga_lenta → alerta_fuga | 13 | 0.50 [0.19-0.81] | 0.23 [0.08-0.50] | 0.75 [0.30-0.95] | 0.23 [0.08-0.50] |
| llenado_umbral | corte_prolongado → corte_prolongado | 3 | 0.11 [0.04-0.26] | 1.00 [0.44-1.00] | 0.11 [0.04-0.25] | 1.00 [0.44-1.00] |
| llenado_umbral | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 0.92 [0.67-0.99] | 1.00 [0.76-1.00] |
| llenado_umbral | falla_sensor → falla_sensor | 76 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| llenado_umbral | todas las llegadas → llegada_agua | 333 | — | 1.00 [0.99-1.00] | — | 1.00 [0.98-1.00] |
| llenado_umbral | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| borde_franja | fuga_lenta → alerta_fuga | 13 | 0.50 [0.19-0.81] | 0.23 [0.08-0.50] | 0.75 [0.30-0.95] | 0.23 [0.08-0.50] |
| borde_franja | corte_prolongado → corte_prolongado | 3 | 0.16 [0.06-0.35] | 1.00 [0.44-1.00] | 0.16 [0.06-0.35] | 1.00 [0.44-1.00] |
| borde_franja | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.70-1.00] | 0.75 [0.47-0.91] | 0.90 [0.60-0.98] | 0.75 [0.47-0.91] |
| borde_franja | falla_sensor → falla_sensor | 76 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| borde_franja | todas las llegadas → llegada_agua | 333 | — | 1.00 [0.98-1.00] | — | 1.00 [0.98-1.00] |
| borde_franja | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| f2b2 | fuga_lenta → alerta_fuga | 12 | 0.80 [0.49-0.94] | 0.33 [0.14-0.61] | 0.90 [0.60-0.98] | 0.42 [0.19-0.68] |
| f2b2 | corte_prolongado → corte_prolongado | 19 | 0.72 [0.54-0.85] | 1.00 [0.83-1.00] | 0.69 [0.51-0.83] | 0.95 [0.75-0.99] |
| f2b2 | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 0.92 [0.67-0.99] | 1.00 [0.76-1.00] |
| f2b2 | falla_sensor → falla_sensor | 80 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| f2b2 | todas las llegadas → llegada_agua | 337 | — | 1.00 [0.99-1.00] | — | 1.00 [0.98-1.00] |
| f2b2 | deriva_termica → alerta_fuga FALSA | 9 | — | 0 con alerta falsa | — | 0 con alerta falsa |
| f2b3 | fuga_lenta → alerta_fuga | 12 | 0.80 [0.49-0.94] | 0.42 [0.19-0.68] | 0.86 [0.49-0.97] | 0.33 [0.14-0.61] |
| f2b3 | corte_prolongado → corte_prolongado | 19 | 0.72 [0.54-0.85] | 1.00 [0.83-1.00] | 0.69 [0.51-0.83] | 0.95 [0.75-0.99] |
| f2b3 | llegada_atipica → llegada_agua (fueraDeFranja) | 12 | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] | 1.00 [0.76-1.00] |
| f2b3 | falla_sensor → falla_sensor | 80 | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] | 1.00 [0.95-1.00] |
| f2b3 | todas las llegadas → llegada_agua | 337 | — | 1.00 [0.99-1.00] | — | 1.00 [0.98-1.00] |
| f2b3 | deriva_termica → alerta_fuga FALSA | 9 | — | 1 con alerta falsa | — | 0 con alerta falsa |

## 2. Historial real (conjunto de DISEÑO, no rendimiento)

| Tanque | Lecturas | Eventos f4.3 | Eventos f4.4 |
|---|---|---|---|
| esp32-tanque-real-01 | 15,711 | autonomia_baja 3, corte_prolongado 4, falla_sensor 16, llegada_agua 13 | falla_sensor 16 |
| thingspeak-1233810 | 8,000 | autonomia_baja 4, corte_prolongado 1 | autonomia_baja 4, corte_prolongado 19, llegada_agua 31 |
| thingspeak-1233815 | 8,000 | corte_prolongado 1 | corte_prolongado 1 |
| thingspeak-283977 | 8,000 | corte_prolongado 1, falla_sensor 1 | corte_prolongado 3, falla_sensor 1, llegada_agua 3 |
| thingspeak-3418755 | 20,211 | autonomia_baja 1, corte_prolongado 16, llegada_agua 49 | autonomia_baja 1, corte_prolongado 21, llegada_agua 65 |
| thingspeak-480876 | 8,000 | corte_prolongado 2, falla_sensor 24, llegada_agua 3, umbral_termico 1 | corte_prolongado 2, falla_sensor 24, llegada_agua 3, umbral_termico 1 |

### ESP32: eventos que desaparecen o aparecen

| Evento | Inicio (Lima) | f4.3 | f4.4 | Criterio / referencia |
|---|---|---|---|---|
| corte_prolongado | 10-01 23:10 | sí | — | inicio_del_flujo |
| corte_prolongado | 10-08 22:33 | sí | — | ultima_llegada |
| llegada_agua | 10-08 22:33 | sí | — | rapido |
| autonomia_baja | 10-15 20:46 | sí | — |  |
| llegada_agua | 10-15 20:48 | sí | — | rapido |
| corte_prolongado | 10-15 20:48 | sí | — | ultima_llegada |
| llegada_agua | 10-22 19:44 | sí | — | rapido |
| corte_prolongado | 10-22 19:44 | sí | — | ultima_llegada |
| llegada_agua | 10-28 17:21 | sí | — | rapido |
| llegada_agua | 10-28 18:44 | sí | — | rapido |
| llegada_agua | 10-28 21:46 | sí | — | rapido |
| llegada_agua | 10-28 23:08 | sí | — | rapido |
| llegada_agua | 10-28 23:55 | sí | — | rapido |
| llegada_agua | 10-29 00:27 | sí | — | rapido |
| llegada_agua | 10-29 00:44 | sí | — | rapido |
| autonomia_baja | 10-29 00:53 | sí | — |  |
| llegada_agua | 10-29 01:12 | sí | — | rapido |
| llegada_agua | 10-29 01:46 | sí | — | rapido |
| llegada_agua | 10-29 09:23 | sí | — | rapido |
| autonomia_baja | 10-29 10:35 | sí | — |  |

(`falla_sensor` iguales en las dos versiones: 16.)
