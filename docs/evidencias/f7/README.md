# Evidencia F7: inferencia en vivo

| Archivo | Qué es |
|---|---|
| `verificacion_paneles.json` | `infra/verificar_tableros.py`: cada consulta de cada panel (BI real 24 h, BI aumentado, pipeline) con sus filas o "sin datos" |
| `recarga_y_reinicio.txt` | log de la recarga de un modelo sin reiniciar (`modelo_recargado`) y del reinicio que retoma desde el checkpoint (mismo id de consulta, estado del lote 13, lote 14 solo con lecturas nuevas) |
| `caida_sin_perdida.md` | la caída del primer despliegue no perdió lecturas: checkpoints retomados, bronce sin huecos, descartes por watermark explicados |
| `d8_3418755_variante_validada.md`, `verificacion_en_vivo_20261030_0409utc.json` | D8 en 3418755 ya usa la variante validada (04:00 UTC); estimación frente al agua real del DS18B20, en arranque del filtro. La verificación a 6 y 12 h sigue pendiente |
| `verificacion_en_vivo_<fecha>.json` | (pendiente, 31-oct) D8 contra el agua real de 3418755 y D4 con una llegada real: `infra/evidencia_f7_pendiente.py` |
| `alertas_estado.json` | estado y salud de las 6 reglas de alerta, incluida "La consulta de predicciones dejó de escribir" |
| `real_d2_autonomia.png`, `real_d4_proxima_llegada.png`, `real_d6_perdida.png`, `real_d8_agua.png` | las 4 tablas F7 con datos reales (24 h): ESP32 y ThingSpeak, con el motivo donde el modelo no aplica |
| `aumentado_d2_autonomia.png`, `aumentado_d4_proxima_llegada.png`, `aumentado_d6_perdida.png`, `aumentado_d8_agua.png` | las mismas tablas con la flota (5 al 7 de octubre simulados), con sus bandas |
| `*_vs_real_d2_nivel.png`, `*_vs_real_d4_error.png`, `*_vs_real_d6.png` | fila "Pronóstico vs real": el error en vivo de las predicciones pasadas |
| `pipeline_latencia_inferencia.png`, `pipeline_predicciones_por_lote.png` | observabilidad de la consulta `predicciones` |

Capturas con el renderizador oficial de Grafana (`lambda26-grafana-renderer`). Cómo fluye cada predicción:
`docs/inferencia-en-vivo.md`.
