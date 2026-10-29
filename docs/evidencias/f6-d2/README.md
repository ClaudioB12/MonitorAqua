# Evidencia F6-D2: modelo de horas de autonomía

| Archivo | Qué es |
|---|---|
| `resultados_d2.json` | Salida completa de `batch/modelo_d2.py`. Incluye los parámetros elegidos, las grillas de ajuste, las importancias del GBT, el pronóstico a 15 y 60 min por nivel y la autonomía al 10 % y al 25 %, todo con IC 95 % |
| `tablas_d2.md` | Las mismas cifras en tablas, tal como las genera el script |
| `holt.json` | Parámetros de Holt por nivel, los que usará F7 (copia de `batch/modelos/d2/<version>/holt.json`) |
| `manifiesto_modelos.json` | Versión, fecha, variables del GBT, hiperparámetros elegidos y archivos del modelo |
| `manifiesto_fuentes_d2.json` | **Snapshot de datos** con el que se entrenó y evaluó: hora de descarga y lecturas por fuente |
| `calidad_snapshot_d2.json` | Reporte de calidad de `batch/capas.py` para ese snapshot (filas, válidas, tramos del ESP32) |

## Snapshot

Las fuentes se volvieron a descargar el 2026-10-29 a las 15:35 UTC (10:35 Lima) para incluir la sesión nocturna del
ESP32 (01:44-08:32, 6.8 h continuas). Plata y oro se regeneraron con el mismo `batch/capas.py` de F6, sin cambios.

Volver a descargar mueve la prueba de ThingSpeak, porque son los últimos 7 días de cada canal. Para repetir
exactamente estas cifras hace falta este mismo snapshot.

La evidencia de F6 común (`docs/evidencias/f6/`) no se tocó: corresponde a su propio snapshot.

## Reproducir

```bash
docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming:latest \
    spark-submit --driver-memory 3g batch/modelo_d2.py          # ~20 min
```
