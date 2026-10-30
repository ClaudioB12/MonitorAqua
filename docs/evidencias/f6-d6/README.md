# Evidencia F6-D6: pérdida proyectada por fuga

| Archivo | Qué es |
|---|---|
| `resultados_d6.json` | Salida completa de `batch/modelo_d6.py`. Contiene el control de la reconstrucción contra la tabla oro `d6_noches` y el entrenamiento (umbral, factor de persistencia, coeficientes, clasificación en el ajuste). Para la flota f2b.2 y f2b.3: detección por episodio y por noche (modelo y líneas base, IC de Wilson y bootstrap), recall según la tasa real, error de la tasa (L/h, IC por episodio), pérdida a 24 h y 7 días (todas las noches y solo las noches con fuga), cobertura de la banda P10-P90 y sesgo de selección. Además: ThingSpeak (descenso de la noche siguiente; diseño, prueba y fuera de alcance), las noches de diseño del ESP32 y la sensibilidad mínima detectable |
| `tablas_d6.md` | Las mismas cifras en tablas |
| `manifiesto_modelos_d6.json` | Versión, fecha, partición (p6.2), reglas de la línea base (f4.4), corridas de la flota (runId, separadas en entrenamiento y evaluación) y configuración |
| `mllib_vs_numpy.json` | El mismo modelo entrenado con Spark MLlib (`batch/modelo_d6_mllib.py`): coeficientes, probabilidades y métricas de evaluación frente a numpy (diferencias de ~10⁻⁷, ninguna decisión distinta) |

**Datos:**
- **Plata y oro (`d6_noches`):** regeneradas el 29-oct con p6.2, la compensación en el firmware y el sensor por
  periodo.
- **Flota:** f2b.2 y f2b.3 reconstruidas con el mismo código de la capa oro: 12 semillas de entrenamiento y 12
  distintas de evaluación, las mismas en los dos generadores. La semilla 7 de f2b.2 reproduce la tabla oro
  exactamente.
- **Todo lo de la flota es consistencia, no rendimiento real:** no hay ninguna fuga real etiquetada.

**Modelo para F7:** `batch/modelos/d6/d6.1-20261029/modelo.json` (se carga con `batch/fuga.py`).

**Reproducir** (~4 min la primera vez; las corridas de la flota quedan en `batch/datos/d6_cache/`, no versionado):

```bash
.venv/Scripts/python batch/modelo_d6.py
MSYS_NO_PATHCONV=1 docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming \
  spark-submit --driver-memory 2g batch/modelo_d6_mllib.py
```
