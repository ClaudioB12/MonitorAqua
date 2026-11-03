# Notebooks de sustentación U2

Un notebook por dimensión, para mostrar en la sustentación el modelo, su validación y su funcionamiento en vivo.

| Notebook | Dimensión | Integrante | Modelo versionado |
|---|---|---|---|
| `d2_autonomia.ipynb` | D2 · Horas de autonomía | Claudio Bustinza Inofuente | `batch/modelos/d2/d2.2-20261029` (base d2.1) |
| `d4_llegadas.ipynb` | D4 · Próxima llegada de agua | Wilbert Alex Mayta Arotaype | `batch/modelos/d4/d4.1-20261029` |
| `d6_fugas.ipynb` | D6 · Pérdida por fuga | Joel Huillca Lima | `batch/modelos/d6/d6.1-20261029` |
| `d8_temperatura.ipynb` | D8 · Temperatura del agua y residencia | Josué Gabriel Ochoa Mamani | `batch/modelos/d8/d8.1-20261029` |

## Regla: sin modelos propios

**De dónde sale cada cosa:**
- **Modelos y evaluación:** se importan de `batch/` (`modelo_dX.py`, `evaluacion.py`).
- **Parámetros:** se leen de `batch/modelos/`.
- **`soporte_u2.py`:** solo contiene infraestructura de presentación: rutas, consulta a InfluxDB, comparación con lo publicado, R² y la lectura de las limitaciones de `docs/modelo-dX.md`.

**Qué verifica cada notebook:**
- **Reentrenamiento:** reentrena con las funciones de `batch/` y comprueba que los parámetros salgan idénticos a los guardados.
- **Resultados:** recalcula todos los resultados y los compara número por número con
  `docs/evidencias/f6-dX/resultados_*.json`.
- **Tablas:** regenera la tabla publicada (`tablas_*.md`, la misma de `docs/modelo-dX.md`) para comprobar que sale igual.

## Secciones

1. **Pregunta y datos:** tabla oro, partición p6.2 y cantidad por nivel.
2. **Líneas base.**
3. **Modelo:** carga y reentrenamiento verificado. En D2 y D6, el entrenamiento distribuido con Spark MLlib.
4. **Resultados por nivel:** flota = consistencia, ThingSpeak = real, ESP32 = pendiente. Con MAE, RMSE, R² e IC frente a las líneas base, y la comparación con lo publicado.
5. **Gráficos:** pronóstico frente a real, residuos y cobertura de la banda.
6. **En vivo:** últimas predicciones de F7 en InfluxDB y su verificación de pronóstico frente a real.
7. **Limitaciones:** copiadas de `docs/modelo-dX.md`.

## Cómo abrirlos

Usan el Jupyter de U1 con un override que no modifica `pyspark/`. Desde la raíz del repo:

```bash
docker compose -f pyspark/compose.yml -f notebooks/u2/jupyter.override.yml up -d
```

**Abrir:** http://localhost:4489/lab?token=sintoken → `MonitorAqua/notebooks/u2/`.

**Requisitos:**
- los datos locales de `batch/datos/` (capas plata y oro, fuentes, caché de D6), que no se versionan;
- el stack levantado, para la sección "En vivo";
- dependencias: la primera celda instala `pyarrow` y `matplotlib` en el kernel.

**Ejecución de punta a punta, sin abrir el navegador:**

```bash
docker exec -w /work/MonitorAqua/notebooks/u2 pyspark-oficial-pyspark-1 \
    jupyter nbconvert --to notebook --execute --inplace d8_temperatura.ipynb
```

**Tiempos aproximados:** D8 ~1 min, D4 ~1 min, D6 ~1 min (con la caché de corridas) y D2 ~12 min. D2 tarda más porque reentrena los GBT con Spark MLlib.
