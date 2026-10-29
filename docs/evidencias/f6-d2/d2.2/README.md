# Evidencia D2 versión d2.2: perfil de consumo

| Archivo | Qué es |
|---|---|
| `resultados_d2.2.json` | Salida de `batch/modelo_d2_perfil.py`. Por tanque, el resumen del perfil: celdas día × hora, hora o global, días de historia y consumo medio. La autonomía al 10 % y al 25 %: casos no censurados y censurados, MAE en horas (no censurados), C-index global y por tanque (todos) y la banda P10-P90 antes y después de la calibración conforme (`calibracion_banda`). El nivel a 15 y 60 min. Todo por nivel, con IC 95 % |
| `tablas_d2.2.md` | Las mismas cifras en tablas, con d2.1 lado a lado sobre las mismas filas |
| `manifiesto_modelos_d2.2.json` | Versión, fecha, base d2.1 usada y configuración |

**Cómo se generó:**
- Mismo snapshot de datos que d2.1 (`../manifiesto_fuentes_d2.json`).
- Holt y los GBT de d2.1 se re-aplicaron desde `batch/modelos/d2/d2.1-20261029/` **sin reentrenar**.
- El perfil se aprendió solo con la partición de entrenamiento de p6.1.

**Reproducir** (~2 min):

```bash
docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming:latest \
    spark-submit --driver-memory 3g batch/modelo_d2_perfil.py
```
