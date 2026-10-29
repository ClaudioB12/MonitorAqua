# Evidencia F6-D4: modelo de próxima llegada de agua

| Archivo | Qué es |
|---|---|
| `resultados_d4.json` | Salida completa de `batch/modelo_d4.py`. Por fuente de llegadas (detector f4.3 y subida sostenida) y por nivel incluye: episodios observados y censurados, evidencia, error en horas con IC por episodio, C-index global y por tanque, ventana P10-P90 antes y después de la calibración conforme, chequeo del sesgo de selección, desglose por tanque y probabilidad de llegada en 6/12/24 h (Brier, skill, calibración). También: caudal descriptivo, llegadas excluidas, resumen de los modelos y calibración |
| `tablas_d4.md` | Las mismas cifras en tablas |
| `manifiesto_modelos_d4.json` | Versión, fecha, partición y configuración |

**Datos:** las tablas oro `d4_llegadas` y `d2_autonomia` del snapshot de D2 (`../f6-d2/manifiesto_fuentes_d2.json`).

**Modelo para F7:** `batch/modelos/d4/d4.1-20261029/modelo.json`.

**Reproducir** (~2 min):

```bash
.venv/Scripts/python batch/modelo_d4.py
```
