# Evidencia F6-D8: temperatura del agua almacenada y residencia

| Archivo | Qué es |
|---|---|
| `resultados_d8.json` | Salida completa de `batch/modelo_d8.py`. Contiene los parámetros de las tres variantes (τ, ganancias, constante) y el τ del principal por bootstrap de días. Para el canal real 3418755: temperatura del agua ahora y a 6 y 12 h en el ajuste y en la prueba (modelo y líneas base, MAE y sesgo con IC por bloques de 24 h), cobertura de la banda P10-P90 y probabilidad de cruce en 12 h (Brier, skill, calibración). Además: precisión limitante del DHT11, transferencia a Juliaca (flota y ESP32: error de transferencia, no medición) y residencia por tanque con su relación con la temperatura |
| `tablas_d8.md` | Las mismas cifras en tablas |
| `manifiesto_modelos_d8.json` | Versión, fecha, partición (p6.2), canal, Open-Meteo usado, regla de la línea base y configuración |

**Datos:**
- **Tablas oro:** `d8_termica`, `d2_autonomia` y `d4_llegadas` (p6.2).
- **Open-Meteo Juliaca:** de la capa plata.
- **Open-Meteo de la ubicación del canal 3418755** (53.45, −2.51): misma API pública, descargado una vez en
  `batch/datos/fuentes/f8_open_meteo_3418755.json` (no versionado).

**Modelo para F7:** `batch/modelos/d8/d8.1-20261029/modelo.json` (se carga con `batch/termico.py`).

**Reproducir** (~35 s):

```bash
.venv/Scripts/python batch/modelo_d8.py
```
