# D8 en vivo en ThingSpeak 3418755 con la variante validada (2026-10-30)

**Qué se comprueba:**
- Desde el redespliegue de las 03:38 UTC, D8 en vivo usa en 3418755 la variante validada (aire de la cámara + Open-Meteo
  de su ubicación, 53.45, −2.51), no "solo aire".
- Su estimación del agua se compara con el agua real del canal (DS18B20, `field3` → `tempAguaC`).

**Estimaciones de D8 para `thingspeak-3418755`** (measurement `predicciones`, `tipo = estimacion`;
`verificacion_en_vivo_20261030_0409utc.json`):

| Hora (UTC) | Variante | Agua estimada | Banda p10-p90 | Aire de la cámara | Agua real (DS18B20, bronce) | Error |
|---|---|---|---|---|---|---|
| 03:00 | `solo_aire` (antes del redespliegue) | 9.66 °C | 9.00-10.47 | 10.22 °C | 10.31 °C (03:00:03) | −0.65 °C |
| **04:00** | **`aire_y_om`** (validada) | **8.94 °C** | 8.37-9.29 | 10.17 °C | **10.19 °C** (03:54:41 y 04:03:49) | **−1.25 °C** |

**Lectura honesta:**
- **Lo que ya está comprobado:** el canal tomó la variante validada en la primera hora completa tras el redespliegue.
  Desde entonces no emite el motivo "esperando Open-Meteo de su ubicación".
- **El error de la primera hora no mide el modelo:**
  - **Sigue en arranque:** el filtro térmico se reinició al cambiar de variante (otro τ) y lleva 1 h de historia, frente
    a las ~48 h que necesita para salir del arranque. La advertencia "filtro en arranque (1 h de aire)" lo dice.
  - **La banda no cubre el valor real:** p10-p90 es 8.37-9.29 °C y el agua real fue 10.19 °C. Es lo esperable en
    arranque, porque la banda de la prueba de F6 se calibró con el filtro estabilizado.
- **La evaluación que cuenta es la verificación a 6 y 12 h** (`tipo = verificacion`, panel "D8 · Agua: real vs
  pronosticada"):
  - **Primeros instantes objetivo:** 10:00 UTC (6 h) y 16:00 UTC (12 h) del 30-oct.
  - **Representativa:** desde el 1-nov, con el filtro estabilizado.
  - **Sigue pendiente,** junto con D4 contra una llegada real (`docs/inferencia-en-vivo.md` → *Pendiente*).

```bash
.venv/Scripts/python infra/evidencia_f7_pendiente.py --horas 48
```
