# Evidencia f4.4: detector de eventos de servicio, versión f4.4 frente a f4.3

| Archivo | Qué es |
|---|---|
| `comparativa_f43_f44.json` | Salida de `streaming/comparar_versiones.py`: (1) consistencia y sensibilidad de f4.3 y f4.4 sobre los **mismos** datos aumentados (6 variantes × 3 semillas: 2026, 7 y 11), con IC 95 % de Wilson; (2) el historial real de la capa plata (ESP32 y ThingSpeak) pasado por las dos versiones, evento por evento |
| `tablas_f44.md` | Las mismas cifras en tablas |

**El historial real es conjunto de DISEÑO.** Con él se encontraron las tres limitaciones de f4.3 y se ajustaron las
reglas de f4.4, así que muestra que los falsos positivos conocidos desaparecen, **no** el rendimiento real de f4.4. El
rendimiento real empieza en el congelamiento de f4.4 (`streaming/reglas_servicio.yml` → `congelamiento`).

**Reproducir** (~25 min; la versión vieja se toma de git):

```bash
.venv/Scripts/python streaming/comparar_versiones.py --ref b400e7d --reglas-nuevas streaming/reglas_servicio.yml \
    --json docs/evidencias/f4/f4.4/comparativa_f43_f44.json
.venv/Scripts/python streaming/tablas_comparativa.py docs/evidencias/f4/f4.4/comparativa_f43_f44.json > docs/evidencias/f4/f4.4/tablas_f44.md
```

Incluye las variantes `f2b2` (fugas persistentes) y `f2b3` (f2b.2 con la compensación térmica en el firmware, como el
ESP32 real: DHT11 con sesgo de ±2 °C y redondeo a grados enteros). En `f2b3` la versión vieja (f4.3) compensa dos
veces, que es lo que hacía con el ESP32.
