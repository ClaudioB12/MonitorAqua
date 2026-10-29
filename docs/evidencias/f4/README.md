# Evidencias de F4 (eventos de servicio), 2026-10-29

Tres niveles de validación, que no se mezclan (detalle en `docs/eventos-servicio.md`, sección Validación):

1. **Consistencia detector-generador.** Datos aumentados: generador y detector comparten definiciones, así que la
   cifra mide consistencia de implementación, no rendimiento real.
2. **Sensibilidad.** Variantes que rompen esos supuestos compartidos.
3. **Rendimiento real.** Bitácora de acciones sobre el tanque del ESP32 contra los eventos detectados.

Las proporciones llevan un intervalo de confianza de 95 % de Wilson (`*_ic95`). Reglas: `streaming/reglas_servicio.yml`
versión **f4.3, congelada desde 2026-10-28 23:37:40 (Lima)**.

| Archivo | Nivel | Contenido |
|---|---|---|
| `eventos_con_compensacion.jsonl` | 1 | eventos que detectó Spark al reproducir `aqua-eventos-aumentados` (172,800 lecturas, `origenLecturas = aumentado`) |
| `eventos_sin_compensacion.jsonl` | 1 | lo mismo con la compensación térmica apagada (`--sin-compensacion`) |
| `validacion_con_compensacion.json`, `validacion_sin_compensacion.json` | 1 | `consistencia_precision` y `consistencia_recall` por tipo, con episodios, eventos e IC 95 % |
| `consistencia_f2b1_vs_f2b2_semilla7.json` | 1 | semilla 7, reglas f4.3: generador f2b.1 frente a f2b.2 (fugas persistentes y llegadas omitidas etiquetadas) |
| `sensibilidad.json` | 2 | 5 variantes (base, ruido ×2, ruido ×3, llenado en el umbral, borde de franja) × 3 semillas: conteos sumados, IC 95 % y recall sobre todas las llegadas efectivas |
| `bitacora_real.csv` | 3 | **bitácora de pruebas, la llena el equipo**: `timestamp, accion (llenado/vaciado/falla/no_verificable), sensorId, timestamp_fin, observador, nota` |
| `bitacora_resultado_f4.1_en_vivo.json` | 3 | bitácora contra lo publicado en vivo en `servicio-eventos` (reglas f4.1) |
| `bitacora_resultado_f4.2_reproceso.json` | 3 | bitácora contra las reglas f4.2 corridas sobre las mismas lecturas reales de bronce |
| `bitacora_resultado_f4.3_reproceso.json` | 3 | f4.3 (congelada) sobre bronce, separado en **conjunto de diseño** (antes del congelamiento) y **rendimiento real** (después; hoy sin acciones) |
| `consumidor_relectura.txt` | — | salida legible del consumidor al releer `servicio-eventos` (51 eventos reales, reglas f4.1) |

## Cómo regenerarlas

```bash
# 1. Consistencia (dentro del contenedor de Spark; publica en un tópico de prueba que después se borra)
docker exec -w /app/streaming lambda26-aqua-streaming spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 validar_detector.py [--sin-compensacion]
.venv/Scripts/python streaming/evaluar_detector.py data/local/validacion/eventos_con_compensacion.jsonl \
  --json docs/evidencias/f4/validacion_con_compensacion.json

# 2. Sensibilidad
.venv/Scripts/python streaming/sensibilidad.py --json docs/evidencias/f4/sensibilidad.json

# 3. Rendimiento real (después de anotar las pruebas en bitacora_real.csv)
.venv/Scripts/python streaming/comparar_bitacora.py               # contra lo publicado en vivo
.venv/Scripts/python streaming/comparar_bitacora.py --reproceso   # reglas vigentes sobre las lecturas de bronce
```

## Cómo anotar una prueba en la bitácora

Una fila por acción, con la hora en que **empezó** (ISO 8601 con zona horaria de Lima):

```csv
2026-10-29T10:15:00-05:00,llenado,esp32-tanque-real-01,2026-10-29T10:17:30-05:00,Claudio,"llené el recipiente con jarra"
2026-10-29T10:25:00-05:00,vaciado,esp32-tanque-real-01,2026-10-29T10:27:00-05:00,Claudio,"vacié hasta el fondo"
```

`observador = sistema` marca las filas registradas desde los propios datos. Esas filas no son independientes y el
script las cuenta aparte. `accion = no_verificable` marca lo que nadie puede confirmar: queda fuera del cálculo,
igual que los eventos a ±5 min de esa fila.

**Solo cuentan como rendimiento real las filas posteriores al congelamiento** (`streaming/reglas_servicio.yml` →
`congelamiento.desde`). Las anteriores son el conjunto de diseño de las reglas.
