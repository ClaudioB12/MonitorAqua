# Evidencias de F5 (tableros y alertas), 2026-10-29

Detalle en `docs/tableros.md`.

| Archivo | Contenido |
|---|---|
| `verificacion_paneles.json` | cada panel y anotación de los dos tableros, ejecutado con la API de Grafana: filas devueltas, "sin datos" o error, en tres escenarios (BI real 24 h, BI aumentado 28-sep a 28-oct, pipeline 24 h) |
| `alertas_estado.json` | las 5 reglas de alerta: salud, estado e instancias (por consulta y por origen) |
| `bi_real_24h.png` | tablero BI, `origen = real`, últimas 24 h |
| `bi_aumentado_30d.png` | tablero BI, `origen = aumentado`, 28-sep a 28-oct |
| `bi_aumentado_fuga_zonaA02.png` | tablero BI, `aumentado`, 3 días de `flota-zonaA-02` |
| `pipeline_24h.png` | tablero de salud del pipeline, últimas 24 h |
| `pipeline_latencia.png` | latencia de ingesta p50 y p95 por origen |
| `alertas.png` | Grafana → Alerting: 5 reglas provisionadas, todas en Normal |

Las capturas las genera el renderizador oficial de Grafana (`lambda26-grafana-renderer`), no son capturas de pantalla
a mano. Se reproducen con los comandos de `docs/tableros.md`.
