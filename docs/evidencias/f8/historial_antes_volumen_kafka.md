# Historial de Kafka antes de darle volumen persistente (F8, 2026-10-30 ~03:50 UTC)

**Hallazgo:** `lambda26-kafka` (apache/kafka:4.3.1) escribe en `/tmp/kafka-logs`, dentro de la capa del contenedor.

- **El volumen anónimo no se usa:** la imagen declara `/var/lib/kafka/data` como volumen, pero ahí solo hay 4 KB, y
  ningún `meta.properties`.
- **Hoy:** los datos sobreviven a `docker restart` y al reinicio del PC.
- **Se pierden** si el contenedor se recrea (`docker compose up` con cambios, `down`/`up`).

**Comprobación (solo lectura; consumidor sin grupo, sin confirmar offsets):**

| Tópico | En Kafka | Dónde más está | Resultado |
|---|---|---|---|
| `aqua-eventos-real` | 25,618 mensajes (25,568 `eventId` únicos), desde 2026-10-23 02:02 UTC | bronce (`data/local/bronce`, 28,200 filas) y Firebase (`/board1/lecturas/historial`, 30,261 registros) | **Faltan en bronce solo 16**, todos del último minuto (03:48), en vuelo hasta el siguiente lote de 1 min. **Faltan en Firebase solo 2**: `prueba-f3-duplicado-7c6dedcc` y `prueba-f3-tardio-f0c5fc05`, inyectados directo a Kafka en las pruebas de F3; están en bronce |
| `aqua-eventos-aumentados` | 172,800 | `batch/datos/aumentados/runId=r7-79a6b935` (Parquet) | Regenerable: el generador es determinista (misma configuración y semilla, mismos eventos) |
| `servicio-eventos` | 87 | InfluxDB (`eventos_servicio`, 30 días) | **Solo en Kafka e InfluxDB:** se respaldan en `servicio_eventos_respaldo.json` antes del cambio |

**Conclusión:** el bronce y Firebase tienen todo el historial de lecturas reales. Darle volumen a Kafka no arriesga
datos si se respalda `servicio-eventos`. Aun así, lo propuesto copia `/tmp/kafka-logs` completo al volumen nuevo: se
conservan offsets e ids, y los checkpoints de Spark siguen siendo válidos.
