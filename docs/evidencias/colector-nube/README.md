# Verificación del colector en la nube (2026-10-29)

Prueba de que el workflow `colector-apis` (GitHub Actions) escribe en Firebase por sí solo, sin el colector local.

## Procedimiento

1. **01:33:46 (Lima):** se detuvo el colector local con `docker stop lambda26-aqua-colector`, sin borrarlo. Desde
   ese momento ninguna escritura de APIs en Firebase puede venir de la PC.
2. **~01:36:** el equipo lanzó a mano el workflow `colector-apis` (ejecución #2, re-run). Terminó en verde en 34 s.
3. **01:37:25:** se contaron, por origen, las entradas de `/board1/lecturas/historial` con `ingestaTs` de los
   últimos 5 min (`verificar_ingesta.py`, solo lectura; imprime conteos, nunca credenciales).
4. **01:37:41:** se volvió a encender el colector local con `docker start lambda26-aqua-colector`. Inició sesión
   y completó su pasada en vivo normal.

## Resultado (`consulta_ingestaTs_5min_2026-10-29_0137.txt`)

| Origen | `ingestaTs` en los últimos 5 min | Escritas hace |
|---|---|---|
| thingspeak | 310 | 76 s |
| ea-uk | 161 | 59-76 s |
| usgs | 58 | 76 s |
| open-meteo | 9 | 76 s |
| **Total** | **538** | |
| esp32-fisico | 0 (el firmware no escribe `ingestaTs`; 1,877 lecturas propias en las últimas 3 h) | — |

- **Las 538 escrituras son del workflow.** Ocurrieron entre las 01:36:09 y las 01:36:26, con el colector local
  detenido, y coinciden con la ejecución #2.
- **Es una pasada en modo ventana:** trae todas las lecturas de las últimas 2 h, no solo la última. El colector en
  vivo escribe unas 40 por pasada; en seco, el modo ventana había dado 537 el mismo día.
- **EA UK tiene un rango de 59-76 s:** sus 23 estaciones se consultan en paralelo y la última respondió unos 17 s
  después que las demás.

## Ejecución #1

La consulta de línea de base, hecha a las 01:34, ya mostraba 515 escrituras en modo ventana a las ~01:31. Fue la
ejecución #1 del workflow, antes de detener el colector local. Por eso la prueba se repitió con el colector
detenido (ejecución #2): así no queda duda de dónde vienen las escrituras.

## Reproducir

```bash
docker run --rm --env-file .env -v "$PWD:/repo" -v "$PWD/docs/evidencias/colector-nube/verificar_ingesta.py:/verificar.py:ro" \
  monitoraqua-ingesta:latest python /verificar.py 5
```

## Recarga del hueco nocturno (2026-10-29, 10:06 Lima)

La primera noche GitHub no disparó el cron entre las 02:07 y las 08:07, y las APIs quedaron sin lecturas en Firebase
entre las 01:48 y las 06:30. Por eso se recargó ese hueco a mano.

- **Script:** `recarga_hueco_2026-10-29.py`. Es el colector en modo ventana, filtrado a (01:47:58, 09:15] y solo con
  las claves que aún no existían en Firebase.
- **Salida:** `recarga_hueco_2026-10-29_salida.txt`.

Resultado: 1,641 lecturas nuevas (ThingSpeak 871, EA UK 513, USGS 236, Open-Meteo 21). El puente las publicó todas
en Kafka, sin duplicados, y el bronce las guardó. Fuera del watermark, no aparecen en los agregados en vivo. Detalle
en `docs/ingesta.md` → *Recarga puntual del hueco nocturno*.
