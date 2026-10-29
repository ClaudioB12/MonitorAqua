# Plan de fusión de las ramas U2 en `main`

**Estado: ejecutado el 2026-10-29** (aprobado por el equipo).

| Resultado | |
|---|---|
| Punto de retorno | Etiqueta `pre-fusion-u2` = `abd316f` (el `main` anterior) |
| Fusiones | 10 commits de fusión, de `8207485` (u2-fase0) a `c7aa73a` (u2-colector-nube), **sin conflictos**; `main` se movió solo al final |
| Comprobaciones | `contratos/validar.py`: 0 fallos · `mkdocs build`: ok · `.env` ya no está versionado |
| Imágenes | `monitoraqua-streaming` y `monitoraqua-ingesta` reconstruidas desde `main`. Se recrearon el puente, el colector, el consumidor de eventos, el de volumen y Spark. Spark retomó sus 6 consultas con los mismos `id` y el puente retomó desde su estado |
| `verificar_stack.py` | Stack completo y operativo |
| Workflow | Usa el código de `main` (se quitó `ref: vars.COLECTOR_REF`). La variable se elimina a mano en GitHub |

El texto de abajo es la propuesta original, tal como se aprobó.

## Situación (2026-10-29)

- **Las fases forman una cadena lineal** que sale del commit `e974b82` de `main`: `u2-fase0 ⊂ u2-fase1 ⊂ u2-fase2 ⊂
  u2-fase2b ⊂ u2-fase3 ⊂ u2-fase4 ⊂ u2-fase5 ⊂ u2-fase6`. Cada una contiene a la anterior.
- **De `u2-fase6` salen dos ramas hermanas:** `u2-fase6-d2` (modelo D2) y `u2-colector-nube` (colector en la nube,
  reintentos a InfluxDB, arranque diario, recarga al arranque).
- **`main` solo avanzó dos commits desde `e974b82`:** el workflow del colector (`c788a67`) y su ventana de 6 h
  (`abd316f`). Su contenido es idéntico al de `u2-colector-nube`.

## Simulación (sin tocar ramas ni archivos)

Con `git merge-tree --write-tree` se simuló la secuencia completa, en orden, sobre lo ya commiteado. Ninguna
fusión tuvo conflictos.

| # | Fusión | Conflictos | Qué cambia en `main` |
|---|---|---|---|
| 1 | `u2-fase0` | No | Estructura U2, contrato, `.env.example`, `.gitignore` corregido. **Desversiona** la basura de U1 (`pyspark/.local`, `.npm/_logs`, `.ipynb_checkpoints`, `.jupyter`, `.ipython`) y **el `.env`** |
| 2-8 | `u2-fase1` … `u2-fase6` | No | Una fase por fusión; como la cadena es lineal, cada una solo agrega sus commits |
| 9 | `u2-fase6-d2` | No | Modelo D2 (`batch/holt.py`, `modelo_d2.*`, evidencia) y bootstrap por grupos en `evaluacion.py` |
| 10 | `u2-colector-nube` | No (`CLAUDE.md` se combina solo) | Colector en la nube y recarga al arranque, reintentos a InfluxDB, `docs/arranque-diario.md` |

**Conflictos esperados:**
- **`CLAUDE.md`:** es el único archivo que tocan las dos ramas hermanas. Hoy git lo combina solo, porque los
  cambios están en secciones distintas.
  - Con los commits pendientes (recarga al arranque en `u2-colector-nube` y d2.2 en `u2-fase6-d2`), las dos ramas
    vuelven a tocarlo. Puede aparecer un conflicto si quedan en líneas vecinas.
  - Regla para resolverlo: **conservar los dos bloques**.
  - La simulación se repite justo antes de ejecutar.
- **`.env` de `main`:** estaba versionado desde `s2-datos` (solo `JUPYTER_TOKEN`). La fusión 1 lo saca del
  seguimiento, pero **queda en el historial**. Se recomienda cambiar ese token de Jupyter; no hay credenciales de
  Firebase en el historial.
- **Workflow:** `.github/workflows/colector_apis.yml` es idéntico en `main` y en `u2-colector-nube`, así que no hay
  conflicto.

## Procedimiento propuesto

0. **Antes:**
   - Hacer los commits pendientes aprobados.
   - Dejar el árbol de trabajo limpio.
   - Marcar el punto de retorno con `git tag pre-fusion-u2 main`.
   - Repetir la simulación.
1. **Fusiones sin cambiar de rama en la carpeta de trabajo**, con `merge-tree` + `commit-tree` + `update-ref`,
   como en la simulación:
   - Un commit de fusión por rama, en el orden de la tabla y con el mensaje "Fusiona u2-faseN: …".
   - Así se ve en el historial dónde empieza y termina cada fase.
   - No se hace `git switch main` mientras `main` no tenga el código U2: la carpeta perdería `ingesta/` y
     `streaming/` por un momento, y el contenedor de Spark monta esos archivos de configuración.
   - Si alguna fusión diera conflicto, se detiene ahí y se resuelve a mano. `main` no se mueve hasta que las 10
     estén listas.
2. **`git switch main`.** Desde `u2-colector-nube` solo aparecen los archivos de D2. Después:
   - `python contratos/validar.py`
   - `infra/verificar_stack.py`
   - `mkdocs build`
3. **Reconstruir las imágenes desde `main`**, solo esos contenedores (los checkpoints y volúmenes se conservan):
   - **Spark:** `docker compose -f infra/compose.yml --env-file .env up -d --build --no-deps aqua-streaming`.
     Comprobar que las 6 consultas retoman con los mismos `id` (`docs/arranque-diario.md`, paso 3).
   - **Ingesta:** `build aqua-puente` y `up -d --no-deps --force-recreate aqua-colector`. El puente no cambió de
     código desde F2; se recrea solo si se quiere dejar todo en la misma imagen.
4. **`git push origin main`.**
5. **Eliminar la variable `COLECTOR_REF`** en GitHub (Settings → Secrets and variables → Actions → Variables),
   **después** del push. Antes de eso, el workflow tomaría `main` sin el código del colector y fallaría. Luego,
   *Run workflow* para comprobar que corre desde `main`.
6. **Commit en `main` que actualiza la documentación:** quitar de `CLAUDE.md`, `docs/ingesta.md` y del comentario
   del workflow las notas de "`COLECTOR_REF` temporal".
7. **Las ramas `u2-fase*` no se borran:** son la evidencia de cada fase. Opcionalmente, una etiqueta por fase.

**Si algo sale mal:**
- **Antes del push:** `git update-ref refs/heads/main pre-fusion-u2` deja `main` como estaba.
- **Después del push:** se revierten los commits de fusión. No se reescribe el historial publicado.
