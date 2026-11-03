"""
Soporte común de los notebooks de sustentación U2 (notebooks/u2/). No contiene ningún modelo: los modelos y su
evaluación se importan de batch/ y los parámetros se leen de batch/modelos/. Aquí solo hay:
  - rutas del repo para importar batch/, ingesta/ y streaming/;
  - consulta a InfluxDB (measurement predicciones de F7) con el token de .env, sin mostrarlo;
  - comparación numérica de lo recalculado contra lo publicado (docs/evidencias/f6-dX/resultados_*.json);
  - R², formato de métricas e intervalos;
  - la sección "Limitaciones" copiada tal cual de docs/modelo-dX.md.
"""
import io
import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import requests

RAIZ = Path(__file__).resolve().parents[2]
for _d in ("batch", "ingesta", "streaming"):
    if str(RAIZ / _d) not in sys.path:
        sys.path.insert(0, str(RAIZ / _d))


# --- InfluxDB -----------------------------------------------------------------------------------------------------

def _env():
    env = {}
    for linea in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if linea and not linea.startswith("#") and "=" in linea:
            k, v = linea.split("=", 1)
            env[k.strip()] = v.strip()
    return env


def influx(flux):
    """Ejecuta una consulta Flux y devuelve un DataFrame. Prueba la red de Docker y luego el puerto del host."""
    env = _env()
    cuerpo = {"query": flux, "dialect": {"annotations": [], "header": True}}
    cab = {"Authorization": f"Token {env.get('INFLUX_TOKEN', '')}", "Content-Type": "application/json",
           "Accept": "application/csv"}
    ultimo = None
    for url in ("http://influxdb:8086", "http://localhost:48086"):
        try:
            r = requests.post(f"{url}/api/v2/query", params={"org": env.get("INFLUX_ORG", "horeb")}, json=cuerpo,
                              headers=cab, timeout=60)
            r.raise_for_status()
            texto = r.text.strip()
            if not texto:
                return pd.DataFrame()
            df = pd.read_csv(io.StringIO(texto))
            df = df[df.get("result", pd.Series(dtype=str)) != "result"] if "result" in df else df
            return df.drop(columns=[c for c in df.columns if c in ("", "result", "table") or c.startswith("Unnamed")])
        except requests.RequestException as ex:
            ultimo = ex
    raise RuntimeError(f"InfluxDB no responde: {type(ultimo).__name__}")


def predicciones(dimension, tipo, bucket="monitoraqua", horas=48, campos=None, limite=None):
    """Puntos de F7 (measurement predicciones) de una dimensión y tipo, pivoteados por campo."""
    filtro = "" if not campos else " and (" + " or ".join(f'r._field == "{c}"' for c in campos) + ")"
    q = f'''from(bucket: "{bucket}") |> range(start: -{horas}h)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "{dimension}" and r.tipo == "{tipo}"{filtro})
  |> pivot(rowKey: ["_time", "sensorId", "version_modelo"], columnKey: ["_field"], valueColumn: "_value")
  |> group() |> sort(columns: ["_time"], desc: true)'''
    if limite:
        q += f" |> limit(n: {limite})"
    df = influx(q)
    if len(df) and "_time" in df:
        df["_time"] = pd.to_datetime(df["_time"], format="ISO8601").dt.tz_convert("America/Lima")
    return df


# --- Comparación con lo publicado ---------------------------------------------------------------------------------

def normalizar(x):
    return json.loads(json.dumps(x, default=lambda o: o.item() if hasattr(o, "item") else float(o)))


def comparar(calc, pub, tol=1e-6, ruta="", ignorar=()):
    """Recorre las dos estructuras y compara cada número. Devuelve (iguales, total, diferencias)."""
    calc, pub = normalizar(calc), normalizar(pub)
    iguales, total, difs = 0, 0, []

    def rec(a, b, r):
        nonlocal iguales, total
        if any(i in r for i in ignorar):
            return
        if isinstance(b, dict):
            for k, v in b.items():
                rec(a.get(k) if isinstance(a, dict) else None, v, f"{r}/{k}")
        elif isinstance(b, list) and b and all(isinstance(x, (int, float, type(None))) for x in b):
            for i, v in enumerate(b):
                rec(a[i] if isinstance(a, list) and i < len(a) else None, v, f"{r}[{i}]")
        elif isinstance(b, (int, float)) and not isinstance(b, bool):
            total += 1
            if isinstance(a, (int, float)) and not isinstance(a, bool) and (
                    (math.isnan(a) and math.isnan(b)) or abs(a - b) <= tol * max(1.0, abs(b))):
                iguales += 1
            else:
                difs.append((r, a, b))
    rec(calc, pub, ruta)
    return iguales, total, difs


def informe_comparacion(nombre, calc, pub, **kw):
    i, t, d = comparar(calc, pub, **kw)
    print(f"{nombre}: {i} de {t} números idénticos a lo publicado" + ("" if not d else f" · {len(d)} distintos"))
    for r, a, b in d[:15]:
        print(f"   {r}: recalculado {a} · publicado {b}")
    return i, t, d


def misma_tabla(texto_calc, ruta_publicada, ignorar=()):
    """¿La tabla markdown regenerada con los resultados recalculados es idéntica a la publicada? ignorar: las filas que
    contienen alguno de estos textos se informan aparte (p. ej. "esp32": sin periodo de prueba, fuera de las métricas)."""
    pub = Path(ruta_publicada).read_text(encoding="utf-8")
    fuera = lambda t: [x for x in t.strip().splitlines() if any(i in x for i in ignorar)]  # noqa: E731
    dentro = lambda t: [x for x in t.strip().splitlines() if not any(i in x for i in ignorar)]  # noqa: E731
    igual = dentro(texto_calc) == dentro(pub)
    nota = f" (salvo {len(fuera(pub))} filas con {', '.join(ignorar)})" if ignorar else ""
    print(f"{Path(ruta_publicada).name}: {'idéntica' if igual else 'DISTINTA'} a la regenerada con los datos de hoy{nota}")
    if ignorar and fuera(texto_calc) != fuera(pub):
        for x, y in zip(fuera(texto_calc), fuera(pub)):
            if x != y:
                print("   fila excluida, hoy:     ", x[:160])
                print("   fila excluida, publicada:", y[:160])
    if not igual:
        a, b = dentro(texto_calc), dentro(pub)
        for x, y in zip(a, b):
            if x != y:
                print("   recalculado:", x[:160])
                print("   publicado:  ", y[:160])
                break
    return igual


# --- Métricas y formato -------------------------------------------------------------------------------------------

def r2(y, p):
    y, p = np.asarray(y, float), np.asarray(p, float)
    ok = ~(np.isnan(y) | np.isnan(p))
    y, p = y[ok], p[ok]
    if len(y) < 2 or np.var(y) == 0:
        return float("nan")
    return float(1 - np.sum((y - p) ** 2) / np.sum((y - y.mean()) ** 2))


def fm(mt, dec=2):
    """'valor [lo, hi]' de una métrica de batch/evaluacion.py."""
    if not mt or mt.get("valor") is None:
        return "—"
    ic = mt.get("ic95") or [None, None]
    s = f"{mt['valor']:.{dec}f}"
    return s + (f" [{ic[0]:.{dec}f}, {ic[1]:.{dec}f}]" if ic[0] is not None else "")


# --- Limitaciones ---------------------------------------------------------------------------------------------------

def secciones(doc, patrones):
    """Texto de las secciones de docs/modelo-dX.md cuyo título contiene alguno de los patrones (tal cual)."""
    lineas = (RAIZ / "docs" / doc).read_text(encoding="utf-8").splitlines()
    out, dentro, nivel = [], False, 0
    for ln in lineas:
        m = re.match(r"^(#+)\s+(.*)", ln)
        if m:
            n = len(m.group(1))
            if dentro and n <= nivel:
                dentro = False
            if any(p.lower() in m.group(2).lower() for p in patrones):
                dentro, nivel = True, n
        if dentro:
            out.append(ln)
    return "\n".join(out)
