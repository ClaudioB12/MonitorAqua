"""
Marco de evaluación común (F6): los cuatro modelos (D2, D4, D6, D8) se reportan igual.

  Métricas    regresión: MAE, RMSE, MAPE · clasificación: precisión, exhaustividad, F1, exactitud
              Cada una con IC 95 % por bootstrap de bloques móviles: los errores de una serie de tiempo están
              autocorrelacionados y remuestrear puntos sueltos daría intervalos demasiado estrechos.
  Líneas base obligatorias, por tramo continuo (nunca cruzan un hueco):
              persistencia  ŷ(t+h) = y(t)
              tendencia     recta de mínimos cuadrados sobre las últimas k observaciones, extrapolada h pasos
              Todo modelo se compara contra las dos ("mejora_vs_*" = 1 - MAE_modelo / MAE_base).
  Niveles     consistencia     flota extrapolada, partición "consistencia" (nunca métrica final)
              thingspeak_real  tanques reales de ThingSpeak
              esp32_real       el tanque del equipo
              Se reportan siempre por separado; no se promedian entre niveles.
  Conjuntos   diseño  (por defecto) último 20 % del entrenamiento de cada sensor: para elegir y ajustar modelos
              prueba  (solo con final=True) partición "prueba" de batch/particion_modelos.yml. La del ESP32 es
                      intocable hasta la evaluación final; mientras la geometría sea provisional no existe.

Uso desde un modelo (F6-D2 en adelante):
    from evaluacion import conjunto, lineas_base, evaluar_regresion, reporte
    datos = conjunto(d2, "thingspeak_real")                       # conjunto de diseño
    base = lineas_base(datos, "nivel_pct", paso_ms=300_000, h=12)  # persistencia y tendencia
    base["pred_modelo"] = ...                                      # predicción del modelo, mismas filas
    resultado = reporte({"thingspeak_real": base}, ["pred_modelo"])

Demostración con las líneas base sobre D2 (sin entrenar ningún modelo):
    .venv/Scripts/python batch/evaluacion.py --demo-d2
"""
import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
ORO = RAIZ / "batch" / "datos" / "oro"
NIVELES = ("consistencia", "thingspeak_real", "esp32_real")
GRUPO = ["fuente", "sensorId", "runId", "tramo"]
BASES = ["pred_persistencia", "pred_tendencia"]


def leer_oro(tabla):
    t = pd.read_parquet(ORO / tabla)
    t["fuente"] = t["fuente"].astype(str)
    return t


def nivel_de(fuente):
    if fuente.startswith("aumentado"):
        return "consistencia"
    return {"thingspeak": "thingspeak_real", "esp32": "esp32_real"}.get(fuente)


def conjunto(tabla, nivel, final=False, frac_diseno=0.2):
    """Filas de evaluación de un nivel. final=False: conjunto de diseño (cola del entrenamiento, por sensor).
    final=True: partición de prueba (o de consistencia para los aumentados)."""
    t = tabla[tabla["fuente"].map(nivel_de) == nivel]
    if nivel == "consistencia":
        return t[t["particion"] == "consistencia"]  # la consistencia nunca es métrica final, no requiere final=True
    if final:
        return t[t["particion"] == "prueba"]
    ent = t[t["particion"] == "entrenamiento"]
    corte = ent.groupby(["sensorId"])["ts"].transform(lambda s: s.quantile(1 - frac_diseno))
    return ent[ent["ts"] >= corte]


def entrenamiento(tabla, nivel, frac_diseno=0.2):
    """Filas para ajustar un modelo: entrenamiento sin la cola de diseño (y nunca prueba ni embargo)."""
    t = tabla[(tabla["fuente"].map(nivel_de) == nivel) & (tabla["particion"] == "entrenamiento")]
    corte = t.groupby(["sensorId"])["ts"].transform(lambda s: s.quantile(1 - frac_diseno))
    return t[t["ts"] < corte] if nivel != "consistencia" else t


# --- Líneas base ---------------------------------------------------------------------------------------------

def lineas_base(datos, col, paso_ms, h, k=12, grupo=GRUPO):
    """Por cada origen t dentro de un tramo: y_real = y(t+h) y las dos líneas base. La rejilla es regular (paso_ms);
    los pasos sin dato quedan NaN y esas filas se descartan, sin interpolar."""
    salida = []
    grupo = [g for g in grupo if g in datos]
    for _, g in datos.dropna(subset=[col]).groupby(grupo, dropna=False):
        g = g.sort_values("ts")
        i = ((g["ts"] - g["ts"].iloc[0]) // paso_ms).astype(int).values
        y = np.full(i.max() + 1, np.nan)
        y[i] = g[col].values
        fut = np.full_like(y, np.nan)
        fut[:-h] = y[h:] if h < len(y) else np.nan
        tend = np.full_like(y, np.nan)
        x = np.arange(k, dtype=float)
        for j in i:
            if j + 1 >= k:
                w = y[j - k + 1:j + 1]
                ok = ~np.isnan(w)
                if ok.sum() >= max(3, k // 2):
                    a, b = np.polyfit(x[ok], w[ok], 1)
                    tend[j] = a * (k - 1 + h) + b
        r = g.copy()
        r["y_real"] = fut[i]
        r["pred_persistencia"] = y[i]
        r["pred_tendencia"] = tend[i]
        salida.append(r)
    if not salida:
        return pd.DataFrame()
    r = pd.concat(salida)
    return r.dropna(subset=["y_real", "pred_persistencia", "pred_tendencia"])


# --- Métricas ------------------------------------------------------------------------------------------------

def _bloques(n, largo, rng):
    """Índices de un remuestreo por bloques móviles de largo fijo (Künsch, 1989)."""
    largo = max(1, min(largo, n))
    inicios = rng.integers(0, n - largo + 1, size=int(np.ceil(n / largo)))
    return np.concatenate([np.arange(s, s + largo) for s in inicios])[:n]


def _ic(fn, arrays, largo, n_boot, semilla):
    rng = np.random.default_rng(semilla)
    n = len(arrays[0])
    if n < 2 * largo:  # con menos de dos bloques todos los remuestreos son la misma serie: el IC sería engañoso
        return [None, None]
    vals = [fn(*(a[ix] for a in arrays)) for ix in (_bloques(n, largo, rng) for _ in range(n_boot))]
    vals = [v for v in vals if v is not None and np.isfinite(v)]
    return [float(np.percentile(vals, 2.5)), float(np.percentile(vals, 97.5))] if vals else [None, None]


def _mae(y, p):
    return float(np.mean(np.abs(y - p)))


def _rmse(y, p):
    return float(np.sqrt(np.mean((y - p) ** 2)))


def _mape(y, p, minimo):
    m = np.abs(y) >= minimo  # el MAPE explota cerca de cero: se excluyen |y| < minimo y se informa cuántas quedan
    return float(np.mean(np.abs((y[m] - p[m]) / y[m])) * 100) if m.any() else None


def evaluar_regresion(y, pred, largo_bloque=12, n_boot=1000, semilla=2026, mape_minimo=1.0):
    y, pred = np.asarray(y, float), np.asarray(pred, float)
    ok = ~(np.isnan(y) | np.isnan(pred))
    y, pred = y[ok], pred[ok]
    if len(y) == 0:
        return {"n": 0}
    mape = lambda a, b: _mape(a, b, mape_minimo)
    return {"n": int(len(y)), "n_mape": int((np.abs(y) >= mape_minimo).sum()), "mape_minimo": mape_minimo,
            "largo_bloque": largo_bloque, "n_boot": n_boot,
            **{nombre: {"valor": fn(y, pred), "ic95": _ic(fn, (y, pred), largo_bloque, n_boot, semilla)}
               for nombre, fn in (("MAE", _mae), ("RMSE", _rmse), ("MAPE", mape))}}


def _clas(y, p):
    tp, fp, fn = int(np.sum(y & p)), int(np.sum(~y & p)), int(np.sum(y & ~p))
    prec = tp / (tp + fp) if tp + fp else None
    rec = tp / (tp + fn) if tp + fn else None
    f1 = 2 * prec * rec / (prec + rec) if prec and rec else (0.0 if prec is not None and rec is not None else None)
    return {"precision": prec, "exhaustividad": rec, "F1": f1, "exactitud": float(np.mean(y == p)),
            "vp": tp, "fp": fp, "fn": fn, "vn": int(np.sum(~y & ~p))}


def evaluar_clasificacion(y, pred, largo_bloque=1, n_boot=1000, semilla=2026):
    """largo_bloque=1 si cada fila es una unidad independiente (una noche en D6, un evento en D4)."""
    y, pred = np.asarray(y, bool), np.asarray(pred, bool)
    base = _clas(y, pred)
    out = {"n": int(len(y)), "positivos": int(y.sum()), "largo_bloque": largo_bloque, "n_boot": n_boot,
           "conteos": {k: base[k] for k in ("vp", "fp", "fn", "vn")}}
    for m in ("precision", "exhaustividad", "F1", "exactitud"):
        out[m] = {"valor": base[m], "ic95": _ic(lambda a, b, m=m: _clas(a, b)[m], (y, pred), largo_bloque, n_boot, semilla)}
    return out


# --- Reporte -------------------------------------------------------------------------------------------------

def reporte(por_nivel, modelos=(), col_y="y_real", **kw):
    """por_nivel: {nivel: DataFrame con y_real, pred_persistencia, pred_tendencia y las columnas de los modelos}.
    Devuelve un bloque por nivel (nunca mezclados) con las métricas de cada predictor y la mejora sobre las bases."""
    res = {}
    for nivel in NIVELES:
        d = por_nivel.get(nivel)
        if d is None or d.empty:
            res[nivel] = {"n": 0, "nota": "sin datos en este conjunto"}
            continue
        filas = {p: evaluar_regresion(d[col_y], d[p], **kw) for p in [*BASES, *modelos]}
        for p in modelos:
            for b in BASES:
                filas[p][f"mejora_vs_{b.removeprefix('pred_')}"] = 1 - filas[p]["MAE"]["valor"] / filas[b]["MAE"]["valor"]
        res[nivel] = {"n": int(len(d)), "sensores": sorted(d["sensorId"].unique().tolist()), "predictores": filas,
                      "uso": "consistencia (nunca métrica final)" if nivel == "consistencia" else "real"}
    return res


def tabla_markdown(res):
    lineas = ["| Nivel | Predictor | n | MAE [IC 95 %] | RMSE [IC 95 %] | MAPE % [IC 95 %] |", "|---|---|---|---|---|---|"]
    fmt = lambda m: "—" if m["valor"] is None else (f'{m["valor"]:.3f} [{m["ic95"][0]:.3f}, {m["ic95"][1]:.3f}]'
                                                    if m["ic95"][0] is not None else f'{m["valor"]:.3f} [n < 2 bloques]')
    for nivel, r in res.items():
        if not r.get("predictores"):
            lineas.append(f"| {nivel} | — | 0 | {r.get('nota', '')} | | |")
            continue
        for p, m in r["predictores"].items():
            lineas.append(f"| {nivel} | {p.removeprefix('pred_')} | {m['n']} | {fmt(m['MAE'])} | {fmt(m['RMSE'])} | {fmt(m['MAPE'])} |")
    return "\n".join(lineas)


def demo_d2(h_min=60, k=12, n_boot=500):
    """Líneas base de D2 (nivel % a h minutos) en los tres niveles, sobre el conjunto de diseño. Sin modelos."""
    d2 = leer_oro("d2_autonomia")
    paso = 300_000
    por_nivel = {n: lineas_base(conjunto(d2, n), "nivel_pct", paso, h_min * 60_000 // paso, k) for n in NIVELES}
    res = reporte(por_nivel, largo_bloque=12, n_boot=n_boot)
    out = {"generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "tabla": "d2_autonomia",
           "objetivo": f"nivel_pct a {h_min} min", "conjunto": "diseño (último 20 % del entrenamiento; consistencia "
           "para los aumentados)", "k_tendencia": k, "particion": d2["particion_version"].iloc[0], "niveles": res}
    destino = RAIZ / "batch" / "datos" / "evaluacion"
    destino.mkdir(parents=True, exist_ok=True)
    (destino / f"lineas_base_d2_h{h_min}.json").write_text(json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    ev = RAIZ / "docs" / "evidencias" / "f6"
    ev.mkdir(parents=True, exist_ok=True)
    shutil.copy(destino / f"lineas_base_d2_h{h_min}.json", ev / f"lineas_base_d2_h{h_min}.json")
    print(tabla_markdown(res))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--demo-d2", action="store_true")
    ap.add_argument("--horizonte-min", type=int, default=60)
    a = ap.parse_args()
    if a.demo_d2:
        demo_d2(a.horizonte_min)
