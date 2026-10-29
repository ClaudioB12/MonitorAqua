"""
F6-D2: modelo de horas de autonomía (dimensión de Claudio). Entrena en batch y deja los modelos listos para F7.

  Entrada   batch/datos/oro/d2_autonomia (F6, paso de 5 min, tramos continuos, columna "particion" de p6.1)
  Modelos   principal: Holt (nivel + tendencia, batch/holt.py) -> nivel futuro y horas hasta el umbral
            comparación: GBTRegressor de Spark MLlib con rezagos, medias móviles, hora del día y tiempo desde el
            último llenado -> nivel a 15 y 60 min
  Ajuste    solo con la partición "entrenamiento": su último 20 % (por sensor y corrida) es el conjunto de AJUSTE
            para elegir hiperparámetros; después se reentrena con todo el entrenamiento. Prueba y consistencia
            solo se miran en la evaluación final.
  Evaluación batch/evaluacion.py, por nivel (consistencia, ThingSpeak real, ESP32 real) y siempre contra
            persistencia y tendencia lineal
  Salidas   batch/modelos/d2/<version>-<fecha>/ (holt.json, manifiesto.json y los GBT de Spark, que no se versionan)
            docs/evidencias/f6-d2/resultados_d2.json y tablas_d2.md

  docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming \
      spark-submit --driver-memory 3g batch/modelo_d2.py
"""
import json
import math
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "batch"))
import evaluacion as ev  # noqa: E402
from holt import PASO_MS, PASOS_POR_HORA, TENDENCIA_MIN_PCT_H, Holt, horas_hasta  # noqa: E402

CFG = yaml.safe_load((RAIZ / "batch" / "modelo_d2.yml").read_text(encoding="utf-8"))
SERIE = ["fuente", "sensorId", "runId", "tramo"]
NIVEL_MODELO = {"consistencia": "consistencia", "thingspeak_real": "thingspeak_real", "esp32_real": "thingspeak_real"}
FEATURES = ["y0", "l1", "l2", "l3", "l6", "l12", "d1", "d3", "d12", "ma3", "ma6", "ma12", "sd12",
            "hora_sin", "hora_cos", "dia_semana", "h_desde_llenado", "sin_llenado", "tend3h"]
MODELOS = ["pred_holt", "pred_holt_pronostico", "pred_gbt"]  # se comparan siempre contra las dos líneas base
ESP32_PENDIENTE = ("sin periodo de prueba: ingesta/tanques.yml tiene la geometría del ESP32 provisional "
                   "(por_medir: true, geometria_desde: null) y p6.1 solo abre la prueba desde max(congelamiento "
                   "f4.3, geometria_desde)")


def log(**c):
    print(json.dumps({"service": "f6-d2", **c}, ensure_ascii=False, default=str), flush=True)


# --- 1. Rejilla, llenados, verdad de autonomía y variables -----------------------------------------------------

def llenados(ym, subida, quieto):
    """[(inicio, fin)] de los llenados sobre la serie suavizada (índices de paso)."""
    out, mn, ini, calmos, prev = [], None, None, 0, None
    for i, v in enumerate(ym):
        if np.isnan(v):
            continue
        if mn is None:
            mn = prev = v
            continue
        if ini is None:
            if v - mn > subida:
                ini, calmos = i, 0
            else:
                mn = min(mn, v)
        else:
            calmos = calmos + 1 if v <= prev + quieto else 0
            if calmos >= 2:
                out.append((ini, i))
                ini, mn = None, v
        prev = v
    if ini is not None:
        out.append((ini, len(ym) - 1))
    return out


def preparar(d2, umbrales=None):
    """Una fila por paso observado, con la rejilla de su serie: objetivos, variables GBT, episodio y verdad."""
    ll = CFG["llenado"]
    umbrales = umbrales or [CFG["umbral_pct"], *CFG["umbrales_sensibilidad"]]
    filas = []
    for clave, g in d2.groupby(SERIE, sort=False):
        g = g.sort_values("ts").reset_index(drop=True)
        k = ((g["ts"] - g["ts"].iloc[0]) // PASO_MS).astype(int).values
        n = k.max() + 1
        Y = np.full(n, np.nan)
        Y[k] = g["nivel_pct"].values
        s = pd.Series(Y)
        ym = s.rolling(ll["suavizado_pasos"], min_periods=1).median().values
        ev_ll = llenados(ym, ll["subida_pp"], ll["quieto_pp"])
        en_llenado = np.zeros(n, bool)
        prox_inicio = np.full(n, n)        # próximo inicio de llenado desde cada paso
        ult_fin = np.full(n, -1)           # último fin de llenado antes de cada paso
        episodio = np.zeros(n, int)
        for j, (a, b) in enumerate(ev_ll):
            en_llenado[a:b + 1] = True
        for a, b in reversed(ev_ll):
            prox_inicio[:a + 1] = np.minimum(prox_inicio[:a + 1], a)
        for j, (a, b) in enumerate(ev_ll):
            ult_fin[b + 1:] = b
            episodio[b + 1:] = j + 1
        r = g[[*SERIE, "ts", "nivel_pct", "particion", "hora_lima", "dia_semana", "tendencia_pct_h_3h"]].copy()
        r["k"] = k
        r["ym"] = ym[k]
        for h in (3, 12):
            fut = np.full(n, np.nan)
            fut[:-h] = Y[h:]
            r[f"y_fut{h}"] = fut[k]
        # Variables del GBT (solo pasado: rezagos y ventanas que terminan en t). Los rezagos usan el último valor
        # disponible hasta 10 min antes (ffill con límite 2 pasos): ThingSpeak 1233810 publica cada ~10 min y con
        # rezagos exactos en la rejilla de 5 min nunca tendría variables completas.
        Yf = s.ffill(limit=2).values
        lag = lambda m: np.concatenate([np.full(m, np.nan), Yf[:-m]])  # noqa: E731
        r["y0"] = Y[k]
        for m in (1, 2, 3, 6, 12):
            r[f"l{m}"] = lag(m)[k]
        r["d1"], r["d3"], r["d12"] = r["y0"] - r["l1"], r["y0"] - r["l3"], r["y0"] - r["l12"]
        r["ma3"] = s.rolling(3, min_periods=1).mean().values[k]
        r["ma6"] = s.rolling(6, min_periods=2).mean().values[k]
        r["ma12"] = s.rolling(12, min_periods=3).mean().values[k]
        r["sd12"] = s.rolling(12, min_periods=3).std().values[k]
        r["hora_sin"] = np.sin(2 * np.pi * r["hora_lima"] / 24)
        r["hora_cos"] = np.cos(2 * np.pi * r["hora_lima"] / 24)
        desde = np.where(ult_fin >= 0, np.arange(n) - ult_fin, np.arange(n))
        r["h_desde_llenado"] = np.minimum(desde[k] / PASOS_POR_HORA, CFG["tope_horas"])
        r["sin_llenado"] = (ult_fin[k] < 0).astype(float)  # 1: no hubo llenado en el tramo (se mide desde su inicio)
        r["tend3h"] = r["tendencia_pct_h_3h"]
        # Pendientes de las líneas base de autonomía (mismas definiciones que las de nivel)
        r["pend_persistencia_h"] = r["d12"]  # cambio en la última hora (%/h)
        r["pend_tendencia_h"] = pendiente_ols(Y, CFG["tendencia_k"])[k] * PASOS_POR_HORA
        # Episodio de descenso y verdad de autonomía (censurada si llega un llenado o termina el tramo antes)
        r["en_llenado"] = en_llenado[k]
        r["episodio"] = [f"{'|'.join(map(str, clave))}#{e}" for e in episodio[k]]
        for u in umbrales:
            cruce = np.full(n, -1)
            sig = -1
            for i in range(n - 1, -1, -1):
                if not np.isnan(ym[i]) and ym[i] <= u:
                    sig = i
                cruce[i] = sig
            verdad = np.where((cruce >= 0) & (cruce < prox_inicio), (cruce - np.arange(n)) / PASOS_POR_HORA, np.nan)
            r[f"verdad_h_u{u}"] = verdad[k]
            r[f"arriba_u{u}"] = ym[k] > u
            # Para el C-index (d2.2): tiempo observado = hasta el umbral si hubo evento; si no, hasta la censura
            # (inicio del próximo llenado o fin del tramo), que es una cota inferior de la autonomía real
            fin = np.minimum(prox_inicio, n - 1)
            r[f"evento_u{u}"] = (~np.isnan(verdad))[k]
            r[f"t_obs_u{u}"] = np.where(~np.isnan(verdad), verdad, (fin - np.arange(n)) / PASOS_POR_HORA)[k]
        filas.append(r)
    out = pd.concat(filas, ignore_index=True)
    out["nivel"] = out["fuente"].map(ev.nivel_de)
    # Conjunto de ajuste: último 20 % del entrenamiento de cada sensor y corrida (el resto es "entrenamiento")
    claves = ["fuente", "sensorId", "runId"]
    ent = out["particion"] == "entrenamiento"
    corte = (out[ent].groupby(claves)["ts"].quantile(1 - CFG["frac_ajuste"]).rename("corte_ajuste").reset_index())
    out = out.merge(corte, on=claves, how="left")
    out["conj"] = np.where((out["particion"] == "entrenamiento") & (out["ts"] >= out["corte_ajuste"]), "ajuste",
                           out["particion"])
    return out.drop(columns="corte_ajuste")


def pendiente_ols(Y, k):
    """Pendiente (%/paso) de la recta de mínimos cuadrados sobre las últimas k observaciones (mín. k/2)."""
    n = len(Y)
    out = np.full(n, np.nan)
    x = np.arange(k, dtype=float)
    for j in range(k - 1, n):
        w = Y[j - k + 1:j + 1]
        ok = ~np.isnan(w)
        if ok.sum() >= max(3, k // 2):
            out[j] = np.polyfit(x[ok], w[ok], 1)[0]
    return out


# --- 2. Holt ----------------------------------------------------------------------------------------------------

def correr_holt(df, alpha, beta):
    """Nivel y tendencia de Holt en cada paso observado (estado después de ver la lectura de ese paso)."""
    nivel = np.full(len(df), np.nan)
    tend = np.full(len(df), np.nan)
    for _, idx in df.groupby(SERIE, sort=False).indices.items():
        h = Holt(alpha, beta)
        sub = df.iloc[idx]
        for pos, ts, y in zip(idx, sub["ts"].values, sub["nivel_pct"].values):
            h.actualizar(int(ts), float(y))
            nivel[pos], tend[pos] = h.nivel, h.tendencia
    return nivel, tend


def horas_vector(nivel, pend_h, u, tope):
    """horas_hasta() vectorizada y con tope (misma regla que batch/holt.py)."""
    h = np.where(nivel <= u, 0.0, np.where(pend_h > -TENDENCIA_MIN_PCT_H, np.inf,
                                           (nivel - u) / np.where(pend_h < 0, -pend_h, 1.0)))
    return np.minimum(h, tope)


def ajustar_holt(df):
    """Dos ajustes por nivel, ambos solo con la partición "entrenamiento" de p6.1 (nunca prueba ni consistencia):
      autonomia   (PRINCIPAL) MAE de las horas hasta el umbral en los casos no censurados del entrenamiento
      pronostico  MAE medio del nivel a 15 y 60 min en el conjunto de AJUSTE (último 20 % del entrenamiento)
    Se separan porque chocan: para pronosticar 15-60 min la tendencia estorba (en ThingSpeak el óptimo es
    alpha 1, beta 0, que es la persistencia) y sin tendencia no se puede proyectar ninguna autonomía."""
    u, tope = CFG["umbral_pct"], CFG["tope_horas"]
    elegidos, elegidos_pron, tabla = {}, {}, []
    for nivel in ("consistencia", "thingspeak_real"):
        d = df[df["nivel"] == nivel].sort_values(SERIE + ["ts"]).reset_index(drop=True)
        aj = (d["conj"] == "ajuste").values
        verdad = d[f"verdad_h_u{u}"].values
        casos = (d["particion"].values == "entrenamiento") & d[f"arriba_u{u}"].values & ~d["en_llenado"].values \
            & ~np.isnan(verdad) & (verdad <= tope)
        mejor, mejor_pron = None, None
        for a in CFG["holt"]["alpha"]:
            for b in CFG["holt"]["beta"]:
                L, T = correr_holt(d, a, b)
                maes = []
                for h in (3, 12):
                    ok = aj & ~np.isnan(d[f"y_fut{h}"].values)
                    maes.append(np.mean(np.abs(d[f"y_fut{h}"].values[ok] - (L[ok] + T[ok] * h))))
                obj_pron = float(np.mean(maes))
                obj_aut = float(np.mean(np.abs(horas_vector(L[casos], T[casos] * PASOS_POR_HORA, u, tope)
                                               - verdad[casos]))) if casos.any() else float("nan")
                tabla.append({"nivel": nivel, "alpha": a, "beta": b, "mae_15": maes[0], "mae_60": maes[1],
                              "objetivo_pronostico": obj_pron, "mae_autonomia_h": obj_aut})
                if mejor_pron is None or obj_pron < mejor_pron[0]:
                    mejor_pron = (obj_pron, a, b)
                if not math.isnan(obj_aut) and (mejor is None or obj_aut < mejor[0]):
                    mejor = (obj_aut, a, b)
        elegidos_pron[nivel] = {"alpha": mejor_pron[1], "beta": mejor_pron[2], "mae_ajuste_medio": mejor_pron[0]}
        if mejor:
            elegidos[nivel] = {"alpha": mejor[1], "beta": mejor[2], "mae_autonomia_entrenamiento_h": mejor[0],
                               "casos_no_censurados_entrenamiento": int(casos.sum())}
        else:  # sin casos no censurados en el entrenamiento: se usa el ajuste de pronóstico y se informa
            elegidos[nivel] = {**elegidos_pron[nivel], "nota": "sin casos no censurados en el entrenamiento"}
        log(status="holt_ajustado", nivel=nivel, autonomia=elegidos[nivel], pronostico=elegidos_pron[nivel])
    origen = "parámetros de ThingSpeak real (el ESP32 no tiene datos de entrenamiento suficientes ni geometría medida)"
    elegidos["esp32_real"] = {**elegidos["thingspeak_real"], "origen": origen}
    elegidos_pron["esp32_real"] = {**elegidos_pron["thingspeak_real"], "origen": origen}
    return elegidos, elegidos_pron, tabla


# --- 3. GBT (Spark MLlib) -----------------------------------------------------------------------------------------

def entrenar_gbt(df, carpeta):
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressor
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (SparkSession.builder.appName("monitoraqua-f6-d2").master("local[4]")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    completas = df[FEATURES].notna().all(axis=1)
    base = df.loc[completas, ["fila", "nivel", "conj", *FEATURES, "y_fut3", "y_fut12"]].copy()
    base["obj3"], base["obj12"] = base["y_fut3"] - base["y0"], base["y_fut12"] - base["y0"]
    sdf = VectorAssembler(inputCols=FEATURES, outputCol="f").transform(
        spark.createDataFrame(base.drop(columns=["y_fut3", "y_fut12"]))).cache()
    preds, resumen = {}, {}
    for nivel in ("consistencia", "thingspeak_real"):
        dn = sdf.filter(sdf.nivel == nivel)
        for h, minutos in ((3, 15), (12, 60)):
            obj = f"obj{h}"
            con_obj = dn.filter(F.col(obj).isNotNull() & ~F.isnan(obj))
            ent, aj = con_obj.filter(con_obj.conj == "entrenamiento"), con_obj.filter(con_obj.conj == "ajuste")
            pruebas = []
            for prof in CFG["gbt"]["maxDepth"]:
                for it in CFG["gbt"]["maxIter"]:
                    m = GBTRegressor(featuresCol="f", labelCol=obj, maxDepth=prof, maxIter=it,
                                     stepSize=CFG["gbt"]["stepSize"], seed=CFG["gbt"]["semilla"]).fit(ent)
                    p = m.transform(aj).select(obj, "prediction").toPandas()
                    pruebas.append({"maxDepth": prof, "maxIter": it, "mae_ajuste": float((p[obj] - p["prediction"]).abs().mean())})
            mejor = min(pruebas, key=lambda x: x["mae_ajuste"])
            final = GBTRegressor(featuresCol="f", labelCol=obj, maxDepth=mejor["maxDepth"], maxIter=mejor["maxIter"],
                                 stepSize=CFG["gbt"]["stepSize"], seed=CFG["gbt"]["semilla"]).fit(
                con_obj.filter(con_obj.conj.isin("entrenamiento", "ajuste")))
            nombre = f"gbt_h{minutos}_{nivel.replace('_real', '')}"
            final.write().overwrite().save(str(carpeta / nombre))
            importancias = dict(sorted(zip(FEATURES, final.featureImportances.toArray().round(4).tolist()),
                                       key=lambda kv: -kv[1])[:8])
            resumen[nombre] = {"grilla": pruebas, "elegido": mejor, "importancias_top8": importancias,
                               "filas_entrenamiento_final": final_count(con_obj)}
            log(status="gbt_entrenado", modelo=nombre, **mejor)
            # Predice el ESP32 con el modelo de ThingSpeak (tanques reales); los aumentados con el suyo
            destino = [nivel, "esp32_real"] if nivel == "thingspeak_real" else [nivel]
            p = final.transform(sdf.filter(sdf.nivel.isin(*destino))).select("fila", "y0", "prediction").toPandas()
            preds[minutos] = pd.concat([preds.get(minutos, pd.DataFrame()),
                                        pd.DataFrame({"fila": p["fila"], f"pred_gbt_{minutos}": p["y0"] + p["prediction"]})])
    spark.stop()
    return preds, resumen


def final_count(con_obj):
    return int(con_obj.filter(con_obj.conj.isin("entrenamiento", "ajuste")).count())


# --- 4. Evaluación ------------------------------------------------------------------------------------------------

def evaluar_pronostico(df, d2, minutos):
    """Por nivel, sobre el conjunto final (prueba / consistencia), contra persistencia y tendencia de evaluacion.py."""
    h = minutos // 5
    base = ev.lineas_base(d2, "nivel_pct", PASO_MS, h, CFG["tendencia_k"], grupo=SERIE)
    t = base[[*SERIE, "ts", "y_real", "pred_persistencia", "pred_tendencia"]].merge(
        df[[*SERIE, "ts", "nivel", "particion", f"pred_holt_{minutos}", f"pred_holtp_{minutos}",
            f"pred_gbt_{minutos}"]], on=[*SERIE, "ts"])
    t = t.rename(columns={f"pred_holt_{minutos}": "pred_holt", f"pred_holtp_{minutos}": "pred_holt_pronostico",
                          f"pred_gbt_{minutos}": "pred_gbt"})
    t = t.dropna(subset=MODELOS)  # mismas filas para todos los predictores
    final = {"consistencia": t[(t["nivel"] == "consistencia") & (t["particion"] == "consistencia")],
             "thingspeak_real": t[(t["nivel"] == "thingspeak_real") & (t["particion"] == "prueba")],
             "esp32_real": t[(t["nivel"] == "esp32_real") & (t["particion"] == "prueba")]}
    bs = CFG["bootstrap"]
    res = ev.reporte(final, MODELOS, largo_bloque=bs["largo_bloque"], n_boot=bs["n_boot"])
    if final["esp32_real"].empty:
        res["esp32_real"] = {"n": 0, "estado": "pendiente", "razon": ESP32_PENDIENTE}
    # Informativo: el tramo nocturno del ESP32 (diseño, no es métrica final), con parámetros de ThingSpeak
    noche = t[(t["nivel"] == "esp32_real") & (t["tramo"] == tramo_mas_largo(d2))]
    info = ev.reporte({"esp32_real": noche}, MODELOS, largo_bloque=bs["largo_bloque"],
                      n_boot=bs["n_boot"])["esp32_real"]
    info["uso"] = "diseño (informativo): partición entrenamiento de p6.1; NO es la métrica final del ESP32"
    return res, info, t


def tramo_mas_largo(d2):
    e = d2[d2["fuente"] == "esp32"]
    return int(e.groupby("tramo")["ts"].agg(lambda s: s.max() - s.min()).idxmax())


def evaluar_autonomia(df, u):
    """Error en horas solo donde el tanque realmente bajó hasta el umbral (verdad no censurada y <= tope).
    El IC remuestrea episodios completos."""
    tope = CFG["tope_horas"]
    bs = CFG["bootstrap"]
    # predictor -> (nivel de partida, pendiente %/h). Holt parte de su nivel suavizado; los demás, de la lectura.
    preds = {"pred_persistencia": ("nivel_pct", "pend_persistencia_h"), "pred_tendencia": ("nivel_pct", "pend_tendencia_h"),
             "pred_holt": ("holt_nivel", "holt_pend_h"), "pred_holt_pronostico": ("holtp_nivel", "holtp_pend_h"),
             "pred_gbt": ("nivel_pct", "gbt_pend_h")}
    columnas = sorted({c for par in preds.values() for c in par})
    res = {}
    for nivel, part in (("consistencia", "consistencia"), ("thingspeak_real", "prueba"), ("esp32_real", "prueba")):
        d = df[(df["nivel"] == nivel) & (df["particion"] == part) & df[f"arriba_u{u}"] & ~df["en_llenado"]]
        if nivel == "esp32_real" and d.empty:
            res[nivel] = {"estado": "pendiente", "razon": ESP32_PENDIENTE}
            continue
        eps = d.groupby("episodio")[f"verdad_h_u{u}"].apply(lambda s: s.notna().any())
        # mismas filas para los cuatro predictores: las que tienen todas las pendientes
        v = d[d[f"verdad_h_u{u}"].notna() & (d[f"verdad_h_u{u}"] <= tope)].dropna(subset=columnas)
        bloque = {"episodios": int(len(eps)), "llegan_al_umbral": int(eps.sum()), "censurados": int((~eps).sum()),
                  "origenes_evaluados": int(len(v)), "verdad_mayor_que_tope": int(
                      (d[f"verdad_h_u{u}"] > tope).sum())}
        if len(v):
            bloque["predictores"] = {}
            for nombre, (col_nivel, col_pend) in preds.items():
                p = np.array([min(horas_hasta(y, s, u), tope) for y, s in zip(v[col_nivel], v[col_pend])])
                r = ev.evaluar_regresion(v[f"verdad_h_u{u}"].values, p, n_boot=bs["n_boot"], mape_minimo=1.0,
                                         grupos=v["episodio"].values)
                r["sin_descenso_previsto"] = float(np.mean(p >= tope))
                bloque["predictores"][nombre] = r
            mae = {k: x["MAE"]["valor"] for k, x in bloque["predictores"].items()}
            for m in MODELOS:
                for b in ("pred_persistencia", "pred_tendencia"):
                    bloque["predictores"][m][f"mejora_vs_{b.removeprefix('pred_')}"] = 1 - mae[m] / mae[b]
        res[nivel] = bloque
    return res


# --- 5. Tablas para la evidencia -----------------------------------------------------------------------------------

def fmt(m):
    if not m or m.get("valor") is None:
        return "—"
    ic = m["ic95"]
    return f"{m['valor']:.2f} [{ic[0]:.2f}, {ic[1]:.2f}]" if ic[0] is not None else f"{m['valor']:.2f} (sin IC)"


def tablas_md(res):
    L = [f"# Resultados D2 ({res['meta']['version']}, {res['meta']['fecha']})", ""]
    for minutos, r in res["pronostico"].items():
        L += [f"## Pronóstico de nivel a {minutos} min (conjunto final)", "",
              "| Nivel | Predictor | n | MAE % [IC 95 %] | RMSE % [IC 95 %] | MAPE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |",
              "|---|---|---|---|---|---|---|---|"]
        for nivel, b in r["final"].items():
            if not b.get("predictores"):
                L.append(f"| {nivel} | — | 0 | {b.get('estado', '')}: {b.get('razon', b.get('nota', ''))} | | | | |")
                continue
            for p, m in b["predictores"].items():
                mp = f"{m['mejora_vs_persistencia']:+.1%}" if "mejora_vs_persistencia" in m else ""
                mt = f"{m['mejora_vs_tendencia']:+.1%}" if "mejora_vs_tendencia" in m else ""
                L.append(f"| {nivel} | {p.removeprefix('pred_')} | {m['n']} | {fmt(m['MAE'])} | {fmt(m['RMSE'])} | "
                         f"{fmt(m['MAPE'])} | {mp} | {mt} |")
        i = r["esp32_noche_informativo"]
        if i.get("predictores"):
            L += ["", f"ESP32, tramo nocturno 29-oct (diseño, informativo, no es métrica final): n = {i['n']}", ""]
            for p, m in i["predictores"].items():
                L.append(f"- {p.removeprefix('pred_')}: MAE {fmt(m['MAE'])}")
        L.append("")
    for u, r in res["autonomia"].items():
        L += [f"## Horas de autonomía hasta {u} % (conjunto final, solo casos no censurados)", "",
              "| Nivel | Episodios | Llegan al umbral | Censurados | Orígenes evaluados | Predictor | MAE h [IC 95 %] | Sin descenso previsto |",
              "|---|---|---|---|---|---|---|---|"]
        for nivel, b in r.items():
            if b.get("estado") == "pendiente":
                L.append(f"| {nivel} | — | — | — | — | pendiente | {b['razon']} | |")
                continue
            if not b.get("predictores"):
                L.append(f"| {nivel} | {b['episodios']} | {b['llegan_al_umbral']} | {b['censurados']} | 0 | — | "
                         "sin casos no censurados | |")
                continue
            for p, m in b["predictores"].items():
                L.append(f"| {nivel} | {b['episodios']} | {b['llegan_al_umbral']} | {b['censurados']} | "
                         f"{b['origenes_evaluados']} | {p.removeprefix('pred_')} | {fmt(m['MAE'])} | {m['sin_descenso_previsto']:.0%} |")
        L.append("")
    return "\n".join(L)


# --- main --------------------------------------------------------------------------------------------------------

def main():
    t0 = time.time()
    fecha = datetime.now(timezone.utc).strftime("%Y%m%d")
    version = CFG["version"]
    carpeta = RAIZ / "batch" / "modelos" / "d2" / f"{version}-{fecha}"
    carpeta.mkdir(parents=True, exist_ok=True)

    d2 = ev.leer_oro("d2_autonomia").dropna(subset=["nivel_pct"])
    d2["runId"] = d2["runId"].fillna("")
    df = preparar(d2)
    df = df.sort_values(SERIE + ["ts"]).reset_index(drop=True)
    df["fila"] = np.arange(len(df))
    log(status="preparado", filas=len(df), por_nivel=df.groupby("nivel").size().to_dict(),
        por_conj={f"{a}/{b}": int(n) for (a, b), n in df.groupby(["nivel", "conj"]).size().items()})

    # Holt: ajuste por nivel y corrida final con los parámetros elegidos (principal = ajuste por autonomía;
    # holt_pronostico = ajuste por el nivel a 15-60 min, solo como comparación)
    params, params_pron, grilla_holt = ajustar_holt(df)
    for prefijo, p in (("holt", params), ("holtp", params_pron)):
        for nivel in ("consistencia", "thingspeak_real", "esp32_real"):
            idx = df.index[df["nivel"] == nivel]
            L, T = correr_holt(df.loc[idx].reset_index(drop=True), p[nivel]["alpha"], p[nivel]["beta"])
            df.loc[idx, f"{prefijo}_nivel"], df.loc[idx, f"{prefijo}_tend"] = L, T
        for minutos in CFG["horizontes_min"]:
            df[f"pred_{prefijo}_{minutos}"] = df[f"{prefijo}_nivel"] + df[f"{prefijo}_tend"] * (minutos // 5)
        df[f"{prefijo}_pend_h"] = df[f"{prefijo}_tend"] * PASOS_POR_HORA

    # GBT
    preds, resumen_gbt = entrenar_gbt(df, carpeta)
    for minutos, p in preds.items():
        df = df.merge(p, on="fila", how="left")
    df["gbt_pend_h"] = df["pred_gbt_60"] - df["nivel_pct"]  # cambio previsto en 1 h = pendiente %/h

    # Evaluación
    res = {"meta": {"version": version, "fecha": fecha, "particion": str(d2["particion_version"].iloc[0]),
                    "fuentes": json.loads((RAIZ / "batch" / "datos" / "fuentes" / "f6_manifiesto.json").read_text(
                        encoding="utf-8"))["descargado_utc"],
                    "config": CFG, "filas": int(len(df))},
           "holt": {"elegidos": params, "elegidos_pronostico": params_pron, "grilla": grilla_holt},
           "gbt": resumen_gbt, "pronostico": {}, "autonomia": {}}
    for minutos in CFG["horizontes_min"]:
        final, info, _ = evaluar_pronostico(df, d2, minutos)
        res["pronostico"][str(minutos)] = {"final": final, "esp32_noche_informativo": info}
        log(status="evaluado_pronostico", minutos=minutos)
    for u in [CFG["umbral_pct"], *CFG["umbrales_sensibilidad"]]:
        res["autonomia"][str(u)] = evaluar_autonomia(df, u)
        log(status="evaluada_autonomia", umbral=u)

    # Modelos para F7 y evidencia
    (carpeta / "holt.json").write_text(json.dumps({
        "version": version, "fecha": fecha, "modelo": "Holt (nivel + tendencia), batch/holt.py",
        "entrada": "mediana del nivel % en bloques de 5 min (como d2_autonomia)", "paso_ms": PASO_MS,
        "umbral_pct": CFG["umbral_pct"], "tope_horas": CFG["tope_horas"],
        "parametros": params, "uso_parametros": "ajustados por el error de autonomía en el entrenamiento: son los de F7",
        "parametros_pronostico": params_pron,
        "uso_parametros_pronostico": "ajustados por el nivel a 15-60 min; solo comparación (en ThingSpeak = persistencia)"},
        ensure_ascii=False, indent=1), encoding="utf-8")
    (carpeta / "manifiesto.json").write_text(json.dumps({
        "version": version, "fecha": fecha, "particion": res["meta"]["particion"], "fuentes_descargadas": res["meta"]["fuentes"],
        "features_gbt": FEATURES, "gbt": {k: v["elegido"] for k, v in resumen_gbt.items()},
        "gbt_objetivo": "cambio de nivel (pp) a 15 o 60 min; la predicción de nivel es y0 + cambio",
        "archivos": sorted(p.name for p in carpeta.iterdir()), "config": CFG}, ensure_ascii=False, indent=1),
        encoding="utf-8")
    evid = RAIZ / "docs" / "evidencias" / "f6-d2"
    evid.mkdir(parents=True, exist_ok=True)
    (evid / "resultados_d2.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (evid / "tablas_d2.md").write_text(tablas_md(res), encoding="utf-8")
    shutil.copy(carpeta / "holt.json", evid / "holt.json")
    shutil.copy(carpeta / "manifiesto.json", evid / "manifiesto_modelos.json")
    log(status="listo", segundos=round(time.time() - t0), modelos=str(carpeta))


if __name__ == "__main__":
    main()
