"""
F6-D2 versión d2.2: perfil de consumo por hora del día como modelo principal de las horas de autonomía.

  Principal  perfil de consumo de cada tanque (batch/perfil_consumo.py), aprendido solo de su partición de
             entrenamiento (p6.1): la autonomía proyecta el nivel restando el consumo esperado de cada hora hasta el
             umbral, con banda P10-P90 (P90 de consumo = pesimista)
  Bases      autonomía: consumo medio (nivel disponible / consumo medio por hora) y Holt d2.1
             nivel a 15 y 60 min: persistencia y tendencia (evaluacion.lineas_base), como en d2.1
  d2.1       Holt (parámetros de holt.json) y los GBT guardados se re-aplican SIN reentrenar, para comparar d2.1 y
             d2.2 sobre las mismas filas
  Métricas   MAE en horas sobre los casos no censurados + C-index global y estratificado por tanque (usan también
             los censurados) + cobertura de la banda antes y después de calibrarla; pronóstico de nivel con
             MAE/RMSE/MAPE. Todo con evaluacion.py, por nivel.
  Banda      calibrada por predicción conforme (split, escala logarítmica) con el conjunto de AJUSTE de p6.1: los
             puntajes salen de un perfil aprendido solo con el 80 % inicial del entrenamiento; nunca con la prueba.

  docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/batch monitoraqua-streaming \
      spark-submit --driver-memory 3g batch/modelo_d2_perfil.py
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
import modelo_d2 as m  # noqa: E402
from holt import PASOS_POR_HORA  # noqa: E402
from perfil_consumo import LIMA_MS, PerfilConsumo  # noqa: E402

CFG = m.CFG
CFG2 = yaml.safe_load((RAIZ / "batch" / "modelo_d2_perfil.yml").read_text(encoding="utf-8"))
H = 3_600_000
TANQUE = ["fuente", "sensorId", "runId"]
FINAL = (("consistencia", "consistencia"), ("thingspeak_real", "prueba"), ("esp32_real", "prueba"))
# Autonomía: predictor -> columna de horas (todas con tope). d2.2 primero; d2.1 para el lado a lado.
AUTONOMIA = {"perfil_p50": "d22", "consumo_medio": "base d2.2", "holt_d21": "base d2.2 (d2.1 principal)",
             "persistencia_vel_d21": "d2.1", "tendencia_d21": "d2.1", "gbt_d21": "d2.1"}
NIVEL = ["pred_perfil", "pred_holt_d21", "pred_gbt_d21"]
EVAL = CFG2["evaluacion"]
UMBRALES = EVAL["umbrales"]


def log(**c):
    print(json.dumps({"service": "f6-d2.2", **c}, ensure_ascii=False, default=str), flush=True)


# --- Perfil -----------------------------------------------------------------------------------------------------

def tasas_horarias(df, solo_conj=None):
    """Consumo (%/h) de cada bloque de una hora de reloj (Lima) sin llenado, solo en la partición entrenamiento
    (o solo en el conjunto solo_conj, p. ej. "entrenamiento" = el 80 % inicial, para la calibración conforme)."""
    p = CFG2["perfil"]
    filtro = (df["conj"] == solo_conj) if solo_conj else (df["particion"] == "entrenamiento")
    d = df[filtro].sort_values(m.SERIE + ["ts"]).copy()
    d["bloque"] = (d["ts"] + LIMA_MS) // H
    b = d.groupby(m.SERIE + ["bloque"]).agg(y0=("ym", "first"), y1=("ym", "last"), t0=("ts", "first"),
                                            t1=("ts", "last"), n=("ts", "size"), llenado=("en_llenado", "max"))
    b = b.reset_index()
    b = b[(b["n"] >= p["min_pasos_bloque"]) & (b["t1"] - b["t0"] >= p["min_minutos_bloque"] * 60_000) & ~b["llenado"]]
    b["consumo"] = ((b["y0"] - b["y1"]) / ((b["t1"] - b["t0"]) / H)).clip(lower=0)
    b["hora"] = b["bloque"] % 24
    b["dia_semana"] = ((b["bloque"] // 24) + 3) % 7
    return b


def aprender_perfiles(df, solo_conj=None):
    b = tasas_horarias(df, solo_conj)
    ent = df[(df["conj"] == solo_conj) if solo_conj else (df["particion"] == "entrenamiento")]
    dias = ((ent.groupby(TANQUE)["ts"].max() - ent.groupby(TANQUE)["ts"].min()) / 86_400_000)
    perfiles = {}
    for clave, g in b.groupby(TANQUE):
        perfiles[clave] = PerfilConsumo.aprender(
            g["dia_semana"], g["hora"], g["consumo"], dias.get(clave, 0), CFG2["perfil"]["min_muestras"],
            CFG2["perfil"]["min_dias_semana"], CFG["tope_horas"])
    log(status="perfiles", tanques=len(perfiles), bloques=len(b), solo_conj=solo_conj)
    return perfiles


def calibrar_banda(df):
    """Predicción conforme (split) de la banda P10-P90, por nivel. Puntaje de cada caso no censurado del conjunto de
    AJUSTE: cuánto se salió la verdad de la banda en escala logarítmica, max(log lo - log y, log y - log hi). Con los
    umbrales del 10 y del 25 % juntos (el puntaje es una razón: no depende del umbral). q = cuantil (1 - alfa) con la
    corrección de muestra finita; la banda calibrada es [lo / e^q, hi * e^q]. Los casos de un mismo episodio no son
    independientes: se informa cuántos episodios la sostienen."""
    c = CFG2["conforme"]
    tope, eps = CFG["tope_horas"], c["horas_minimas"]
    perfiles_cal = aprender_perfiles(df, "entrenamiento")
    out = {}
    for nivel in ("consistencia", "thingspeak_real"):
        puntajes, episodios = [], set()
        for u in [CFG["umbral_pct"], *CFG["umbrales_sensibilidad"]]:
            a = df[(df["nivel"] == nivel) & (df["conj"] == "ajuste") & df[f"arriba_u{u}"] & ~df["en_llenado"]
                   & df[f"evento_u{u}"] & (df[f"verdad_h_u{u}"] <= tope)]
            for clave, g in a.groupby(TANQUE):
                pc = perfiles_cal.get(clave)
                if pc is None:
                    continue
                lo = np.maximum(pc.horas_autonomia(g["ym"].values, g["ts"].values, u, "p90"), eps)
                hi = np.maximum(pc.horas_autonomia(g["ym"].values, g["ts"].values, u, "p10"), eps)
                y = np.maximum(g[f"verdad_h_u{u}"].values, eps)
                puntajes += list(np.maximum(np.log(lo) - np.log(y), np.log(y) - np.log(hi)))
                episodios |= set(f"u{u}|{x}" for x in g["episodio"])
        n = len(puntajes)
        if n == 0:
            out[nivel] = {"estado": "sin casos no censurados en el ajuste: no se calibra"}
            continue
        nivel_q = min(1.0, math.ceil((n + 1) * (1 - c["alfa"])) / n)
        q = float(np.quantile(puntajes, nivel_q, method="higher"))
        out[nivel] = {"q_log": q, "factor": math.exp(q), "casos": n, "episodios": len(episodios),
                      "alfa": c["alfa"], "cobertura_nominal": 1 - c["alfa"]}
    if "q_log" in out.get("thingspeak_real", {}):
        out["esp32_real"] = {**out["thingspeak_real"], "origen": "la de ThingSpeak real (el ESP32 no tiene ajuste)"}
    log(status="banda_calibrada", **{k: v.get("factor", v.get("estado")) for k, v in out.items()})
    return out


def banda_calibrada(lo, hi, cal):
    if not cal or "q_log" not in cal:
        return lo, hi
    f = math.exp(cal["q_log"])
    return lo / f, np.minimum(hi * f, CFG["tope_horas"])


# --- Predicciones -------------------------------------------------------------------------------------------------

def predecir(df, perfiles, holt_params, carpeta_d21):
    u, tope = CFG["umbral_pct"], CFG["tope_horas"]
    # Perfil y consumo medio (desde el nivel suavizado, el mismo con el que se mide el cruce del umbral)
    for col in ("aut_perfil_p50", "aut_perfil_p10", "aut_perfil_p90", "aut_consumo_medio", "pred_perfil_15",
                "pred_perfil_60"):
        df[col] = np.nan
    for clave, idx in df.groupby(TANQUE).groups.items():
        p = perfiles.get(clave)
        if p is None:
            continue
        sub = df.loc[idx]
        for q in ("p50", "p10", "p90"):
            df.loc[idx, f"aut_perfil_{q}"] = p.horas_autonomia(sub["ym"].values, sub["ts"].values, u, q)
        media = p.consumo_medio_h()
        falta = np.maximum(sub["ym"].values - u, 0)
        df.loc[idx, "aut_consumo_medio"] = np.minimum(falta / media, tope) if media > 0 else tope
        for minutos in CFG["horizontes_min"]:
            df.loc[idx, f"pred_perfil_{minutos}"] = p.pronostico(sub["nivel_pct"].values, sub["ts"].values, minutos)
    # Holt d2.1 (parámetros guardados, sin reajustar)
    for nivel in ("consistencia", "thingspeak_real", "esp32_real"):
        idx = df.index[df["nivel"] == nivel]
        L, T = m.correr_holt(df.loc[idx].reset_index(drop=True), holt_params[nivel]["alpha"], holt_params[nivel]["beta"])
        df.loc[idx, "holt_nivel"], df.loc[idx, "holt_tend"] = L, T
    for minutos in CFG["horizontes_min"]:
        df[f"pred_holt_d21_{minutos}"] = df["holt_nivel"] + df["holt_tend"] * (minutos // 5)
    # GBT d2.1 (modelos guardados, sin reentrenar)
    gbt = predecir_gbt(df, carpeta_d21)
    for minutos, serie in gbt.items():
        df[f"pred_gbt_d21_{minutos}"] = serie
    # Horas de autonomía de los predictores de pendiente (d2.1), con la misma regla de holt.py
    hv = m.horas_vector
    df["aut_holt_d21"] = hv(df["holt_nivel"].values, df["holt_tend"].values * PASOS_POR_HORA, u, tope)
    df["aut_persistencia_vel_d21"] = hv(df["nivel_pct"].values, df["pend_persistencia_h"].values, u, tope)
    df["aut_tendencia_d21"] = hv(df["nivel_pct"].values, df["pend_tendencia_h"].values, u, tope)
    df["aut_gbt_d21"] = hv(df["nivel_pct"].values, (df["pred_gbt_d21_60"] - df["nivel_pct"]).values, u, tope)
    for c in [c for c in df.columns if c.startswith("aut_")]:  # NaN de entrada se conserva como NaN
        base = "holt_nivel" if c == "aut_holt_d21" else ("ym" if "perfil" in c or "consumo" in c else "nivel_pct")
        df.loc[df[base].isna(), c] = np.nan
    df.loc[df["pend_persistencia_h"].isna(), "aut_persistencia_vel_d21"] = np.nan
    df.loc[df["pend_tendencia_h"].isna(), "aut_tendencia_d21"] = np.nan
    df.loc[df["pred_gbt_d21_60"].isna(), "aut_gbt_d21"] = np.nan
    return df


def predecir_gbt(df, carpeta):
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import GBTRegressionModel
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("monitoraqua-f6-d2.2").master("local[4]")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    completas = df[m.FEATURES].notna().all(axis=1)
    base = df.loc[completas, ["fila", "nivel", *m.FEATURES]]
    sdf = VectorAssembler(inputCols=m.FEATURES, outputCol="f").transform(spark.createDataFrame(base)).cache()
    out = {}
    for minutos in CFG["horizontes_min"]:
        partes = []
        for nivel, modelo in (("consistencia", "consistencia"), ("thingspeak_real", "thingspeak"),
                              ("esp32_real", "thingspeak")):
            mod = GBTRegressionModel.load(str(carpeta / f"gbt_h{minutos}_{modelo}"))
            p = mod.transform(sdf.filter(sdf.nivel == nivel)).select("fila", "y0", "prediction").toPandas()
            partes.append(pd.Series((p["y0"] + p["prediction"]).values, index=p["fila"].values))
        s = pd.concat(partes)
        out[minutos] = df["fila"].map(s)
    spark.stop()
    return out


# --- Evaluación ---------------------------------------------------------------------------------------------------

def evaluar_nivel(df, d2, minutos):
    h = minutos // 5
    base = ev.lineas_base(d2, "nivel_pct", m.PASO_MS, h, CFG["tendencia_k"], grupo=m.SERIE)
    cols = {f"pred_perfil_{minutos}": "pred_perfil", f"pred_holt_d21_{minutos}": "pred_holt_d21",
            f"pred_gbt_d21_{minutos}": "pred_gbt_d21"}
    t = base[[*m.SERIE, "ts", "y_real", "pred_persistencia", "pred_tendencia"]].merge(
        df[[*m.SERIE, "ts", "nivel", "particion", *cols]], on=[*m.SERIE, "ts"]).rename(columns=cols)
    t = t.dropna(subset=NIVEL)
    final = {n: t[(t["nivel"] == n) & (t["particion"] == p)] for n, p in FINAL}
    bs = CFG["bootstrap"]
    res = ev.reporte(final, NIVEL, largo_bloque=bs["largo_bloque"], n_boot=bs["n_boot"])
    if final["esp32_real"].empty:
        res["esp32_real"] = {"n": 0, "estado": "pendiente", "razon": m.ESP32_PENDIENTE}
    return res


def evaluar_autonomia(df, u, cal):
    tope = CFG["tope_horas"]
    bs, cc = CFG["bootstrap"], CFG2["cindex"]
    columnas = [f"aut_{p}" for p in AUTONOMIA]
    res = {}
    for nivel, part in FINAL:
        d = df[(df["nivel"] == nivel) & (df["particion"] == part) & df[f"arriba_u{u}"] & ~df["en_llenado"]]
        if nivel == "esp32_real" and d.empty:
            res[nivel] = {"estado": "pendiente", "razon": m.ESP32_PENDIENTE}
            continue
        # Episodios contados como en d2.1 (antes de exigir predicciones); las métricas, sobre filas con todas
        eps = d.groupby("episodio")[f"evento_u{u}"].any()
        bloque = {"episodios": int(len(eps)), "llegan_al_umbral": int(eps.sum()), "censurados": int((~eps).sum())}
        d = d.dropna(subset=columnas)
        # 1) Error en horas, solo no censurados (verdad <= tope); IC por episodios
        v = d[d[f"evento_u{u}"] & (d[f"verdad_h_u{u}"] <= tope)]
        bloque["origenes_no_censurados"] = int(len(v))
        bloque["episodios_no_censurados_evaluados"] = k_ep = int(v["episodio"].nunique())
        bloque["episodios_censurados"] = bloque["censurados"]
        bloque["evidencia"] = "suficiente" if k_ep >= EVAL["min_episodios_evidencia"] else "indicativa"
        # 2) C-index con todos los orígenes (también censurados), uno por hora
        c = d[d["k"] % cc["submuestra_pasos"] == 0]
        bloque["origenes_cindex"] = int(len(c))
        bloque["cindex_no_censurados"] = int(c[f"evento_u{u}"].sum())
        bloque["cindex_censurados"] = int((~c[f"evento_u{u}"]).sum())
        estrato = (c["fuente"] + "|" + c["sensorId"] + "|" + c["runId"]).values
        bloque["predictores"] = {}
        for p, rol in AUTONOMIA.items():
            r = {"rol": rol}
            if len(v):
                r["error_h"] = ev.evaluar_regresion(v[f"verdad_h_u{u}"].values, v[f"aut_{p}"].values,
                                                    n_boot=bs["n_boot"], grupos=v["episodio"].values)
                if k_ep < EVAL["min_episodios_ic"]:  # pocos episodios: el bootstrap por episodio no informa
                    r["error_h"]["MAE"]["ic95_calculado"] = r["error_h"]["MAE"]["ic95"]
                    r["error_h"]["MAE"]["ic95"] = [None, None]
                    r["error_h"]["ic_nota"] = f"no informativo: {k_ep} episodios (< {EVAL['min_episodios_ic']})"
                r["sin_descenso_previsto"] = float((v[f"aut_{p}"] >= tope).mean())
            r["cindex"] = ev.evaluar_concordancia(c[f"t_obs_u{u}"].values, c[f"evento_u{u}"].values,
                                                  c[f"aut_{p}"].values, grupos=c["episodio"].values,
                                                  n_boot=cc["n_boot"], max_n=cc["max_n"])
            r["cindex_por_tanque"] = ev.evaluar_concordancia(
                c[f"t_obs_u{u}"].values, c[f"evento_u{u}"].values, c[f"aut_{p}"].values, grupos=c["episodio"].values,
                n_boot=cc["n_boot"], max_n=cc["max_n"], estratos=estrato)
            bloque["predictores"][p] = r
        if len(v):  # banda del perfil, antes y después de la calibración conforme
            y = v[f"verdad_h_u{u}"].values
            lo, hi = v["aut_perfil_p90"].values, v["aut_perfil_p10"].values
            lo_c, hi_c = banda_calibrada(lo, hi, cal.get(nivel))
            bloque["banda_p10_p90"] = {
                "antes": {"cobertura": float(np.mean((lo <= y) & (y <= hi))), "ancho_mediano_h": float(np.median(hi - lo))},
                "despues": {"cobertura": float(np.mean((lo_c <= y) & (y <= hi_c))),
                            "ancho_mediano_h": float(np.median(hi_c - lo_c))},
                "calibracion": cal.get(nivel)}
        res[nivel] = bloque
    return res


# --- Tablas -------------------------------------------------------------------------------------------------------

def f(mt, dec=2):
    if not mt or mt.get("valor") is None:
        return "—"
    ic = mt.get("ic95") or [None, None]
    return (f"{mt['valor']:.{dec}f} [{ic[0]:.{dec}f}, {ic[1]:.{dec}f}]" if ic[0] is not None
            else f"{mt['valor']:.{dec}f} (sin IC)")


def tablas_md(res, d21):
    L = [f"# Resultados D2 {res['meta']['version']} (con d2.1 lado a lado), {res['meta']['fecha']}", ""]
    L += ["## Autonomía: evidencia por umbral (conjunto final)", "",
          f"Evidencia suficiente: al menos {EVAL['min_episodios_evidencia']} episodios no censurados. IC 95 % del MAE "
          f"por bootstrap de episodios; con menos de {EVAL['min_episodios_ic']} episodios se marca como no informativo.",
          "", "| Umbral | Nivel | Episodios | No censurados: episodios (orígenes) | Censurados: episodios | Evidencia | "
          "Mejor MAE h | Banda calibrada: cobertura antes → después |", "|---|---|---|---|---|---|---|---|"]
    for u, r in res["autonomia"].items():
        for nivel, b in r.items():
            if b.get("estado") == "pendiente":
                L.append(f"| {u} % | {nivel} | — | — | — | pendiente | | |")
                continue
            maes = {p: x["error_h"]["MAE"]["valor"] for p, x in b["predictores"].items() if "error_h" in x}
            mejor = min(maes, key=maes.get) if maes else None
            banda = b.get("banda_p10_p90")
            bt = (f"{banda['antes']['cobertura']:.0%} → {banda['despues']['cobertura']:.0%}" if banda else "—")
            L.append(f"| {u} % | {nivel} | {b['episodios']} | {b['episodios_no_censurados_evaluados']} "
                     f"({b['origenes_no_censurados']}) | {b['censurados']} | **{b['evidencia']}** | "
                     f"{f'{mejor}: {maes[mejor]:.1f}' if mejor else '—'} | {bt} |")
    L.append("")
    for u, r in res["autonomia"].items():
        L += [f"## Horas de autonomía hasta {u} % (conjunto final)", "",
              "| Nivel | Predictor | Versión / rol | MAE h [IC 95 % por episodio] | C-index global [IC 95 %] | C-index por tanque [IC 95 %] | Sin descenso previsto |",
              "|---|---|---|---|---|---|---|"]
        for nivel, b in r.items():
            if b.get("estado") == "pendiente":
                L.append(f"| {nivel} | — | pendiente | {b['razon']} | | | |")
                continue
            L.append(f"| **{nivel}** | evidencia **{b['evidencia']}** | episodios {b['episodios']}: "
                     f"{b['episodios_no_censurados_evaluados']} no censurados ({b['origenes_no_censurados']} orígenes), "
                     f"{b['censurados']} censurados | C-index: {b['origenes_cindex']} orígenes "
                     f"({b['cindex_no_censurados']} no censurados, {b['cindex_censurados']} censurados) | | | |")
            for p, x in b["predictores"].items():
                sd = f"{x['sin_descenso_previsto']:.0%}" if "sin_descenso_previsto" in x else "—"
                eh = x.get("error_h", {})
                mae = f(eh.get("MAE"), 1)
                if eh.get("ic_nota"):
                    mae = f"{eh['MAE']['valor']:.1f} (IC {eh['ic_nota']})"
                L.append(f"| {nivel} | {p} | {x['rol']} | {mae} | "
                         f"{f(x['cindex'].get('C'), 3)} | {f(x['cindex_por_tanque'].get('C'), 3)} | {sd} |")
            if "banda_p10_p90" in b:
                bb = b["banda_p10_p90"]
                L.append(f"| {nivel} | banda P10-P90 del perfil | d2.2 | antes: cobertura {bb['antes']['cobertura']:.0%}, "
                         f"ancho {bb['antes']['ancho_mediano_h']:.1f} h | después (conforme): cobertura "
                         f"{bb['despues']['cobertura']:.0%}, ancho {bb['despues']['ancho_mediano_h']:.1f} h | | |")
        L.append("")
    for minutos, r in res["nivel"].items():
        L += [f"## Pronóstico de nivel a {minutos} min (conjunto final; mismas filas para todos)", "",
              "| Nivel | Predictor | n | MAE % [IC 95 %] | Mejora vs persistencia | Mejora vs tendencia |",
              "|---|---|---|---|---|---|"]
        for nivel, b in r.items():
            if not b.get("predictores"):
                L.append(f"| {nivel} | — | 0 | {b.get('estado', '')} | | |")
                continue
            for p, x in b["predictores"].items():
                mp = f"{x['mejora_vs_persistencia']:+.1%}" if "mejora_vs_persistencia" in x else ""
                mt = f"{x['mejora_vs_tendencia']:+.1%}" if "mejora_vs_tendencia" in x else ""
                L.append(f"| {nivel} | {p.removeprefix('pred_')} | {x['n']} | {f(x['MAE'])} | {mp} | {mt} |")
        L.append("")
    return "\n".join(L)


def main():
    t0 = time.time()
    fecha = datetime.now(timezone.utc).strftime("%Y%m%d")
    carpeta_d21 = RAIZ / CFG2["base_d21"]
    holt21 = json.loads((carpeta_d21 / "holt.json").read_text(encoding="utf-8"))["parametros"]
    carpeta = RAIZ / "batch" / "modelos" / "d2" / f"{CFG2['version']}-{fecha}"
    carpeta.mkdir(parents=True, exist_ok=True)

    d2 = ev.leer_oro("d2_autonomia").dropna(subset=["nivel_pct"])
    d2["runId"] = d2["runId"].fillna("")
    df = m.preparar(d2, UMBRALES).sort_values(m.SERIE + ["ts"]).reset_index(drop=True)
    df["fila"] = np.arange(len(df))
    perfiles = aprender_perfiles(df)
    cal = calibrar_banda(df)
    df = predecir(df, perfiles, holt21, carpeta_d21)
    log(status="predicho", filas=len(df))

    res = {"meta": {"version": CFG2["version"], "fecha": fecha, "base_d21": CFG2["base_d21"],
                    "particion": str(d2["particion_version"].iloc[0]), "config_d22": CFG2, "config_comun": CFG},
           "perfiles": {"|".join(k): p.nivel_detalle | {"consumo_medio_pct_h": round(p.consumo_medio_h(), 4)}
                        for k, p in perfiles.items()},
           "calibracion_banda": cal, "autonomia": {}, "nivel": {}}
    for u in UMBRALES:
        res["autonomia"][str(u)] = evaluar_autonomia(df, u, cal)
        log(status="autonomia", umbral=u)
    for minutos in CFG["horizontes_min"]:
        res["nivel"][str(minutos)] = evaluar_nivel(df, d2, minutos)
        log(status="nivel", minutos=minutos)

    # Modelo para F7 y evidencia
    (carpeta / "perfil.json").write_text(json.dumps({
        "version": CFG2["version"], "fecha": fecha, "modelo": "perfil de consumo por hora (batch/perfil_consumo.py)",
        "umbral_pct": CFG["umbral_pct"], "tope_horas": CFG["tope_horas"],
        "entrada": "nivel % (mediana móvil de 15 min) y hora del dato (epoch ms)",
        "uso_f7": "cifra principal: consumo medio (consumo_medio_pct_h de cada tanque); rango: banda P10-P90 del "
                  "perfil calibrada: [aut_p90 / factor, aut_p10 * factor] con el factor de su nivel",
        "calibracion_banda": cal,
        "tanques": {"|".join(k): {**p.a_dict(), "consumo_medio_pct_h": p.consumo_medio_h()} for k, p in perfiles.items()},
        "nota_esp32": "perfil aprendido con la geometría provisional y pocas horas de consumo: provisional"},
        ensure_ascii=False), encoding="utf-8")
    (carpeta / "manifiesto.json").write_text(json.dumps({
        "version": CFG2["version"], "fecha": fecha, "particion": res["meta"]["particion"],
        "base_d21": CFG2["base_d21"], "archivos": sorted(p.name for p in carpeta.iterdir()),
        "config_d22": CFG2}, ensure_ascii=False, indent=1), encoding="utf-8")
    evid = RAIZ / "docs" / "evidencias" / "f6-d2" / "d2.2"
    evid.mkdir(parents=True, exist_ok=True)
    (evid / "resultados_d2.2.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float),
                                               encoding="utf-8")
    d21 = json.loads((RAIZ / "docs" / "evidencias" / "f6-d2" / "resultados_d2.json").read_text(encoding="utf-8"))
    (evid / "tablas_d2.2.md").write_text(tablas_md(res, d21), encoding="utf-8")
    shutil.copy(carpeta / "manifiesto.json", evid / "manifiesto_modelos_d2.2.json")
    log(status="listo", segundos=round(time.time() - t0), modelos=str(carpeta))


if __name__ == "__main__":
    main()
