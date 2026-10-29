"""
F6-D4: modelo de la próxima llegada de agua (dimensión de Wilbert).

  Llegadas   principal: tabla oro d4_llegadas (detector congelado f4.3), sin los artefactos de reanudación ni las
             no_verificable (se reportan aparte). Complemento (ThingSpeak): llenados por subida sostenida > 10 pp,
             el criterio de D2 y de la adecuación de D4, porque f4.3 no ve los llenados de los canales nivelPct.
  Modelo     supervivencia en tiempo discreto por hora (batch/llegadas.py): riesgo según las horas desde la última
             llegada (tabla de vida = Kaplan-Meier, o Weibull si hay intervalos suficientes) por la hora del día.
             De él: hora más probable, mediana, ventana P10-P90 calibrada por predicción conforme (conjunto de
             ajuste de p6.1) y la probabilidad de llegada en 6/12/24 h. Un modelo por tanque, solo con su
             partición de entrenamiento.
  Bases      (a) misma hora del día que la última llegada; (b) última llegada + mediana del intervalo del tanque.
  Evaluación por nivel y fuente: error en horas por intervalo entre llegadas (episodio), cobertura de la ventana,
             C-index global y por tanque, sesgo de selección, y el ingrediente observable siempre: probabilidad de
             llegada en H horas (Brier, skill, calibración). Caudal de llenado: descriptivo.

  .venv/Scripts/python batch/modelo_d4.py
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
import modelo_d2 as m2  # noqa: E402
from llegadas import ModeloLlegadas  # noqa: E402

CFG = yaml.safe_load((RAIZ / "batch" / "modelo_d4.yml").read_text(encoding="utf-8"))
H = 3_600_000
LIMA_MS = -5 * H
TANQUE = ["fuente", "sensorId", "runId"]
FINAL = {"consistencia": "consistencia", "thingspeak_real": "prueba", "esp32_real": "prueba",
         "fuera_de_alcance": "prueba"}
FUERA = CFG.get("fuera_de_alcance", {})
ESP32_PENDIENTE = m2.ESP32_PENDIENTE
PREDICTORES = ["km_moda", "km_mediana", "weibull_moda", "base_misma_hora", "base_mediana_intervalo"]


def log(**c):
    print(json.dumps({"service": "f6-d4", **c}, ensure_ascii=False, default=str), flush=True)


def hora_lima(ts_ms):
    return ((np.asarray(ts_ms, np.int64) + LIMA_MS) // H) % 24


# --- 1. Llegadas -------------------------------------------------------------------------------------------------

def llegadas_detector():
    d4 = ev.leer_oro("d4_llegadas")
    d4["runId"] = d4["runId"].fillna("")
    ll = d4[d4["tipo"] == "llegada_agua"].copy()
    excluir = ll["artefacto_reanudacion"].fillna(False).astype(bool) & CFG["excluir"]["artefacto_reanudacion"]
    excluir |= ll["bitacora"].isin(CFG["excluir"]["bitacora"])
    ll["motivo_exclusion"] = np.where(ll["artefacto_reanudacion"].fillna(False).astype(bool), "artefacto_reanudacion",
                                      np.where(ll["bitacora"].isin(CFG["excluir"]["bitacora"]), ll["bitacora"], None))
    validas = ll[~excluir][[*TANQUE, "ts", "caudal_lpm", "subida_pct"]].assign(fuente_llegadas="detector_f43")
    return validas, ll[excluir]


def llegadas_subida(d2):
    """Llenados por subida sostenida (regla de modelo_d2: > 10 pp sobre la mediana de 15 min, termina cuando deja
    de subir), con su caudal = subida × capacidad / duración."""
    fuentes = CFG["fuentes"]["subida_sostenida"]
    sub = d2[d2["fuente"].isin(fuentes)]
    if sub.empty:
        return pd.DataFrame()
    df = m2.preparar(sub).sort_values(m2.SERIE + ["ts"])
    cap = {sid: g.get("capacidad_l") for sid, g in
           yaml.safe_load((RAIZ / "ingesta" / "tanques.yml").read_text(encoding="utf-8"))["tanques"].items()}
    filas = []
    for clave, g in df.groupby(m2.SERIE, sort=False):
        en = g["en_llenado"].values
        ini = np.flatnonzero(en & ~np.concatenate([[False], en[:-1]]))
        for i in ini:
            j = i
            while j + 1 < len(en) and en[j + 1]:
                j += 1
            # subida desde el mínimo de la media hora previa al disparo hasta el máximo del llenado
            ym, tsv = g["ym"].values, g["ts"].values
            a0 = max(0, i - 6)
            i0 = a0 + int(np.argmin(ym[a0:i + 1]))
            j1 = i + int(np.argmax(ym[i:j + 1]))
            subida = float(ym[j1] - ym[i0])
            minutos = (tsv[j1] - tsv[i0]) / 60_000
            c = cap.get(clave[1])
            filas.append({"fuente": clave[0], "sensorId": clave[1], "runId": clave[2], "ts": int(g["ts"].values[i]),
                          "subida_pct": subida, "caudal_lpm": subida / 100 * c / minutos if c and minutos > 0 else np.nan})
    return pd.DataFrame(filas).assign(fuente_llegadas="subida_sostenida")


# --- 2. Horas en riesgo y objetivos -----------------------------------------------------------------------------

def horas_tanque(d2):
    """Una fila por hora observada de cada tanque, con su partición y el conjunto de ajuste (último 20 % del
    entrenamiento)."""
    h = d2.assign(slot=d2["ts"] // H).groupby([*TANQUE, "slot"], as_index=False).agg(particion=("particion", "first"))
    ent = h[h["particion"] == "entrenamiento"]
    corte = ent.groupby(TANQUE)["slot"].quantile(1 - CFG["frac_ajuste"]).rename("corte").reset_index()
    h = h.merge(corte, on=TANQUE, how="left")
    h["conj"] = np.where((h["particion"] == "entrenamiento") & (h["slot"] >= h["corte"]), "ajuste", h["particion"])
    return h.drop(columns="corte")


def persona_periodo(horas, llegadas):
    """Para cada hora observada con una llegada previa conocida (sin huecos desde ella): horas desde la última
    llegada (e), hora del día, si llega agua en esa hora, y el objetivo de evaluación desde el inicio de la hora:
    tiempo hasta la próxima llegada (observado) o hasta el fin de la observación continua (censurado)."""
    filas = []
    por_tanque = {k: g.sort_values("ts")["ts"].values for k, g in llegadas.groupby(TANQUE)}
    for clave, g in horas.groupby(TANQUE, sort=False):
        ts_ll = por_tanque.get(clave)
        if ts_ll is None or len(ts_ll) == 0:
            continue
        g = g.sort_values("slot")
        slots = g["slot"].values
        corrida = np.concatenate([[0], np.cumsum(np.diff(slots) > 1)])
        for c in np.unique(corrida):
            sel = corrida == c
            s_run, part, conj = slots[sel], g["particion"].values[sel], g["conj"].values[sel]
            t0, t1 = s_run[0] * H, (s_run[-1] + 1) * H
            ll = ts_ll[(ts_ll >= t0) & (ts_ll < t1)]
            if len(ll) == 0:
                continue
            ll_slot = ll // H
            for s, pa, co in zip(s_run, part, conj):
                origen = s * H
                previas = ll[ll < origen]
                if len(previas) == 0:
                    continue  # sin llegada previa observada en esta corrida: no se sabe cuánto pasó
                ultima = previas[-1]
                proximas = ll[ll >= origen]
                filas.append({"fuente": clave[0], "sensorId": clave[1], "runId": clave[2], "slot": s, "origen": origen,
                              "particion": pa, "conj": co, "e": int(s - ultima // H), "hora": int(hora_lima(origen)),
                              "evento_hora": bool((ll_slot == s).any()), "ultima": int(ultima),
                              "episodio": f"{'|'.join(map(str, clave))}#{int(ultima)}",
                              "observado": len(proximas) > 0,
                              "t_obs_h": ((proximas[0] if len(proximas) else t1) - origen) / H})
    return pd.DataFrame(filas)


# --- 3. Modelos y predicciones ----------------------------------------------------------------------------------

def intervalos(pp):
    """Intervalos entre llegadas (h) del conjunto dado, con censura al fin de la observación continua."""
    ini = pp.groupby("episodio").agg(t=("e", "max"), obs=("evento_hora", "max"))
    return ini["t"].values + 1.0, ini["obs"].values.astype(bool)


ZONAS = {sid: g.get("zona") for sid, g in
         yaml.safe_load((RAIZ / "ingesta" / "tanques.yml").read_text(encoding="utf-8"))["tanques"].items()}


def grupo_modelo(fuente, sensor_id, run_id):
    """Clave del modelo: la zona (y la corrida) para las fuentes de agrupar_por_zona; si no, el tanque."""
    if fuente in CFG["modelo"]["agrupar_por_zona"] and ZONAS.get(sensor_id):
        return (fuente, f"zona:{ZONAS[sensor_id]}", run_id)
    return (fuente, sensor_id, run_id)


def ajustar(pp, conj):
    """Un modelo por tanque (o por zona, en la flota), con las horas en riesgo del conjunto `conj`
    ("entrenamiento" = el 80 % inicial, para la calibración conforme; "train" = toda la partición de entrenamiento,
    para el modelo final). La línea base (b) usa la mediana del intervalo de cada TANQUE."""
    c = CFG["modelo"]
    sel = pp["particion"] == "entrenamiento" if conj == "train" else pp["conj"] == "entrenamiento"
    modelos = {}
    pp = pp.assign(grupo=[grupo_modelo(*x) for x in zip(pp["fuente"], pp["sensorId"], pp["runId"])])
    medianas = {}
    for clave, g in pp[sel].groupby(TANQUE):
        t, d = intervalos(g)
        medianas[clave] = float(np.median(t[d])) if d.any() else None
    for clave, g in pp[sel].groupby("grupo"):
        if g["evento_hora"].sum() < c["min_llegadas_entrenamiento"]:
            continue
        km = ModeloLlegadas.ajustar_km(g["e"], g["hora"], g["evento_hora"], c["e_max_h"], c["iteraciones_ipf"],
                                       c["horizonte_h"])
        t, d = intervalos(g)
        wb = (ModeloLlegadas.ajustar_weibull(g["e"], g["hora"], g["evento_hora"], t, d, c["e_max_h"],
                                             horizonte_h=c["horizonte_h"]) if len(t) >= c["weibull_min_intervalos"] else None)
        mediana = float(np.median(t[d])) if d.any() else None
        modelos[clave] = {"km": km, "weibull": wb, "mediana_intervalo_h": mediana, "medianas_tanque": medianas,
                          "tanques": sorted({"|".join(x) for x in zip(g["fuente"], g["sensorId"], g["runId"])}),
                          "base_prob": {Hh: float(np.mean([(tt <= Hh) for tt in g.loc[g["observado"] | (g["t_obs_h"] >= Hh), "t_obs_h"]]))
                                        for Hh in CFG["evaluacion"]["horizontes_prob_h"]}}
    return modelos


def predecir(pp, modelos, prefijo=""):
    out = {k: np.full(len(pp), np.nan) for k in [f"{prefijo}{c}" for c in (
        "km_moda", "km_mediana", "km_p10", "km_p90", "weibull_moda", "weibull_mediana", "base_misma_hora",
        "base_mediana_intervalo", *[f"prob_km_{h}" for h in CFG["evaluacion"]["horizontes_prob_h"]],
        *[f"prob_wb_{h}" for h in CFG["evaluacion"]["horizontes_prob_h"]],
        *[f"prob_base_{h}" for h in CFG["evaluacion"]["horizontes_prob_h"]])]}
    claves = list(zip(pp["fuente"], pp["sensorId"], pp["runId"]))
    for i, (cl, e, hr, origen, ultima) in enumerate(zip(claves, pp["e"], pp["hora"], pp["origen"], pp["ultima"])):
        mo = modelos.get(grupo_modelo(*cl))
        if mo is None:
            continue
        r = mo["km"].predecir(e, hr, CFG["evaluacion"]["horizontes_prob_h"])
        out[f"{prefijo}km_moda"][i], out[f"{prefijo}km_mediana"][i] = r["moda_h"], r["p50_h"]
        out[f"{prefijo}km_p10"][i], out[f"{prefijo}km_p90"][i] = r["p10_h"], r["p90_h"]
        for h in CFG["evaluacion"]["horizontes_prob_h"]:
            out[f"{prefijo}prob_km_{h}"][i] = r[f"prob_{h}h"]
            out[f"{prefijo}prob_base_{h}"][i] = mo["base_prob"][h]
        if mo["weibull"] is not None:
            w = mo["weibull"].predecir(e, hr, CFG["evaluacion"]["horizontes_prob_h"])
            out[f"{prefijo}weibull_moda"][i], out[f"{prefijo}weibull_mediana"][i] = w["moda_h"], w["p50_h"]
            for h in CFG["evaluacion"]["horizontes_prob_h"]:
                out[f"{prefijo}prob_wb_{h}"][i] = w[f"prob_{h}h"]
        # Base (a): la próxima vez que el reloj marque la hora de la última llegada
        k = math.floor((origen - ultima) / (24 * H)) + 1
        out[f"{prefijo}base_misma_hora"][i] = (ultima + k * 24 * H - origen) / H
        # Base (b): última + k × mediana del intervalo del tanque (el primer múltiplo que cae después del origen)
        med = mo["medianas_tanque"].get(cl)
        if med:
            k = math.floor((origen - ultima) / (med * H)) + 1
            out[f"{prefijo}base_mediana_intervalo"][i] = (ultima + k * med * H - origen) / H
    for k, v in out.items():
        pp[k] = v
    return pp


def calibrar(pp_cal):
    """Predicción conforme (split) de la ventana P10-P90, en horas, por nivel: puntaje max(P10 - y, y - P90) en los
    orígenes del conjunto de AJUSTE con llegada observada (modelo ajustado sin el ajuste)."""
    alfa = CFG["conforme"]["alfa"]
    out = {}
    for nivel, g in pp_cal.groupby("nivel"):
        v = g[(g["conj"] == "ajuste") & g["observado"]].dropna(subset=["cal_km_p10", "cal_km_p90"])
        if v.empty:
            out[nivel] = {"estado": "sin llegadas observadas en el ajuste: no se calibra"}
            continue
        k_ep = int(v["episodio"].nunique())
        if k_ep < CFG["conforme"]["min_episodios"]:
            out[nivel] = {"estado": f"no calibrable: {k_ep} episodios en el ajuste (< {CFG['conforme']['min_episodios']})"}
            continue
        s = np.maximum(v["cal_km_p10"] - v["t_obs_h"], v["t_obs_h"] - v["cal_km_p90"]).values
        n = len(s)
        q = float(np.quantile(s, min(1.0, math.ceil((n + 1) * (1 - alfa)) / n), method="higher"))
        out[nivel] = {"q_h": q, "casos": n, "episodios": int(v["episodio"].nunique()), "alfa": alfa}
    return out


# --- 4. Evaluación -----------------------------------------------------------------------------------------------

def ic_episodios(r, k_ep):
    e = CFG["evaluacion"]
    if k_ep < e["min_episodios_ic"]:
        r["MAE"]["ic95_calculado"] = r["MAE"]["ic95"]
        r["MAE"]["ic95"] = [None, None]
        r["ic_nota"] = f"no informativo: {k_ep} episodios (< {e['min_episodios_ic']})"
    return r


def km_mediana_intervalos(t, d):
    """Mediana Kaplan-Meier de intervalos con censura (None si la curva no baja de 0.5)."""
    orden = np.argsort(t)
    t, d = t[orden], d[orden]
    S, n = 1.0, len(t)
    for i, (ti, di) in enumerate(zip(t, d)):
        if di:
            S *= 1 - 1 / (n - i)
            if S <= 0.5:
                return float(ti)
    return None


def evaluar_nivel(pp, nivel, cal, llegadas_usadas):
    e = CFG["evaluacion"]
    d = pp[(pp["nivel"] == nivel) & (pp["particion"] == FINAL[nivel]) & pp["km_moda"].notna()]
    if d.empty:
        return {"estado": "sin datos evaluables", "llegadas_usadas": int(llegadas_usadas)}
    tanque = (d["fuente"] + "|" + d["sensorId"] + "|" + d["runId"]).values
    ep = d.groupby("episodio").agg(t=("t_obs_h", "max"), obs=("observado", "max"))
    v = d[d["observado"]]
    k_ep = int(v["episodio"].nunique())
    b = {"tanques": sorted(set(tanque)), "llegadas_usadas": int(llegadas_usadas), "origenes": int(len(d)),
         "episodios": int(len(ep)), "episodios_observados": k_ep, "episodios_censurados": int((~ep["obs"]).sum()),
         "origenes_observados": int(len(v)),
         "evidencia": "suficiente" if k_ep >= e["min_episodios_evidencia"] else "indicativa", "predictores": {}}
    # Error en horas (por episodio = intervalo entre llegadas)
    for p in PREDICTORES:
        if v[p].notna().sum() == 0:
            continue
        vv = v[v[p].notna()]
        r = ev.evaluar_regresion(vv["t_obs_h"].values, vv[p].values, n_boot=e["n_boot"], grupos=vv["episodio"].values)
        r = ic_episodios(r, int(vv["episodio"].nunique()))
        c = ev.evaluar_concordancia(d["t_obs_h"].values, d["observado"].values, d[p].values,
                                    grupos=d["episodio"].values, n_boot=200)
        cpt = ev.evaluar_concordancia(d["t_obs_h"].values, d["observado"].values, d[p].values,
                                      grupos=d["episodio"].values, n_boot=200, estratos=tanque)
        b["predictores"][p] = {"error_h": r, "cindex": c, "cindex_por_tanque": cpt}
    # Desglose por tanque (error medio en horas, intervalos observados)
    b["por_tanque"] = {}
    for tq, g in v.assign(tq=v["fuente"] + "|" + v["sensorId"] + "|" + v["runId"]).groupby("tq"):
        b["por_tanque"][tq] = {"episodios": int(g["episodio"].nunique()),
                               **{p: float((g[p] - g["t_obs_h"]).abs().mean()) for p in PREDICTORES if g[p].notna().all()}}
    # Ventana P10-P90 (cruda y calibrada)
    if len(v):
        q = cal.get(nivel, {}).get("q_h")
        lo, hi, y = v["km_p10"].values, v["km_p90"].values, v["t_obs_h"].values
        b["ventana_p10_p90"] = {"antes": {"cobertura": float(np.mean((lo <= y) & (y <= hi))),
                                          "ancho_mediano_h": float(np.median(hi - lo))}, "calibracion": cal.get(nivel)}
        if q is not None:
            lo_c, hi_c = np.maximum(lo - q, 0), hi + q
            b["ventana_p10_p90"]["despues"] = {"cobertura": float(np.mean((lo_c <= y) & (y <= hi_c))),
                                               "ancho_mediano_h": float(np.median(hi_c - lo_c))}
    # Sesgo de selección: ¿los intervalos observados son más cortos que todos (con censura)?
    t_all, d_all = ep["t"].values, ep["obs"].values.astype(bool)
    b["sesgo_seleccion"] = {"mediana_observados_h": float(np.median(t_all[d_all])) if d_all.any() else None,
                            "mediana_km_todos_h": km_mediana_intervalos(t_all, d_all),
                            "fraccion_censurados": float((~d_all).mean())}
    # Ingrediente observable: ¿llega agua en las próximas H horas?
    b["probabilidad"] = {}
    for Hh in e["horizontes_prob_h"]:
        w = d[d["observado"] | (d["t_obs_h"] >= Hh)]
        y = (w["observado"] & (w["t_obs_h"] <= Hh)).astype(float).values
        st = (w["fuente"] + "|" + w["sensorId"] + "|" + w["runId"]).values
        r = {"ventanas": int(len(w)), "frecuencia": float(y.mean()) if len(y) else None}
        for nombre, col in (("km", f"prob_km_{Hh}"), ("weibull", f"prob_wb_{Hh}")):
            if w[col].notna().all() and len(w):
                r[nombre] = ev.evaluar_probabilidad(y, w[col].values, base=w[f"prob_base_{Hh}"].values,
                                                    largo_bloque=Hh, n_boot=e["n_boot"], estratos=st)
        for nombre, col in (("base_misma_hora", "base_misma_hora"), ("base_mediana_intervalo", "base_mediana_intervalo")):
            if len(w) and w[col].notna().all():
                r[nombre] = ev.evaluar_probabilidad(y, (w[col].values <= Hh).astype(float),
                                                    base=w[f"prob_base_{Hh}"].values, largo_bloque=Hh,
                                                    n_boot=e["n_boot"], estratos=st)
        b["probabilidad"][str(Hh)] = r
    return b


# --- 5. Tablas ----------------------------------------------------------------------------------------------------

def f(mt, dec=1):
    if not mt or mt.get("valor") is None:
        return "—"
    ic = mt.get("ic95") or [None, None]
    return (f"{mt['valor']:.{dec}f} [{ic[0]:.{dec}f}, {ic[1]:.{dec}f}]" if ic[0] is not None
            else f"{mt['valor']:.{dec}f} (sin IC)")


def tablas_md(res):
    L = [f"# Resultados D4 {res['meta']['version']}, {res['meta']['fecha']}", ""]
    for fuente_ll, por_nivel in res["evaluacion"].items():
        L += [f"## Llegadas: {fuente_ll}", "", "### Hora de la próxima llegada (conjunto final)", "",
              "| Nivel | Episodios (observados / censurados) | Evidencia | Predictor | Error h [IC 95 % por episodio] | C-index global | C-index por tanque |",
              "|---|---|---|---|---|---|---|"]
        for nivel, b in por_nivel.items():
            if "predictores" not in b:
                L.append(f"| {nivel} | — | {b.get('estado', '')} {b.get('razon', '')} | | | | |")
                continue
            for p, x in b["predictores"].items():
                eh = x["error_h"]
                mae = f(eh["MAE"]) if not eh.get("ic_nota") else f"{eh['MAE']['valor']:.1f} (IC {eh['ic_nota']})"
                L.append(f"| {nivel} | {b['episodios_observados']} / {b['episodios_censurados']} | {b['evidencia']} | "
                         f"{p} | {mae} | {f(x['cindex'].get('C'), 3)} | {f(x['cindex_por_tanque'].get('C'), 3)} |")
            if "ventana_p10_p90" in b:
                vt = b["ventana_p10_p90"]
                dsp = (f"{vt['despues']['cobertura']:.0%} (ancho {vt['despues']['ancho_mediano_h']:.1f} h)"
                       if "despues" in vt else "sin calibrar")
                L.append(f"| {nivel} | ventana P10-P90 | | cobertura antes {vt['antes']['cobertura']:.0%} (ancho "
                         f"{vt['antes']['ancho_mediano_h']:.1f} h) → después {dsp} | | | |")
            sb = b["sesgo_seleccion"]
            r1 = lambda x: "—" if x is None else f"{x:.1f}"  # noqa: E731
            L.append(f"| {nivel} | sesgo de selección | | intervalo mediano: observados {r1(sb['mediana_observados_h'])} h vs "
                     f"Kaplan-Meier (todos) {r1(sb['mediana_km_todos_h'])} h; censurados {sb['fraccion_censurados']:.0%} | | | |")
            for tq, x in b.get("por_tanque", {}).items():
                errores = ", ".join(f"{p} {x[p]:.1f}" for p in PREDICTORES if p in x)
                L.append(f"| {nivel} | por tanque: {tq} | {x['episodios']} episodios | {errores} | | | |")
        L += ["", "### Probabilidad de llegada en las próximas H horas (ingrediente observable; conjunto final)", "",
              "| Nivel | H | Ventanas | Frecuencia | Predictor | Brier [IC 95 %] | Skill vs frecuencia del entrenamiento [IC 95 %] | Calibración global |",
              "|---|---|---|---|---|---|---|---|"]
        for nivel, b in por_nivel.items():
            for Hh, r in b.get("probabilidad", {}).items():
                for p in ("km", "weibull", "base_misma_hora", "base_mediana_intervalo"):
                    if p in r:
                        x = r[p]
                        L.append(f"| {nivel} | {Hh} h | {r['ventanas']} | {r['frecuencia']:.2f} | {p} | "
                                 f"{f(x['brier'], 3)} | {f(x.get('skill'), 2)} | {x['calibracion_global']:+.2f} |")
        L.append("")
    L += ["## Caudal de llenado estimado (L/min, descriptivo)", "", "| Fuente de llegadas | Tanque | Llenados | P10 | Mediana | P90 |",
          "|---|---|---|---|---|---|"]
    for c in res["caudal"]:
        L.append(f"| {c['fuente_llegadas']} | {c['tanque']} | {c['n']} | {c['p10']} | {c['mediana']} | {c['p90']} |")
    L += ["", "## Llegadas excluidas (detector f4.3)", "", "| Tanque | Hora (UTC) | Motivo |", "|---|---|---|"]
    for x in res["excluidas"]:
        L.append(f"| {x['tanque']} | {x['ts_utc']} | {x['motivo']} |")
    return "\n".join(L)


# --- main -------------------------------------------------------------------------------------------------------

def main():
    t0 = time.time()
    fecha = datetime.now(timezone.utc).strftime("%Y%m%d")
    carpeta = RAIZ / "batch" / "modelos" / "d4" / f"{CFG['version']}-{fecha}"
    carpeta.mkdir(parents=True, exist_ok=True)
    d2 = ev.leer_oro("d2_autonomia").dropna(subset=["nivel_pct"])
    d2["runId"] = d2["runId"].fillna("")
    horas = horas_tanque(d2)
    det, excluidas = llegadas_detector()
    fuentes = {"detector_f43": det}
    sub = llegadas_subida(d2)
    if len(sub):
        fuentes["subida_sostenida"] = sub
    res = {"meta": {"version": CFG["version"], "fecha": fecha, "particion": str(d2["particion_version"].iloc[0]),
                    "config": CFG}, "evaluacion": {}, "calibracion": {}, "modelos": {}}
    guardar = {}
    for nombre, ll in fuentes.items():
        pp = persona_periodo(horas, ll)
        pp["nivel"] = np.where(pp["sensorId"].isin(list(FUERA)), "fuera_de_alcance", pp["fuente"].map(ev.nivel_de))
        mods = ajustar(pp, "train")
        mods_cal = ajustar(pp, "entrenamiento")
        pp = predecir(pp, mods)
        pp = predecir(pp, mods_cal, prefijo="cal_")
        cal = calibrar(pp)
        res["calibracion"][nombre] = cal
        res["modelos"][nombre] = {"|".join(k): {"km": v["km"].detalle, "weibull": v["weibull"].detalle if v["weibull"] else None,
                                                 "mediana_intervalo_h": v["mediana_intervalo_h"]} for k, v in mods.items()}
        res["evaluacion"][nombre] = {}
        part_hora = {(f_, s_, r_, sl): pa for f_, s_, r_, sl, pa in
                     zip(horas["fuente"], horas["sensorId"], horas["runId"], horas["slot"], horas["particion"])}
        for nivel in FINAL:
            nivel_ll = np.where(ll["sensorId"].isin(list(FUERA)), "fuera_de_alcance", ll["fuente"].map(ev.nivel_de))
            ll_n = ll[nivel_ll == nivel]
            if ll_n.empty:
                continue
            usadas = sum(part_hora.get((f_, s_, r_, t_ // H)) == FINAL[nivel]
                         for f_, s_, r_, t_ in zip(ll_n["fuente"], ll_n["sensorId"], ll_n["runId"], ll_n["ts"]))
            if nivel == "esp32_real" and pp[(pp["nivel"] == nivel) & (pp["particion"] == "prueba")].empty:
                res["evaluacion"][nombre][nivel] = {"estado": "pendiente", "razon": ESP32_PENDIENTE,
                                                    "llegadas_totales": int(len(ll_n))}
                continue
            res["evaluacion"][nombre][nivel] = evaluar_nivel(pp, nivel, cal, usadas)
            log(status="evaluado", fuente_llegadas=nombre, nivel=nivel)
        guardar[nombre] = {"|".join(k): {"fuera_de_alcance": FUERA.get(k[1]), "km": v["km"].a_dict(), "weibull": v["weibull"].a_dict() if v["weibull"] else None,
                                          "mediana_intervalo_h": v["mediana_intervalo_h"],
                                          "ventana_q_h": cal.get("fuera_de_alcance" if k[1] in FUERA else ev.nivel_de(k[0]), {}).get("q_h")}
                           for k, v in mods.items()}
    # Caudal (descriptivo)
    caudal = []
    for nombre, ll in fuentes.items():
        for clave, g in ll.dropna(subset=["caudal_lpm"]).groupby(TANQUE):
            x = g["caudal_lpm"].values
            caudal.append({"fuente_llegadas": nombre, "tanque": "|".join(clave), "n": int(len(x)),
                           "p10": round(float(np.percentile(x, 10)), 1), "mediana": round(float(np.median(x)), 1),
                           "p90": round(float(np.percentile(x, 90)), 1)})
    res["caudal"] = caudal
    res["excluidas"] = [{"tanque": f"{r.fuente}|{r.sensorId}", "ts_utc": datetime.fromtimestamp(r.ts / 1000, timezone.utc)
                         .strftime("%Y-%m-%d %H:%M"), "motivo": r.motivo_exclusion} for r in excluidas.itertuples()]
    # Modelo para F7 y evidencia
    (carpeta / "modelo.json").write_text(json.dumps({
        "version": CFG["version"], "fecha": fecha, "modelo": "supervivencia por hora (batch/llegadas.py)",
        "uso_f7": "por tanque: predecir(horas desde la última llegada, hora de Lima) -> hora más probable, ventana "
                  "P10-P90 calibrada [p10 - q, p90 + q] y probabilidad de llegada en 6/12/24 h",
        "fuentes_llegadas": guardar}, ensure_ascii=False), encoding="utf-8")
    (carpeta / "manifiesto.json").write_text(json.dumps({"version": CFG["version"], "fecha": fecha,
                                                         "particion": res["meta"]["particion"], "config": CFG},
                                                        ensure_ascii=False, indent=1), encoding="utf-8")
    evid = RAIZ / "docs" / "evidencias" / "f6-d4"
    evid.mkdir(parents=True, exist_ok=True)
    (evid / "resultados_d4.json").write_text(json.dumps(res, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (evid / "tablas_d4.md").write_text(tablas_md(res), encoding="utf-8")
    shutil.copy(carpeta / "manifiesto.json", evid / "manifiesto_modelos_d4.json")
    log(status="listo", segundos=round(time.time() - t0), modelos=str(carpeta))


if __name__ == "__main__":
    main()
