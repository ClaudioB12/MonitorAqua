"""
F6-D8: temperatura del agua almacenada y horas de residencia (dimensión de Josué).

  Temperatura  inercia de primer orden ajustada (batch/termico.py): agua = Σ g_k · F_τ(aire_k) + c, con el aire de la
               cámara (en el canal real, el aire dentro del tanque) y el aire de Open-Meteo. Se entrena y evalúa con
               ThingSpeak 3418755, el único canal con agua real, sin usar nunca el agua medida como entrada: el agua
               se simula desde el aire, como en Juliaca. Pronóstico a 6 y 12 h con el aire futuro = ciclo + anomalía.
               Banda P10-P90 conforme (conjunto de ajuste). Tres variantes: aire + Open-Meteo, solo aire, solo
               Open-Meteo (el ESP32 casi nunca transmite de noche).
  Bases        (a) agua = aire actual; (b) agua = media del aire de las últimas 24 h; (c) la inercia provisional de
               la regla umbral_termico congelada (τ = 25.2 h, ganancia 1).
  Cruce        los umbrales configurados (4 y 25 °C) nunca se cruzan en el canal real: probabilidad de cruce en 12 h
               con umbrales de evaluación declarados (P10 y P90 del agua del entrenamiento), Brier y calibración.
  Juliaca      error de TRANSFERENCIA (no hay agua medida): diferencia con la provisional e incertidumbre por
               parámetros (bootstrap del ajuste), sobre la flota y el ESP32.
  Residencia   edad media del agua en un tanque bien mezclado con las llegadas de D4: cada llegada aporta agua de
               edad 0 en proporción al volumen renovado. Descriptiva, y su relación con la temperatura.

  .venv/Scripts/python batch/modelo_d8.py
"""
import json
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import yaml

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "batch"))
import evaluacion as ev  # noqa: E402
from particion import Particion  # noqa: E402
from termico import ModeloTermico, cuantil_conforme  # noqa: E402

CFG = yaml.safe_load((RAIZ / "batch" / "modelo_d8.yml").read_text(encoding="utf-8"))
REGLAS = yaml.safe_load((RAIZ / "streaming" / "reglas_servicio.yml").read_text(encoding="utf-8"))
PROV = REGLAS["umbral_termico"]
H = 3_600_000
LIMA = timezone(timedelta(hours=-5))
EVID = RAIZ / "docs" / "evidencias" / "f6-d8"
OM_CANAL = RAIZ / "batch" / "datos" / "fuentes" / "f8_open_meteo_3418755.json"
HMAX = CFG["horizonte_cruce_h"]


def log(**c):
    print(json.dumps({"service": "f6-d8", **c}, ensure_ascii=False, default=str), flush=True)


def taus():
    t = CFG["taus_h"]
    return np.unique(np.round(np.geomspace(t["desde"], t["hasta"], t["n"]), 2))


# --- 1. Datos del canal real ------------------------------------------------------------------------------------

def open_meteo_canal(desde):
    """Open-Meteo horario en la ubicación del canal 3418755 (misma API pública que batch/descargar_fuentes.py)."""
    if not OM_CANAL.exists():
        p = {**CFG["ubicacion_canal"], "hourly": "temperature_2m", "timezone": "UTC"}
        archivo = requests.get("https://archive-api.open-meteo.com/v1/archive", timeout=120,
                               params={**p, "start_date": desde.isoformat(), "end_date": date.today().isoformat()}).json()
        reciente = requests.get("https://api.open-meteo.com/v1/forecast", timeout=120,
                                params={**p, "past_days": 14, "forecast_days": 0}).json()
        OM_CANAL.write_text(json.dumps({"descargado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                        "archivo": archivo, "pronostico_past_days": reciente}), encoding="utf-8")
    d = json.loads(OM_CANAL.read_text(encoding="utf-8"))
    serie = {}
    for k in ("archivo", "pronostico_past_days"):  # lo reciente del pronóstico completa el atraso del archivo
        hh = d[k].get("hourly", {})
        for t, v in zip(hh.get("time", []), hh.get("temperature_2m", [])):
            if v is not None:
                serie[int(datetime.fromisoformat(t).replace(tzinfo=timezone.utc).timestamp() * 1000)] = v
    return pd.Series(serie).sort_index()


def grilla_canal():
    d8 = ev.leer_oro("d8_termica")
    t = d8[d8["sensorId"] == CFG["canal_validacion"]].sort_values("ts").set_index("ts")
    ini = int(t.index.min()) - CFG["spin_up_h"] * H
    om = open_meteo_canal(datetime.fromtimestamp(ini / 1000, timezone.utc).date())
    idx = np.arange(ini, int(t.index.max()) + H, H)
    g = pd.DataFrame(index=idx)
    g["om"] = om.reindex(idx).interpolate(limit=3).to_numpy()
    for col, nuevo in (("temp_aire_camara_c", "aire_medido"), ("temp_agua_real_c", "agua"), ("particion", "particion"),
                       ("nivel_pct", "nivel_pct")):
        g[nuevo] = t[col].reindex(idx).to_numpy()
    g["hora"] = (g.index // H) % 24
    ent = (g["particion"] == "entrenamiento") & g["aire_medido"].notna() & g["om"].notna()
    beta, alfa = np.polyfit(g.loc[ent, "om"], g.loc[ent, "aire_medido"], 1)
    g["aire"] = g["aire_medido"].fillna(alfa + beta * g["om"])  # huecos del aire: con Open-Meteo, como en F7
    # Conjuntos: el último 20 % del entrenamiento (por tiempo) es el AJUSTE
    corte = g.index[g["particion"] == "entrenamiento"].to_series().quantile(1 - CFG["frac_ajuste"])
    g["conj"] = np.where((g["particion"] == "entrenamiento") & (g.index >= corte), "ajuste",
                         np.where(g["particion"] == "entrenamiento", "entrenamiento", g["particion"].fillna("spin_up")))
    g.attrs["relleno"] = {"alfa": float(alfa), "beta": float(beta), "huecos_rellenos_h": int(g["aire_medido"].isna().sum())}
    if g["om"].isna().any():
        raise SystemExit(f"Open-Meteo del canal tiene {int(g['om'].isna().sum())} horas sin dato")
    return g


# --- 2. Modelos y predicciones ----------------------------------------------------------------------------------

def matriz(fn_estimar, fn_pronostico, n):
    """P[i, h] para h = 0..HMAX: h = 0 la estimación actual, h > 0 el pronóstico desde i."""
    P = np.full((n, HMAX + 1), np.nan)
    P[:, 0] = fn_estimar()
    for i in range(CFG["spin_up_h"], n - 1):
        P[i, 1:] = fn_pronostico(i)
    return P


def predicciones_modelo(m, U, hora):
    F = m.estados(U)
    return matriz(lambda: m.estimar(U), lambda i: m.pronosticar(U, hora, i, HMAX, F), len(hora))


def predicciones_bases(g):
    aire = g["aire"].to_numpy()
    n = len(aire)
    media24 = pd.Series(aire).rolling(24, min_periods=12).mean().to_numpy()
    out = {"base_a_aire_actual": np.repeat(aire[:, None], HMAX + 1, axis=1),
           "base_b_media_aire_24h": np.repeat(media24[:, None], HMAX + 1, axis=1)}
    prov = ModeloTermico(["aire"], PROV["tau_h"], [1.0], 0.0, nombre="provisional_f44")
    out["base_c_provisional_f44"] = predicciones_modelo(prov, {"aire": aire}, g["hora"].to_numpy())
    return out, n


def pares(g, P, h, conj):
    """(y, pred, ts) con el objetivo en i + h dentro del conjunto."""
    agua, c = g["agua"].to_numpy(), g["conj"].to_numpy()
    i = np.arange(len(agua) - h)
    ok = (c[i + h] == conj) & ~np.isnan(agua[i + h]) & ~np.isnan(P[i, h]) & (i >= CFG["spin_up_h"])
    return agua[i + h][ok], P[i, h][ok], g.index.to_numpy()[i + h][ok]


def evaluar_temperatura(g, preds, conj):
    nb = CFG["evaluacion"]["n_boot"]
    out = {}
    for h in CFG["horizontes_h"]:
        fila = {}
        for nombre, P in preds.items():
            y, p, ts = pares(g, P, h, conj)
            fila[nombre] = ev.evaluar_regresion(y, p, largo_bloque=CFG["evaluacion"]["largo_bloque_h"], n_boot=nb,
                                                mape_minimo=1.0)
        y, _, ts = pares(g, preds[next(iter(preds))], h, conj)
        dias = int(len({datetime.fromtimestamp(x / 1000, timezone.utc).date() for x in ts}))
        fila["_dias"] = dias
        fila["_evidencia"] = "suficiente" if dias >= CFG["evaluacion"]["min_dias_evidencia"] else "indicativa"
        out[f"{h}h"] = fila
    return out


def residuos(g, P, conj):
    return {h: (lambda y, p, _: y - p)(*pares(g, P, h, conj)) for h in range(HMAX + 1)}


def cobertura(g, m, P, conj):
    out = {}
    for h in CFG["horizontes_h"]:
        y, p, _ = pares(g, P, h, conj)
        lo, hi = m.banda(p, h)
        if lo is None or not len(y):
            out[f"{h}h"] = {"n": int(len(y))}
            continue
        dentro = (y >= lo) & (y <= hi)
        out[f"{h}h"] = {"n": int(len(y)), "cobertura": float(dentro.mean()), "ancho_c": float(np.median(hi - lo))}
    return out


# --- 3. Cruce de umbral -----------------------------------------------------------------------------------------

def eventos_cruce(g, umbral, lado):
    """y[i] = el agua está del lado seguro en i y cruza el umbral en las HMAX horas siguientes (>= 10 h con dato)."""
    agua = g["agua"].to_numpy()
    n = len(agua)
    y = np.full(n, np.nan)
    for i in range(n - HMAX):
        fut = agua[i + 1:i + HMAX + 1]
        if np.isnan(agua[i]) or np.sum(~np.isnan(fut)) < HMAX - 2:
            continue
        if lado == "bajo":
            y[i] = float(np.nanmin(fut) < umbral) if agua[i] > umbral else np.nan
        else:
            y[i] = float(np.nanmax(fut) > umbral) if agua[i] < umbral else np.nan
    return y


def prob_cruce(P, res_aj, umbral, lado):
    """P(cruce en 12 h) = máx sobre h de la fracción de residuos del ajuste que llevarían el pronóstico al otro lado."""
    n = P.shape[0]
    p = np.full(n, np.nan)
    for h in range(1, HMAX + 1):
        r = np.sort(res_aj[h][~np.isnan(res_aj[h])])
        if not len(r):
            continue
        if lado == "bajo":
            ph = np.searchsorted(r, umbral - P[:, h], side="left") / len(r)
        else:
            ph = 1 - np.searchsorted(r, umbral - P[:, h], side="right") / len(r)
        p = np.fmax(p, ph)
    return p


def evaluar_cruce(g, P_modelo, bases, res_aj):
    """Los umbrales configurados (4 y 25 °C) nunca se cruzan en el canal. Para medir la probabilidad de cruce se usa
    una REJILLA de umbrales de evaluación (cada paso_c, dentro del rango del agua del entrenamiento) y se suman los
    cruces de todos: es la prueba de que la probabilidad está bien calibrada, no de un umbral de riesgo en particular."""
    conf = CFG["umbrales"]["configurados"]
    rej = CFG["umbrales"]["rejilla_evaluacion"]
    agua_s = g["agua"].dropna()
    out = {"umbrales_configurados": conf,
           "horas_del_canal_bajo_el_configurado": int((agua_s < conf["bajo_c"]).sum()),
           "horas_del_canal_sobre_el_configurado": int((agua_s > conf["alto_c"]).sum()),
           "rejilla_evaluacion_c": rej, "por_lado": {}}
    c = g["conj"].to_numpy()
    umbrales = np.arange(rej["desde_c"], rej["hasta_c"] + 1e-9, rej["paso_c"])
    for lado in ("bajo", "alto"):
        y_all, p_all, b_all, base_all, conj_all, ep_all = [], [], {n: [] for n in bases}, [], [], 0
        episodios = {"ajuste": 0, "prueba": 0, "entrenamiento": 0}
        for u in umbrales:
            y = eventos_cruce(g, u, lado)
            freq = np.nanmean(y[c == "entrenamiento"]) if np.any(~np.isnan(y[c == "entrenamiento"])) else np.nan
            pm = prob_cruce(P_modelo, res_aj, u, lado)
            for conj in episodios:
                m = (c == conj) & ~np.isnan(y)
                episodios[conj] += int(np.sum(np.diff(np.r_[0, y[m]]) == 1))
            m = np.isin(c, ["ajuste", "prueba"]) & ~np.isnan(y)
            y_all.append(y[m]), p_all.append(pm[m]), conj_all.append(c[m])
            base_all.append(np.full(m.sum(), freq if not np.isnan(freq) else 0.0))
            for n, P in bases.items():
                fut = P[:, 1:]
                binario = (np.nanmin(fut, 1) < u) if lado == "bajo" else (np.nanmax(fut, 1) > u)
                b_all[n].append(binario[m].astype(float))
        y, pm, base, cj = (np.concatenate(x) for x in (y_all, p_all, base_all, conj_all))
        res_lado = {"umbrales": len(umbrales)}
        for conj in ("ajuste", "prueba"):
            m = cj == conj
            fila = {"origenes": int(m.sum()), "origenes_con_cruce": int(y[m].sum()), "episodios_cruce": episodios[conj],
                    "evidencia": "suficiente" if episodios[conj] >= CFG["evaluacion"]["min_eventos_cruce"] else "indicativa"}
            fila["modelo"] = ev.evaluar_probabilidad(y[m], pm[m], base=base[m], largo_bloque=CFG["evaluacion"]["largo_bloque_h"],
                                                     n_boot=200)
            for n in bases:
                fila[n] = ev.evaluar_probabilidad(y[m], np.concatenate(b_all[n])[m], base=base[m],
                                                  largo_bloque=CFG["evaluacion"]["largo_bloque_h"], n_boot=200)
            res_lado[conj] = fila
        out["por_lado"][lado] = res_lado
    return out


# --- 4. Precisión limitante del DHT11 ------------------------------------------------------------------------------

def precision_dht(g, modelos):
    out = {}
    agua, c = g["agua"].to_numpy(), g["conj"].to_numpy()
    ok = (c == "prueba") & ~np.isnan(agua)
    for nombre, m in modelos.items():
        if "aire" not in m.entradas:
            continue
        base_U = {"aire": g["aire"].to_numpy(), "om": g["om"].to_numpy()}
        w0 = m.estimar(base_U)
        fila = {"mae_base": float(np.mean(np.abs(w0[ok] - agua[ok])))}
        medido = g["aire_medido"].notna().to_numpy()
        for caso, f in (("sesgo_+2C", lambda a: a + CFG["dht11"]["sesgo_c"]),
                        ("sesgo_-2C", lambda a: a - CFG["dht11"]["sesgo_c"]),
                        ("redondeo_1C", lambda a: np.round(a / CFG["dht11"]["resolucion_c"]) * CFG["dht11"]["resolucion_c"]))[:3]:
            a = base_U["aire"].copy()
            a[medido] = f(a[medido])
            w = m.estimar({**base_U, "aire": a})
            fila[caso] = {"cambio_medio_estimacion_c": float(np.mean(w[ok] - w0[ok])),
                          "mae": float(np.mean(np.abs(w[ok] - agua[ok])))}
        out[nombre] = fila
    return out


# --- 5. Transferencia a Juliaca --------------------------------------------------------------------------------

def om_juliaca():
    om = pd.read_parquet(RAIZ / "batch" / "datos" / "plata", filters=[("fuente", "=", "open-meteo"),
                                                                      ("sensorId", "=", "open-meteo-juliaca")],
                         columns=["timestamp", "temperaturaC"])
    return om.groupby(om["timestamp"] // H * H)["temperaturaC"].mean().sort_index()


def ajustes_bootstrap(g, entradas, n_boot=40, semilla=2026):
    """Reajuste del modelo remuestreando días del entrenamiento (bloques de 24 h): incertidumbre de los parámetros."""
    rng = np.random.default_rng(semilla)
    U = {"aire": g["aire"].to_numpy(), "om": g["om"].to_numpy()}
    agua = g["agua"].to_numpy()
    dia = g.index.to_numpy() // (24 * H)
    ent = (g["conj"] == "entrenamiento").to_numpy()
    dias = np.unique(dia[ent])
    out = []
    for _ in range(n_boot):
        elegidos = rng.choice(dias, len(dias), replace=True)
        peso = np.zeros(len(agua))
        for d in elegidos:
            peso[dia == d] += 1
        # mínimos cuadrados ponderados = repetir días; se implementa con la máscara y pesos por fila
        mejor = None
        for tau in taus():
            F = np.column_stack([ModeloTermico(entradas, tau, [0] * len(entradas), 0).estados(U)[e] for e in entradas]
                                + [np.ones(len(agua))])
            m = ent & ~np.isnan(agua) & (peso > 0)
            W = np.sqrt(peso[m])
            coef, *_ = np.linalg.lstsq(F[m] * W[:, None], agua[m] * W, rcond=None)
            mse = float(np.sum(peso[m] * (F[m] @ coef - agua[m]) ** 2) / peso[m].sum())
            if mejor is None or mse < mejor[0]:
                mejor = (mse, tau, coef)
        out.append(ModeloTermico(entradas, mejor[1], mejor[2][:-1], mejor[2][-1]))
    return out


def transferencia(principal, modelos, boot):
    d8 = ev.leer_oro("d8_termica")
    omj = om_juliaca()
    media_om = float(omj.mean())
    amort = CFG["juliaca"]["amortiguacion_camara"]
    prov = ModeloTermico(["aire"], PROV["tau_h"], [1.0], 0.0)
    filas = []
    # Flota: aire de la cámara simulado y Open-Meteo Juliaca, continuos (sin huecos)
    for (fuente, run, sid), t in d8[d8["fuente"].str.startswith("aumentado")].groupby(["fuente", "runId", "sensorId"]):
        t = t.sort_values("ts")
        U = {"aire": t["temp_aire_camara_c"].to_numpy(), "om": t["temp_juliaca_c"].to_numpy()}
        filas.append(("flota", fuente, sid, t["ts"].to_numpy(), U, CFG["spin_up_h"]))
    # ESP32: Open-Meteo Juliaca continuo; el aire de la cámara donde hay DHT11, y si no, cámara = media + 0.6·(OM - media)
    e = d8[d8["fuente"] == "esp32"].sort_values("ts").set_index("ts")
    if len(e):
        idx = np.arange(int(e.index.min()) - CFG["spin_up_h"] * H, int(e.index.max()) + H, H)
        om = omj.reindex(idx).interpolate(limit=3).to_numpy()
        aire = e["temp_aire_camara_c"].reindex(idx).to_numpy()
        aire = np.where(np.isnan(aire), media_om + amort * (om - media_om), aire)
        filas.append(("esp32", "esp32", "esp32-tanque-real-01", idx, {"aire": aire, "om": om}, CFG["spin_up_h"],
                      e.index.to_numpy()))
    out = {}
    for f in filas:
        grupo, fuente, sid, ts, U, spin = f[:6]
        sel = np.isin(ts, f[6]) if len(f) > 6 else (np.arange(len(ts)) >= spin)
        est = {n: m.estimar(U)[sel] for n, m in modelos.items()}
        est["provisional_f44"] = prov.estimar(U)[sel]
        bt = np.array([m.estimar(U)[sel] for m in boot])
        out.setdefault(fuente, []).append({"sensorId": sid, "horas": int(sel.sum()), "est": est,
                                           "p10": np.percentile(bt, 10, 0), "p90": np.percentile(bt, 90, 0)})
    res = {}
    for fuente, lista in out.items():
        e = {n: np.concatenate([x["est"][n] for x in lista]) for n in lista[0]["est"]}
        ancho = np.concatenate([x["p90"] - x["p10"] for x in lista])
        prin = e[principal]
        res[fuente] = {"tanques": len(lista), "horas": int(len(prin)),
                       "estimacion_c": {n: {"p05": float(np.percentile(v, 5)), "p50": float(np.median(v)),
                                            "p95": float(np.percentile(v, 95)), "horas_bajo_4c": int((v < 4).sum())}
                                        for n, v in e.items()},
                       "dif_principal_vs_provisional_c": {"media_abs": float(np.mean(np.abs(prin - e["provisional_f44"]))),
                                                          "media": float(np.mean(prin - e["provisional_f44"]))},
                       "dif_principal_vs_solo_om_c": {"media_abs": float(np.mean(np.abs(prin - e["solo_om"])))},
                       "incertidumbre_parametros_p10_p90_c": {"ancho_mediano": float(np.median(ancho)),
                                                              "ancho_p90": float(np.percentile(ancho, 90))}}
    return res


# --- 6. Residencia ------------------------------------------------------------------------------------------------

def residencia():
    """Edad media del agua (h) en un tanque bien mezclado: cada llenado aporta agua de edad 0 en proporción al volumen
    renovado y el consumo saca agua de todas las edades por igual. El agua que ya estaba antes del primer llenado
    visto tiene edad desconocida: la edad solo se informa cuando más del 90 % del agua tiene edad conocida.
    Llenados: las llegadas de D4 (detector f4.3, tabla d4_llegadas) y los llenados por subida sostenida (criterio de
    D2 y de D4, en_llenado de batch/modelo_d2.py), que son los que ven los canales de ThingSpeak en %."""
    import modelo_d2 as m2
    r = CFG["residencia"]
    d2 = ev.leer_oro("d2_autonomia")
    d4 = ev.leer_oro("d4_llegadas")
    d4 = d4[(d4["tipo"] == "llegada_agua") & ~d4["artefacto_reanudacion"].fillna(False).astype(bool)]
    d2["runId"], d4["runId"] = d2["runId"].fillna(""), d4["runId"].fillna("")
    margen = 30 * 60_000
    ventanas = {k: list(zip(g["ts"] - margen, g["fin_ts"] + margen)) for k, g in d4.groupby(["fuente", "sensorId", "runId"])}
    # Llenados por subida sostenida: en_llenado se activa cuando el nivel YA subió el umbral, así que la ventana se
    # extiende hacia atrás hasta el mínimo del nivel en las 3 h previas (como llegadas_subida de batch/modelo_d4.py)
    pre = m2.preparar(d2).sort_values(["fuente", "sensorId", "runId", "ts"])
    pre["runId"] = pre["runId"].fillna("")
    for k, g in pre.groupby(["fuente", "sensorId", "runId"]):
        en, ts, niv = g["en_llenado"].to_numpy(), g["ts"].to_numpy(), g["nivel_pct"].to_numpy()
        ini = np.flatnonzero(en & ~np.r_[False, en[:-1]])
        for i in ini:
            j = i
            while j + 1 < len(en) and en[j + 1]:
                j += 1
            previos = np.flatnonzero((ts >= ts[i] - 3 * H) & (ts <= ts[i]))
            a = previos[np.nanargmin(niv[previos])] if len(previos) else i
            ventanas.setdefault(k, []).append((ts[a], ts[j]))
    ventanas = {k: np.array(v) for k, v in ventanas.items()}
    paso_h = r["paso_min"] / 60
    filas = []
    for (fuente, sid, run), g in d2.groupby(["fuente", "sensorId", "runId"]):
        g = g.sort_values("ts")
        v = ventanas.get((fuente, sid, run), np.empty((0, 2)))
        ts, nivel, tramo = g["ts"].to_numpy(), g["nivel_pct"].to_numpy(), g["tramo"].to_numpy()
        K = Uk = A = 0.0
        edad = np.full(len(ts), np.nan)
        for i in range(len(ts)):
            V = max(nivel[i], 1.0) if not np.isnan(nivel[i]) else np.nan
            if i == 0 or tramo[i] != tramo[i - 1] or np.isnan(V):
                K, Uk, A = 0.0, (V if not np.isnan(V) else 1.0), 0.0
                continue
            Vp = K + Uk
            d = V - Vp
            llenado = bool(len(v)) and bool(np.any((v[:, 0] < ts[i]) & (ts[i] <= v[:, 1])))
            A += paso_h
            if d > r["subida_min_pp"] and llenado:
                A = A * K / (K + d) if K + d > 0 else 0.0
                K += d
            elif Vp > 0:  # consumo, o ruido fuera de un llenado: cambia el volumen, no la mezcla de edades
                K, Uk = K * V / Vp, Uk * V / Vp
            if Uk / max(K + Uk, 1e-9) < 0.1:
                edad[i] = A
        filas.append(pd.DataFrame({"fuente": fuente, "sensorId": sid, "runId": run, "ts": ts, "edad_h": edad}))
    e = pd.concat(filas, ignore_index=True)
    resumen = {}
    for (fuente, sid), x in e.groupby(["fuente", "sensorId"]):
        y = x["edad_h"].dropna()
        resumen[f"{fuente}|{sid}"] = {"pasos": int(len(x)), "con_edad_conocida": round(float(x["edad_h"].notna().mean()), 3),
                                      "mediana_h": float(y.median()) if len(y) else None,
                                      "p90_h": float(y.quantile(0.9)) if len(y) else None}
    return e, resumen


def relacion_residencia_temperatura(edades, g_canal, temp_flota):
    bins = CFG["residencia"]["bins_h"]
    out = {}

    def tabla(x, col):
        x = x.dropna(subset=["edad_h", col])
        x = x.assign(bin=pd.cut(x["edad_h"], bins, right=False))
        rk = x[["edad_h", col]].rank()
        return {"horas": int(len(x)), "correlacion_spearman": float(rk.corr().iloc[0, 1]) if len(x) > 2 else None,
                "por_edad": [{"edad_h": str(b), "horas": int(len(s)), "temp_media_c": float(s[col].mean())}
                             for b, s in x.groupby("bin", observed=True)]}

    canal = edades[edades["sensorId"] == CFG["canal_validacion"]].copy()
    canal["hora"] = canal["ts"] // H * H
    c = canal.groupby("hora")["edad_h"].mean().to_frame().join(g_canal[["agua"]], how="inner")
    out["canal_real_agua_medida"] = tabla(c, "agua")
    f = edades[edades["fuente"].str.startswith("aumentado")].copy()
    f["hora"] = f["ts"] // H * H
    f = f.groupby(["runId", "sensorId", "hora"])["edad_h"].mean().reset_index().merge(temp_flota, on=["runId", "sensorId", "hora"])
    out["flota_agua_estimada"] = tabla(f, "agua_estimada")
    return out


# --- 7. Tablas ------------------------------------------------------------------------------------------------

def _r(d, dec=2):
    if not d.get("n"):
        return "—"
    ic = d["MAE"]["ic95"]
    ic = f" [{ic[0]:.{dec}f}, {ic[1]:.{dec}f}]" if ic and None not in ic else " (sin IC)"
    return f"{d['MAE']['valor']:.{dec}f}{ic} · sesgo {d['SESGO']['valor']:+.{dec}f}"


def _b(d):
    if "brier" not in d:
        return "—"
    sk = d.get("skill", {}).get("valor")
    return f"{d['brier']['valor']:.3f} (skill {sk:+.2f}; calib. {d['calibracion_global']:+.3f})" if sk is not None else \
        f"{d['brier']['valor']:.3f}"


def tablas_md(r):
    nombres = ["aire_y_om", "solo_aire", "solo_om", "base_a_aire_actual", "base_b_media_aire_24h", "base_c_provisional_f44"]
    w = ["# Tablas D8 (temperatura del agua y residencia)", "",
         f"Modelo `{r['version']}`; principal (elegido en el ajuste): **{r['principal']}**. Canal "
         f"{CFG['canal_validacion']}: {r['datos_canal']['horas']} h con agua medida "
         f"({r['datos_canal']['horas_por_conjunto']}).", "", "| Modelo | τ (h) | Ganancias | Constante (°C) |", "|---|---|---|---|"]
    for n, m in r["parametros"].items():
        w.append(f"| {n} | {m['tau_h']:.1f} | " + ", ".join(f"{e} {g:.3f}" for e, g in zip(m["entradas"], m["ganancias"]))
                 + f" | {m['constante']:+.2f} |")
    w.append(f"\nτ del principal por bootstrap de días: P10 {r['tau_bootstrap']['p10']:.1f} h, mediana "
             f"{r['tau_bootstrap']['p50']:.1f} h, P90 {r['tau_bootstrap']['p90']:.1f} h.")
    for conj, t in (("Diseño (ajuste)", r["temperatura"]["diseno_ajuste"]), ("Prueba", r["temperatura"]["prueba"])):
        w += ["", f"## Temperatura del agua: {conj} (MAE en °C, IC por bloques de 24 h)", "",
              "| Predictor | " + " | ".join(f"{h} ({t[h]['_dias']} días, {t[h]['_evidencia']})" for h in t) + " |",
              "|---|" + "---|" * len(t)]
        for n in nombres:
            w.append(f"| {n} | " + " | ".join(_r(t[h][n]) for h in t) + " |")
    w += ["", "## Cobertura de la banda P10-P90 (nominal 80 %)", "", "| Modelo | Conjunto | 0 h | 6 h | 12 h |", "|---|---|---|---|---|"]
    for n, c in r["cobertura_banda"].items():
        for conj, x in c.items():
            w.append(f"| {n} | {conj} | " + " | ".join(
                f"{x[h]['cobertura']:.0%} (ancho {x[h]['ancho_c']:.2f} °C)" if "cobertura" in x[h] else "—" for h in x) + " |")
    c = r["cruce"]
    w += ["", "## Cruce de umbral en las próximas 12 h", "",
          f"Umbrales configurados {c['umbrales_configurados']}: horas del canal bajo 4 °C = "
          f"{c['horas_del_canal_bajo_el_configurado']}, sobre 25 °C = {c['horas_del_canal_sobre_el_configurado']} "
          "(no evaluables). Rejilla de evaluación: " + str(c["rejilla_evaluacion_c"]) + ".", "",
          "| Lado | Conjunto | Orígenes (con cruce) | Episodios | Evidencia | Modelo: Brier | (a) aire actual | (b) media 24 h | (c) provisional f4.4 |",
          "|---|---|---|---|---|---|---|---|---|"]
    for lado, x in c["por_lado"].items():
        for conj in ("ajuste", "prueba"):
            f = x[conj]
            w.append(f"| {lado} | {conj} | {f['origenes']} ({f['origenes_con_cruce']}) | {f['episodios_cruce']} | {f['evidencia']} | "
                     f"{_b(f['modelo'])} | {_b(f['base_a_aire_actual'])} | {_b(f['base_b_media_aire_24h'])} | "
                     f"{_b(f['base_c_provisional_f44'])} |")
    w += ["", "## Precisión limitante del DHT11 (prueba, estimación actual)", "",
          "| Modelo | MAE base | +2 °C: cambio / MAE | −2 °C: cambio / MAE | Redondeo a 1 °C: cambio / MAE |", "|---|---|---|---|---|"]
    for n, d in r["precision_dht11"].items():
        w.append(f"| {n} | {d['mae_base']:.2f} | " + " | ".join(
            f"{d[k]['cambio_medio_estimacion_c']:+.2f} / {d[k]['mae']:.2f}" for k in ("sesgo_+2C", "sesgo_-2C", "redondeo_1C")) + " |")
    w += ["", "## Transferencia a Juliaca (NO es una medición: no hay agua medida)", "",
          "| Fuente | Horas | Principal P05 / P50 / P95 (°C) | Provisional P50 | Solo OM P50 | Diferencia absoluta principal vs provisional | Incertidumbre por parámetros (ancho P10-P90 mediano) | Horas < 4 °C (principal / provisional) |",
          "|---|---|---|---|---|---|---|---|"]
    for f, x in r["transferencia_juliaca"].items():
        e = x["estimacion_c"]
        w.append(f"| {f} | {x['horas']} | {e[r['principal']]['p05']:.1f} / {e[r['principal']]['p50']:.1f} / "
                 f"{e[r['principal']]['p95']:.1f} | {e['provisional_f44']['p50']:.1f} | {e['solo_om']['p50']:.1f} | "
                 f"{x['dif_principal_vs_provisional_c']['media_abs']:.2f} °C | "
                 f"{x['incertidumbre_parametros_p10_p90_c']['ancho_mediano']:.2f} °C | "
                 f"{e[r['principal']]['horas_bajo_4c']} / {e['provisional_f44']['horas_bajo_4c']} |")
    w += ["", "## Residencia (edad media del agua, tanque bien mezclado)", "",
          "| Tanque | Pasos de 5 min | Con edad conocida | Mediana (h) | P90 (h) |", "|---|---|---|---|---|"]
    for k, v in r["residencia"]["por_tanque"].items():
        med = "—" if v["mediana_h"] is None else f"{v['mediana_h']:.0f}"
        p90 = "—" if v["p90_h"] is None else f"{v['p90_h']:.0f}"
        w.append(f"| {k.replace('|', ' · ')} | {v['pasos']} | {v['con_edad_conocida']:.0%} | {med} | {p90} |")
    for k, v in r["residencia"]["relacion_temperatura"].items():
        w += ["", f"Residencia y temperatura, {k}: {v['horas']} h, correlación de Spearman "
                  f"{v['correlacion_spearman']:+.2f}.", "", "| Edad (h) | Horas | Temperatura media (°C) |", "|---|---|---|"]
        for b_ in v["por_edad"]:
            w.append(f"| {b_['edad_h']} | {b_['horas']} | {b_['temp_media_c']:.2f} |")
    return "\n".join(w) + "\n"


# --- 8. Principal -----------------------------------------------------------------------------------------------

def main():
    t0 = time.time()
    g = grilla_canal()
    U = {"aire": g["aire"].to_numpy(), "om": g["om"].to_numpy()}
    hora = g["hora"].to_numpy()
    agua = g["agua"].to_numpy()
    mascara = (g["conj"] == "entrenamiento").to_numpy()
    modelos = {n: ModeloTermico.ajustar(U, agua, mascara, e, taus(), nombre=n) for n, e in CFG["modelos"].items()}
    preds = {n: predicciones_modelo(m, U, hora) for n, m in modelos.items()}
    bases, _ = predicciones_bases(g)
    todas = {**preds, **bases}

    diseno = evaluar_temperatura(g, todas, "ajuste")
    principal = min(modelos, key=lambda n: diseno["0h"][n]["MAE"]["valor"])
    for n, m in modelos.items():
        m.calibrar(residuos(g, preds[n], "ajuste"), CFG["conforme"]["alfa"], CFG["conforme"]["min_n"])
    prueba = evaluar_temperatura(g, todas, "prueba")
    cob = {n: {"ajuste": cobertura(g, m, preds[n], "ajuste"), "prueba": cobertura(g, m, preds[n], "prueba")}
           for n, m in modelos.items()}
    res_aj = residuos(g, preds[principal], "ajuste")
    cruce = evaluar_cruce(g, preds[principal], bases, res_aj)
    dht = precision_dht(g, modelos)
    log(status="canal", principal=principal, taus={n: m.tau for n, m in modelos.items()})

    boot = ajustes_bootstrap(g, modelos[principal].entradas)
    transf = transferencia(principal, modelos, boot)

    # Agua estimada en la flota (modelo principal), para relacionarla con la residencia
    d8 = ev.leer_oro("d8_termica")
    temp_flota = []
    for (run, sid), t in d8[d8["fuente"].str.startswith("aumentado")].groupby(["runId", "sensorId"]):
        t = t.sort_values("ts")
        w = modelos[principal].estimar({"aire": t["temp_aire_camara_c"].to_numpy(), "om": t["temp_juliaca_c"].to_numpy()})
        temp_flota.append(pd.DataFrame({"runId": run, "sensorId": sid, "hora": t["ts"].to_numpy(), "agua_estimada": w}))
    edades, resumen_res = residencia()
    rel = relacion_residencia_temperatura(edades, g, pd.concat(temp_flota))

    fecha = datetime.now(LIMA).strftime("%Y%m%d")
    destino = RAIZ / "batch" / "modelos" / "d8" / f"{CFG['version']}-{fecha}"
    destino.mkdir(parents=True, exist_ok=True)
    modelo_json = {"version": CFG["version"], "principal": principal,
                   "modelos": {n: m.a_dict() for n, m in modelos.items()},
                   "relleno_aire_canal": g.attrs["relleno"], "juliaca": CFG["juliaca"],
                   "umbrales_configurados": CFG["umbrales"]["configurados"], "horizonte_cruce_h": HMAX,
                   "residuos_ajuste_por_h": {str(h): np.round(r, 3).tolist() for h, r in res_aj.items()},
                   "parametros_bootstrap": [m.a_dict() for m in boot]}
    (destino / "modelo.json").write_text(json.dumps(modelo_json, ensure_ascii=False, indent=1), encoding="utf-8")
    manifiesto = {"version": CFG["version"], "generado": datetime.now(LIMA).isoformat(timespec="seconds"),
                  "particion": Particion().version(), "canal": CFG["canal_validacion"],
                  "open_meteo_canal": str(OM_CANAL.relative_to(RAIZ)).replace("\\", "/"),
                  "regla_linea_base": {"version_regla": PROV["version_regla"], "tau_h": PROV["tau_h"],
                                       "reglas": REGLAS["version"]}, "config": CFG}
    (destino / "manifiesto.json").write_text(json.dumps(manifiesto, ensure_ascii=False, indent=1, default=str),
                                             encoding="utf-8")
    c = g["conj"]
    resultados = {"version": CFG["version"], "principal": principal,
                  "parametros": {n: m.a_dict() for n, m in modelos.items()},
                  "datos_canal": {"horas": int(g["agua"].notna().sum()), "relleno_aire": g.attrs["relleno"],
                                  "horas_por_conjunto": {k: int(((c == k) & g["agua"].notna()).sum())
                                                         for k in ("entrenamiento", "ajuste", "embargo", "prueba")}},
                  "temperatura": {"diseno_ajuste": diseno, "prueba": prueba}, "cobertura_banda": cob,
                  "cruce": cruce, "precision_dht11": dht, "transferencia_juliaca": transf,
                  "tau_bootstrap": {"p10": float(np.percentile([m.tau for m in boot], 10)),
                                    "p50": float(np.median([m.tau for m in boot])),
                                    "p90": float(np.percentile([m.tau for m in boot], 90))},
                  "residencia": {"por_tanque": resumen_res, "relacion_temperatura": rel},
                  "modelo": str(destino.relative_to(RAIZ)).replace("\\", "/")}
    EVID.mkdir(parents=True, exist_ok=True)
    (EVID / "resultados_d8.json").write_text(json.dumps(resultados, ensure_ascii=False, indent=1, default=float),
                                             encoding="utf-8")
    shutil.copy(destino / "manifiesto.json", EVID / "manifiesto_modelos_d8.json")
    (EVID / "tablas_d8.md").write_text(tablas_md(json.loads(json.dumps(resultados, default=float))), encoding="utf-8")
    log(status="listo", s=round(time.time() - t0), modelo=str(destino))


if __name__ == "__main__":
    main()
