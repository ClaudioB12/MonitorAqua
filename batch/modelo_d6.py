"""
F6-D6: pérdida proyectada por fuga (dimensión de Joel).

  Noches     una fila por tanque y noche (franja 00:00-03:30 Lima, la de d6_noches y de alerta_fuga). La base es la
             función de la tabla oro (batch/oro.py -> d6) sobre la distancia de la capa plata, que ya respeta
             compensacion_en_firmware (no se compensa dos veces) y el sensor vigente (rango válido por periodo). Una
             noche con dos sensores o con compensación desconocida no se usa.
  Tasa       pendiente de Theil-Sen de las medianas por minuto (batch/fuga.py), en L/h con la geometría; exceso =
             tasa - referencia (mediana de las 7 noches válidas anteriores del tanque).
  Modelo     regresión logística (IRLS, L2) P(fuga esta noche) con el exceso, el de la noche anterior, la estabilidad
             entre noches, el enfriamiento del DHT11 y de Open-Meteo, las horas desde el último llenado y el ruido;
             pérdida a 24 h y 7 días = P · max(exceso, 0) · H, banda P10-P90 conforme (conjunto de ajuste).
  Bases      (a) sin fuga (0 L); (b) el exceso de la noche anterior; (c) la regla alerta_fuga congelada f4.4.
  Niveles    flota f2b.2 y f2b.3 (consistencia con fugas etiquetadas: 12 semillas para entrenar y 12 DISTINTAS para
             evaluar, las mismas en los dos generadores), ThingSpeak real
             (ingrediente observable: el descenso de la noche siguiente), ESP32 (pendiente de la prueba real).

  .venv/Scripts/python batch/modelo_d6.py            (~25 min la primera vez; las corridas quedan en caché)
"""
import json
import math
import shutil
import sys
import time
from argparse import Namespace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

RAIZ = Path(__file__).resolve().parents[1]
for sub in ("batch", "streaming", "ingesta"):
    sys.path.insert(0, str(RAIZ / sub))
import evaluacion as ev  # noqa: E402
import generador  # noqa: E402
import oro  # noqa: E402
from detector_servicio import Configuracion, Detector  # noqa: E402
from evaluar_detector import wilson  # noqa: E402
from fuga import ModeloFuga, elegir_umbral, theil_sen  # noqa: E402
from particion import Particion  # noqa: E402

CFG = yaml.safe_load((RAIZ / "batch" / "modelo_d6.yml").read_text(encoding="utf-8"))
TANQUES = yaml.safe_load((RAIZ / "ingesta" / "tanques.yml").read_text(encoding="utf-8"))["tanques"]
REGLAS = RAIZ / "streaming" / "reglas_servicio.yml"   # f4.4 congelada: línea base (c)
CACHE = RAIZ / "batch" / "datos" / "d6_cache"
EVID = RAIZ / "docs" / "evidencias" / "f6-d6"
H, MIN = 3_600_000, 60_000
LIMA = timezone(timedelta(hours=-5))
INI_H, FIN_H = CFG["franja_lima_h"]
FIRMWARE = {"desde": "2000-01-01T00:00:00-05:00"}
RESOLUCION_CM = yaml.safe_load((RAIZ / "ingesta" / "generador.yml").read_text(encoding="utf-8"))["medicion"]["resolucion_cm"]


def log(**c):
    print(json.dumps({"service": "f6-d6", **c}, ensure_ascii=False, default=str), flush=True)


def l_por_cm(geom):
    cap, alto = geom.get("capacidad_l"), geom.get("altura_interior_cm")
    return cap / alto if cap and alto else None


def inicio_franja(noche):
    d = datetime.fromisoformat(str(noche)).replace(tzinfo=LIMA)
    return int(d.timestamp() * 1000 + INI_H * H)


def juliaca():
    """Enfriamiento de Open-Meteo Juliaca en la franja: T(00 h) - T(03 h) de cada fecha (Lima)."""
    om = pd.read_parquet(RAIZ / "batch" / "datos" / "plata", filters=[("fuente", "=", "open-meteo"),
                                                                      ("sensorId", "=", "open-meteo-juliaca")],
                         columns=["timestamp", "temperaturaC"])
    om["t"] = pd.to_datetime(om["timestamp"], unit="ms", utc=True).dt.tz_convert("America/Lima")
    om = om.groupby(om["t"].dt.floor("h"))["temperaturaC"].mean()
    out = {}
    for t, v in om.items():
        if t.hour == 0 and (t + pd.Timedelta(hours=3)) in om.index:
            out[t.date().isoformat()] = float(v - om[t + pd.Timedelta(hours=3)])
    return out


# --- 1. Noches ---------------------------------------------------------------------------------------------------

def horas_desde_llenado(df, inicios):
    """Horas entre el último llenado observable (mediana de 15 min > subida_pp sobre el mínimo de los 30 min previos)
    y el inicio de cada franja (sin llenado visto: 72 h, tope), y si hubo llenado dentro de la franja. No se usa
    hubo_subida de d6_noches (una subida > 2 % entre dos lecturas seguidas): los saltos del sensor la disparan en
    casi todas las noches de la flota."""
    s = df.groupby(df["timestamp"] // MIN)["nivelPct"].median()
    s.index = pd.to_datetime(s.index * MIN, unit="ms")
    m15 = s.rolling("15min").median()
    sube = (m15 - m15.rolling("30min").min()) > CFG["noche_valida"]["subida_llenado_pp"]
    t_sube = sube[sube].index.as_unit("ms").asi8  # epoch ms (pandas 3 puede crear el índice en ms o en ns)
    horas, en_franja = [], []
    for ini in inicios:
        previos = t_sube[t_sube < ini]
        horas.append(min(72.0, (ini - previos[-1]) / H) if len(previos) else 72.0)
        en_franja.append(bool(((t_sube >= ini) & (t_sube < ini + (FIN_H - INI_H) * H)).any()))
    return horas, en_franja


def noches_tanque(df, geom, fuente, sid, run, fuga=None, tiempos=None, paso_h=None, dT_jul=None):
    """df: lecturas válidas del tanque (timestamp, distanciaCm, distanciaCompensadaCm, temperaturaC, nivelPct y,
    opcionales, tipoAnomalia, episodioId, compensacionOrigen, sensorModelo). fuga/tiempos: verdad del generador."""
    base = oro.d6(df, fuente, sid, run, INI_H, FIN_H)
    if base.empty:
        return base
    lpc = l_por_cm(geom)
    inicios = [inicio_franja(n) for n in base["noche"]]
    base["ini_ts"] = inicios
    base["horas_llenado"], base["llenado_en_franja"] = horas_desde_llenado(df, inicios)
    filas = []
    for ini in inicios:
        fin = ini + int((FIN_H - INI_H) * H)
        w = df[(df["timestamp"] >= ini) & (df["timestamp"] < fin)]
        g = w.groupby(w["timestamp"] // MIN)["distanciaCompensadaCm"].median()
        pend, mad = theil_sen((g.index.to_numpy() * MIN - ini) / H, g.to_numpy())
        f = {"tasa_cm_h": pend, "ruido_cm": mad, "tasa_lph": pend * lpc if pend is not None and lpc else np.nan,
             "l_por_cm": lpc, "sensores": int(w["sensorModelo"].nunique()) if "sensorModelo" in w else 1,
             "sensor_modelo": w["sensorModelo"].mode().iloc[0] if "sensorModelo" in w and w["sensorModelo"].notna().any() else None,
             "compensacion_desconocida": bool((w.get("compensacionOrigen", pd.Series(dtype=str)) == "desconocida").any())}
        if "episodioId" in w:
            ep = w.loc[w["tipoAnomalia"] == "fuga_lenta", "episodioId"].dropna()
            f["episodio_fuga"] = ep.mode().iloc[0] if len(ep) else None
        if fuga is not None:
            en = (tiempos >= ini) & (tiempos < fin)
            f["fuga_real_lph"] = float(fuga[en].mean()) if en.any() else np.nan
            for h in CFG["horizontes_h"]:
                hasta = fin + h * H
                f[f"perdida_{h}h_real_l"] = (float(fuga[(tiempos >= fin) & (tiempos < hasta)].sum() * paso_h)
                                             if hasta <= tiempos[-1] else np.nan)
        filas.append(f)
    out = pd.concat([base.reset_index(drop=True), pd.DataFrame(filas)], axis=1)
    out["fuente"], out["sensorId"], out["runId"] = fuente, sid, run
    out["dT_dht_c"] = out["temp_inicio_c"] - out["temp_fin_c"]
    out["dT_juliaca_c"] = out["noche"].map(dT_jul or {})
    return secuencia(out)


def secuencia(n):
    """Validez de cada noche, referencia (mediana de las noches válidas anteriores), exceso y estabilidad."""
    nv = CFG["noche_valida"]
    n = n.sort_values("ts").reset_index(drop=True)
    n["valida"] = ((n["cobertura"] >= nv["cobertura_min"]) & ~n["llenado_en_franja"]
                   & (n["horas_llenado"] >= nv["sin_llenado_previo_h"]) & n["tasa_lph"].notna()
                   & (n["sensores"] <= 1) & ~n["compensacion_desconocida"])
    ref, est, prev = [], [], []
    hist, ult_exceso, ult_ts = [], np.nan, None
    k, kmin, ke = CFG["referencia"]["noches"], CFG["referencia"]["minimo"], CFG["estabilidad_noches"]
    for r in n.itertuples():
        ref.append(float(np.median(hist[-k:])) if len(hist) >= kmin else np.nan)
        est.append(float(np.std(hist[-ke:])) if len(hist) >= kmin else np.nan)
        prev.append(ult_exceso if ult_ts is not None and r.ts - ult_ts <= 2 * 24 * H else np.nan)
        if r.valida:
            if not np.isnan(ref[-1]):
                ult_exceso, ult_ts = r.tasa_lph - ref[-1], r.ts
            hist.append(r.tasa_lph)
    n["referencia_lph"], n["estabilidad_lph"], n["exceso_prev_lph"] = ref, est, prev
    n["exceso_lph"] = n["tasa_lph"] - n["referencia_lph"]
    n["log_horas_llenado"] = np.log1p(n["horas_llenado"])
    n["evaluable"] = n["valida"] & n["referencia_lph"].notna()
    return n


# --- 2. Flota (f2b.2 y f2b.3) -----------------------------------------------------------------------------------

def corrida(modelo, semilla, det, dT_jul):
    args = Namespace(config=str(RAIZ / CFG["flota"][modelo]["config"]), tanques=None, dias=None, intervalo_s=None,
                     semilla=semilla, anomalias=None, factor_ruido=None, caudal_llenado=None, franja_atipica=None)
    cfg, perfil, run_id = generador.cargar_config(args)
    cache = CACHE / f"{run_id}.parquet"
    if cache.exists():
        return pd.read_parquet(cache), run_id
    t0 = time.time()
    tiempos, filas, episodios = generador.generar(cfg, perfil, run_id)
    firmware = bool(cfg["medicion"].get("compensacion_en_firmware"))
    fuente = f"aumentado_{modelo}"
    part = Particion(inicio_corridas={run_id: int(tiempos[0])})
    paso_h = cfg["corrida"]["intervalo_s"] / 3600
    tanques_archivo = det.cfg.refrescar().tanques
    salida = []
    for f in filas:
        sid = cfg["tanques"][f["t"]]["sensorId"]
        geom = {**TANQUES[sid], **({"compensacion_en_firmware": FIRMWARE} if firmware else {})}
        lect = [{"timestamp": int(ts), "distanciaCm": float(d), "temperaturaC": float(t)}
                for ts, d, t in zip(tiempos, f["dist"], f["temp"])]
        niv = [det.nivel(x, geom, det.cfg.reglas) for x in lect]
        # Igual que la capa plata: una falla del DHT11 deja humedadPct = -1, fuera de rango, e invalida la lectura
        ok = np.array([v[0] for v in niv]) & (np.asarray(f["hum"]) != -1)
        df = pd.DataFrame({"timestamp": tiempos, "distanciaCm": f["dist"], "temperaturaC": f["temp"],
                           "distanciaCompensadaCm": [v[4] for v in niv], "nivelPct": [v[1] for v in niv],
                           "tipoAnomalia": [generador.TIPOS[e] if e >= 0 else None for e in f["etiqueta"]],
                           "episodioId": [episodios[p]["episodioId"] if e >= 0 and p >= 0 else None
                                          for e, p in zip(f["etiqueta"], f["episodio"])]})[ok]
        n = noches_tanque(df, geom, fuente, sid, run_id, f["fuga_lph"], tiempos, paso_h, dT_jul)
        # Línea base (c): la regla alerta_fuga congelada f4.4, sobre las mismas lecturas
        det.cfg.tanques = {**tanques_archivo, sid: geom}
        evs, estado = [], None
        try:
            for i in range(0, len(lect), 5000):
                nuevos, estado = det.procesar(sid, lect[i:i + 5000], json.loads(json.dumps(estado)) if estado else None,
                                              "aumentado", int(time.time() * 1000))
                evs += nuevos
        finally:
            det.cfg.tanques = tanques_archivo
        alertas = {datetime.fromtimestamp(e["inicioTs"] / 1000, LIMA).date().isoformat(): e
                   for e in evs if e["tipo"] == "alerta_fuga"}
        lpc = l_por_cm(geom)
        n["alerta_f44"] = n["noche"].isin(alertas)
        n["alerta_f44_exceso_lph"] = [((alertas[x]["detalle"]["tasaCmH"] - alertas[x]["detalle"]["baseCmH"]) * lpc)
                                      if x in alertas else 0.0 for x in n["noche"]]
        n["particion"] = [part.asignar(fuente, sid, run_id, int(ts)) for ts in n["ts"]]
        n["modelo_generador"], n["semilla"] = modelo, semilla
        salida.append(n)
    out = pd.concat(salida, ignore_index=True)
    out["runId"] = run_id
    CACHE.mkdir(parents=True, exist_ok=True)
    out.to_parquet(cache)
    log(status="corrida", modelo=modelo, semilla=semilla, runId=run_id, noches=len(out), s=round(time.time() - t0))
    return out, run_id


def comprobar_oro(flota):
    """La reconstrucción de f2b.2 semilla 7 debe reproducir la tabla oro d6_noches (misma corrida)."""
    o = ev.leer_oro("d6_noches")
    o = o[o["fuente"] == "aumentado_f2b2"]
    r = flota[(flota["modelo_generador"] == "f2b2") & (flota["runId"].isin(o["runId"].unique()))]
    m = o.merge(r, on=["sensorId", "noche"], suffixes=("_oro", ""))
    dif = (m["descenso_cm_compensado_oro"] - m["descenso_cm_compensado"]).abs()
    return {"runId": sorted(o["runId"].unique().tolist()), "noches_oro": int(len(o)), "noches_emparejadas": int(len(m)),
            "max_dif_descenso_cm": float(dif.max()) if len(m) else None,
            "noches_con_dif_mayor_0_05_cm": int((dif > 0.05).sum()),
            "etiquetas_iguales": bool((m["etiqueta_oro"].fillna("-").astype(str) == m["etiqueta"].fillna("-").astype(str)).all())
            if len(m) else None}


# --- 3. Modelo ---------------------------------------------------------------------------------------------------

def separar_ajuste(t):
    """Entrenamiento evaluable: el último frac_ajuste de cada tanque (por corrida) es el conjunto de AJUSTE. Solo las
    semillas de entrenamiento entran al ajuste; las de evaluación solo aportan su partición de consistencia."""
    t = t.copy()
    t["conj"] = np.where(t["uso"] == "entrenamiento", t["particion"], "evaluacion_" + t["particion"])
    ent = (t["particion"] == "entrenamiento") & (t["uso"] == "entrenamiento")
    corte = t[ent].groupby(["runId", "sensorId"])["ts"].transform(lambda s: s.quantile(1 - CFG["frac_ajuste"]))
    t.loc[corte.index, "conj"] = np.where(t.loc[corte.index, "ts"] >= corte, "ajuste", "entrenamiento")
    return t


def X(t):
    v = CFG["modelo"]["variables"]
    return t[v].astype(float).fillna(0.0).to_numpy()


def entrenar(flota):
    t = separar_ajuste(flota)
    ok = t["evaluable"]
    ent, aj = t[ok & (t["conj"] == "entrenamiento")], t[ok & (t["conj"] == "ajuste")]
    y = (ent["etiqueta"] == "fuga_lenta").astype(int).to_numpy()
    m = ModeloFuga.ajustar(X(ent), y, CFG["modelo"]["variables"], CFG["modelo"]["l2"], CFG["horizontes_h"])
    p = m.probabilidad(X(aj))
    yaj = (aj["etiqueta"] == "fuga_lenta").to_numpy()
    m.umbral, f1_ajuste = elegir_umbral(p, yaj)
    m.calibrar(p, aj["exceso_lph"], {h: aj[f"perdida_{h}h_real_l"] for h in CFG["horizontes_h"]},
               CFG["conforme"]["alfa"], CFG["conforme"]["min_n"])
    info = {"umbral_elegido": m.umbral, "f1_ajuste": f1_ajuste, "factor_persistencia": m.factor_h,
            "noches_entrenamiento": int(len(ent)), "fugas_entrenamiento": int(y.sum()),
            "noches_ajuste": int(len(aj)), "fugas_ajuste": int(yaj.sum()),
            "ajuste_clasificacion": ev.evaluar_clasificacion(yaj, p >= m.umbral, n_boot=CFG["evaluacion"]["n_boot"]),
            "coeficientes": dict(zip(["intercepto"] + m.variables, np.round(m.beta, 3).tolist()))}
    return m, t, info


# --- 4. Evaluación de la flota ----------------------------------------------------------------------------------

def predecir(m, t):
    out = m.proyectar(X(t), t["exceso_lph"].to_numpy())
    t = t.copy()
    for k, v in out.items():
        t[k] = v
    for h in CFG["horizontes_h"]:
        t[f"base_a_{h}h"] = 0.0
        t[f"base_b_{h}h"] = np.maximum(t["exceso_prev_lph"].fillna(0.0), 0.0) * h
        t[f"base_c_{h}h"] = np.where(t["alerta_f44"], np.maximum(t["alerta_f44_exceso_lph"], 0.0), 0.0) * h
    return t


def proporcion(k, n):
    return {"k": int(k), "n": int(n), "valor": round(k / n, 3) if n else None, "ic95": wilson(k, n)}


def evaluar_flota(t, fuente):
    c = t[(t["fuente"] == fuente) & (t["particion"] == "consistencia") & (t["uso"] == "evaluacion")]
    e = c[c["evaluable"]]
    fuga = (e["etiqueta"] == "fuga_lenta").to_numpy()
    toca = (e["frac_fuga_lenta"] > 0).to_numpy()          # una alerta en una noche que toca una fuga no es falsa
    nb = CFG["evaluacion"]["n_boot"]
    res = {"noches": int(len(c)), "noches_evaluables": int(len(e)), "noches_fuga": int(fuga.sum()),
           "noches_deriva": int((e["etiqueta"] == "deriva_termica").sum())}
    # Episodios de fuga con al menos una noche evaluable en consistencia; todos los del periodo para el sesgo
    eps_todos = c["episodio_fuga"].dropna().unique()
    eps = e.loc[fuga, "episodio_fuga"].dropna().unique()
    res["episodios_fuga"] = int(len(eps))
    res["episodios_fuga_en_el_periodo"] = int(len(eps_todos))
    res["evidencia"] = "suficiente" if len(eps) >= CFG["evaluacion"]["min_episodios_evidencia"] else "indicativa"
    clas = {}
    for nombre, marca in (("modelo", e["fuga"].to_numpy()), ("base_c_alerta_f44", e["alerta_f44"].to_numpy()),
                          ("base_a_sin_fuga", np.zeros(len(e), bool))):
        detect = {ep for ep in eps if marca[(e["episodio_fuga"] == ep).to_numpy() & fuga].any()}
        clas[nombre] = {
            "recall_episodios": proporcion(len(detect), len(eps)),
            "recall_noches": proporcion((marca & fuga).sum(), fuga.sum()),
            "precision_noches": proporcion((marca & toca).sum(), marca.sum()),
            "alarmas_falsas_en_deriva": proporcion((marca & (e["etiqueta"] == "deriva_termica").to_numpy()).sum(),
                                                   (e["etiqueta"] == "deriva_termica").sum()),
            "alarmas_falsas_por_100_noches_sin_fuga": round(100 * (marca & ~toca).sum() / max(1, (~toca).sum()), 2),
            "bootstrap_noches": ev.evaluar_clasificacion(fuga, marca, n_boot=nb)}
    res["deteccion"] = clas
    # Recall por tasa real de la fuga: ¿desde cuántos L/h se ve?
    bins = [(1.5, 2.5), (2.5, 4.01)]
    res["recall_por_tasa_real"] = {f"{a}-{min(b, 4)} L/h": {
        n: proporcion((m_ & fuga & (e["fuga_real_lph"] >= a).to_numpy() & (e["fuga_real_lph"] < b).to_numpy()).sum(),
                      (fuga & (e["fuga_real_lph"] >= a).to_numpy() & (e["fuga_real_lph"] < b).to_numpy()).sum())
        for n, m_ in (("modelo", e["fuga"].to_numpy()), ("base_c_alerta_f44", e["alerta_f44"].to_numpy()))}
        for a, b in bins}
    # Error de la tasa en las noches con fuga (L/h), IC por episodio
    ef = e[fuga]
    grupos = ef["episodio_fuga"].fillna("sin").to_numpy()
    tasas = {"modelo": np.maximum(ef["exceso_lph"], 0.0), "base_a_sin_fuga": np.zeros(len(ef)),
             "base_b_noche_anterior": np.maximum(ef["exceso_prev_lph"].fillna(0.0), 0.0),
             "base_c_alerta_f44": np.where(ef["alerta_f44"], np.maximum(ef["alerta_f44_exceso_lph"], 0.0), 0.0)}
    res["error_tasa_lph"] = {k: ev.evaluar_regresion(ef["fuga_real_lph"], v, grupos=grupos, n_boot=nb, mape_minimo=0.5)
                             for k, v in tasas.items()} if len(ef) else {}
    # Pérdida proyectada (L) a 24 h y 7 días, todas las noches evaluables; IC por tanque y corrida
    gr = (e["runId"] + "|" + e["sensorId"]).to_numpy()
    res["perdida"] = {}
    for h in CFG["horizontes_h"]:
        y = e[f"perdida_{h}h_real_l"]
        r = {k: ev.evaluar_regresion(y, e[col], grupos=gr, n_boot=nb, mape_minimo=1.0)
             for k, col in (("modelo", f"perdida_{h}h_l"), ("base_a_sin_fuga", f"base_a_{h}h"),
                            ("base_b_noche_anterior", f"base_b_{h}h"), ("base_c_alerta_f44", f"base_c_{h}h"))}
        if f"p10_{h}h_l" in e:
            ok = y.notna()
            dentro = (y[ok] >= e.loc[ok, f"p10_{h}h_l"]) & (y[ok] <= e.loc[ok, f"p90_{h}h_l"])
            r["banda_p10_p90"] = {"cobertura": proporcion(dentro.sum(), ok.sum()),
                                  "cobertura_noches_con_fuga": proporcion(dentro[fuga[ok.to_numpy()]].sum(),
                                                                          fuga[ok.to_numpy()].sum()),
                                  "ancho_mediano_l": float((e.loc[ok, f"p90_{h}h_l"] - e.loc[ok, f"p10_{h}h_l"]).median())}
        con = fuga & y.notna().to_numpy()
        r["solo_noches_con_fuga"] = {k: ev.evaluar_regresion(y[con], e.loc[con, col], grupos=gr[con], n_boot=nb,
                                                             mape_minimo=1.0)
                                     for k, col in (("modelo", f"perdida_{h}h_l"), ("base_a_sin_fuga", f"base_a_{h}h"),
                                                    ("base_b_noche_anterior", f"base_b_{h}h"),
                                                    ("base_c_alerta_f44", f"base_c_{h}h"))}
        res["perdida"][f"{h}h"] = r
    # Sesgo de selección: ¿las noches con fuga se descartan más que las demás?
    cf = c["etiqueta"] == "fuga_lenta"
    res["sesgo_seleccion"] = {"evaluables_noches_fuga": proporcion((cf & c["evaluable"]).sum(), cf.sum()),
                              "evaluables_otras_noches": proporcion((~cf & c["evaluable"]).sum(), (~cf).sum()),
                              "episodios_sin_noche_evaluable": int(len(set(eps_todos) - set(eps)))}
    return res


# --- 5. ThingSpeak: el descenso de la noche siguiente ----------------------------------------------------------

def noches_thingspeak():
    tcfg = CFG["thingspeak"]
    sids = tcfg["canales"] + list(tcfg["fuera_de_alcance"])
    p = pd.read_parquet(RAIZ / "batch" / "datos" / "plata", filters=[("fuente", "=", "thingspeak")],
                        columns=["timestamp", "sensorId", "nivelPct", "nivelCm", "lectura_valida"])
    p = p[p["lectura_valida"] & p["sensorId"].isin(sids)]
    lim = p.groupby("sensorId")["timestamp"].max().to_dict()
    part = Particion(limites_thingspeak=lim)
    filas = []
    for sid, g in p.groupby("sensorId"):
        geom = TANQUES.get(sid, {})
        modo = geom.get("modo")
        g = g.sort_values("timestamp")
        v = g["nivelCm"] if modo == "nivelM" else g["nivelPct"]
        unidad = "cm" if modo == "nivelM" else "pp"
        g = g.assign(v=v.to_numpy(), noche=pd.to_datetime(g["timestamp"], unit="ms", utc=True)
                     .dt.tz_convert("America/Lima").dt.date.astype(str))
        # Llenados con el mismo criterio robusto que la flota (mediana de 15 min sobre el mínimo de 30 min): con el
        # ruido de 1233810 (~1 pp) un salto de 2 pp entre dos lecturas no indica un llenado
        noches = sorted(g["noche"].unique())
        horas, en_franja = horas_desde_llenado(g, [inicio_franja(x) for x in noches])
        llen = dict(zip(noches, zip(horas, en_franja)))
        for noche, w in g.groupby("noche"):
            ini = inicio_franja(noche)
            w = w[(w["timestamp"] >= ini) & (w["timestamp"] < ini + (FIN_H - INI_H) * H) & w["v"].notna()]
            if len(w) < tcfg["min_lecturas"] or (w["timestamp"].iloc[-1] - w["timestamp"].iloc[0]) / H < tcfg["min_horas"]:
                continue
            pend, mad = theil_sen((w["timestamp"].to_numpy() - ini) / H, w["v"].to_numpy())
            filas.append({"sensorId": sid, "noche": noche, "ts": ini, "unidad": unidad,
                          "capacidad_l": geom.get("capacidad_l"),
                          "subida": bool(llen[noche][1] or llen[noche][0] < CFG["noche_valida"]["sin_llenado_previo_h"]),
                          "descenso": -pend * (FIN_H - INI_H),
                          "ruido": mad, "particion": part.asignar("thingspeak", sid, None, ini),
                          "fuera_de_alcance": sid in tcfg["fuera_de_alcance"]})
    return pd.DataFrame(filas)


def pronostico_thingspeak(n):
    """Para cada noche válida: mediana de las k noches válidas anteriores (14 días), la anterior y cero."""
    n = n[~n["subida"]].sort_values(["sensorId", "ts"]).copy()
    for k in CFG["thingspeak"]["k_opciones"]:
        n[f"pred_k{k}"] = np.nan
    n["base_b_noche_anterior"] = np.nan
    for sid, g in n.groupby("sensorId"):
        for i, (idx, r) in enumerate(g.iterrows()):
            prev = g.iloc[:i]
            prev = prev[prev["ts"] >= r["ts"] - 14 * 24 * H]
            if len(prev):
                n.loc[idx, "base_b_noche_anterior"] = prev["descenso"].iloc[-1]
                for k in CFG["thingspeak"]["k_opciones"]:
                    n.loc[idx, f"pred_k{k}"] = float(prev["descenso"].iloc[-k:].median())
    n["base_a_sin_fuga"] = 0.0
    return n[n["base_b_noche_anterior"].notna()]


def evaluar_thingspeak(n):
    nb = CFG["evaluacion"]["n_boot"]
    alc = n[~n["fuera_de_alcance"]]
    ent = alc[alc["particion"] == "entrenamiento"]
    corte = ent.groupby("sensorId")["ts"].transform(lambda s: s.quantile(1 - CFG["frac_ajuste"]))
    dis = ent[ent["ts"] >= corte]
    maes = {k: float(np.mean(np.abs(dis["descenso"] - dis[f"pred_k{k}"]))) for k in CFG["thingspeak"]["k_opciones"]}
    k = min(maes, key=maes.get)

    def bloque(d):
        if d.empty:
            return {"n": 0}
        out = {"n": int(len(d)), "tanques": sorted(d["sensorId"].unique().tolist()), "unidad": d["unidad"].iloc[0]}
        for nombre, col in (("modelo_mediana_k", f"pred_k{k}"), ("base_a_sin_fuga", "base_a_sin_fuga"),
                            ("base_b_noche_anterior", "base_b_noche_anterior")):
            out[nombre] = ev.evaluar_regresion(d["descenso"], d[col], estratos=d["sensorId"].to_numpy(), largo_bloque=3,
                                               n_boot=nb, mape_minimo=0.5)
            lit = d["capacidad_l"].notna()
            if lit.any():
                y = d.loc[lit, "descenso"] / 100 * d.loc[lit, "capacidad_l"]
                pr = d.loc[lit, col] / 100 * d.loc[lit, "capacidad_l"]
                out[nombre + "_litros"] = ev.evaluar_regresion(y, pr, estratos=d.loc[lit, "sensorId"].to_numpy(),
                                                               largo_bloque=3, n_boot=nb, mape_minimo=5)
        out["por_tanque"] = {s: {"n": int(len(x)), "descenso_mediano": float(x["descenso"].median()),
                                 "MAE_modelo": float(np.mean(np.abs(x["descenso"] - x[f"pred_k{k}"]))),
                                 "MAE_base_b": float(np.mean(np.abs(x["descenso"] - x["base_b_noche_anterior"]))),
                                 "MAE_base_a": float(np.mean(np.abs(x["descenso"])))} for s, x in d.groupby("sensorId")}
        return out

    fuera = n[n["fuera_de_alcance"]]
    return {"k_elegido": k, "mae_diseno_por_k": maes,
            "diseno": bloque(dis), "prueba": bloque(alc[alc["particion"] == "prueba"]),
            "fuera_de_alcance_prueba": bloque(fuera[fuera["particion"] == "prueba"])}


# --- 6. ESP32 (diseño) y sensibilidad mínima detectable -----------------------------------------------------------

def plata_esp32():
    p = pd.read_parquet(RAIZ / "batch" / "datos" / "plata", filters=[("fuente", "=", "esp32")])
    p = p[p["lectura_valida"] & p["distanciaCompensadaCm"].notna()].sort_values("timestamp")
    return p.drop_duplicates("timestamp")


def noches_esp32(p, dT_jul):
    sid = "esp32-tanque-real-01"
    cols = ["timestamp", "distanciaCm", "distanciaCompensadaCm", "temperaturaC", "nivelPct", "compensacionOrigen",
            "sensorModelo"]
    n = noches_tanque(p[cols], TANQUES[sid], "esp32", sid, None, dT_jul=dT_jul)
    return n


def mdl_ruido(serie_min, bloque, n_sim, factor, rng):
    """Ruido de la tasa: residuos de las medianas por minuto, remuestreados en bloques para simular franjas de 3.5 h
    sin cambio real; la mínima detectable es factor × la desviación de la pendiente de Theil-Sen."""
    t = np.arange(len(serie_min)) / 60
    pend, _ = theil_sen(t, serie_min)
    res = serie_min - np.median(serie_min - pend * t) - pend * t
    largo = int((FIN_H - INI_H) * 60)
    pendientes = []
    for _ in range(n_sim):
        idx = np.concatenate([np.arange(s, s + bloque) % len(res) for s in rng.integers(0, len(res), largo // bloque + 1)])[:largo]
        pendientes.append(theil_sen(np.arange(largo) / 60, res[idx])[0])
    sd = float(np.std(pendientes))
    # Piso: el sensor informa con 1 decimal. Un cambio menor que la resolución en toda la franja no se ve aunque el
    # ruido quede por debajo (un HC-SR04 quieto repite 17.0 cm y la desviación de la pendiente sale 0)
    piso = RESOLUCION_CM / (FIN_H - INI_H)
    return {"minutos": int(len(serie_min)), "sd_residuo_cm": float(np.std(res)), "sd_tasa_cm_h": sd,
            "minima_por_ruido_cm_h": factor * sd, "piso_resolucion_cm_h": piso, "minima_cm_h": max(factor * sd, piso)}


def sensibilidad(p, flota_ent):
    s = CFG["sensibilidad"]
    rng = np.random.default_rng(2026)
    lpc = l_por_cm(TANQUES["esp32-tanque-real-01"])
    out = {"factor": s["factor"], "l_por_cm_esp32_provisional": lpc}
    a, b = (int(datetime.fromisoformat(x).timestamp() * 1000) for x in s["tramo_hc_sr04"])
    hc = p[(p["timestamp"] >= a) & (p["timestamp"] < b)]
    serie = hc.groupby(hc["timestamp"] // MIN)["distanciaCompensadaCm"].median().to_numpy()
    r = mdl_ruido(serie, s["bloque_min"], s["n_sim"], s["factor"], rng)
    out["hc_sr04"] = {**r, "tramo": s["tramo_hc_sr04"], "distancia_cm": float(np.median(serie)),
                      "minima_lph": r["minima_cm_h"] * lpc}
    jsn = p[p["sensorModelo"].astype(str).str.startswith("JSN")]
    if len(jsn):
        g = jsn.groupby(jsn["timestamp"] // MIN)["distanciaCompensadaCm"].median()
        tramos = (np.diff(g.index.to_numpy(), prepend=g.index[0]) > 2).cumsum()
        mejor = pd.Series(tramos).value_counts().idxmax()
        serie = g.to_numpy()[tramos == mejor]
        if len(serie) >= 30:
            r = mdl_ruido(serie, s["bloque_min"], s["n_sim"], s["factor"], rng)
            out["jsn_sr04t"] = {**r, "distancia_cm": float(np.median(serie)), "minima_lph": r["minima_cm_h"] * lpc,
                                "nota": "provisional: pocas lecturas válidas (sonda aún mal montada, < 25 cm la mayor parte)"}
        else:
            out["jsn_sr04t"] = {"minutos": int(len(serie)), "nota": "sin un tramo de 30 min válido: pendiente"}
    k = 0.606 / 331.3  # cambio relativo de c por °C (≈ 0.183 %/°C)
    horas = FIN_H - INI_H
    out["dht11"] = {f"{d}_cm": {"por_c_cm_h": k * d / horas,
                                "minima_cm_h": s["factor"] * k * d * s["dht_cambio_c"] / horas,
                                "minima_lph": s["factor"] * k * d * s["dht_cambio_c"] / horas * lpc,
                                "peor_caso_lph": s["factor"] * k * d * s["dht_max_c"] / horas * lpc}
                    for d in s["distancias_cm"]}
    # Variabilidad del consumo nocturno (flota): el exceso de las noches SIN fuga
    sin = flota_ent[flota_ent["evaluable"] & flota_ent["etiqueta"].isna()]
    out["consumo_nocturno_flota"] = {f: {"noches": int(len(x)), "p90_exceso_lph": float(x["exceso_lph"].quantile(0.9)),
                                         "p95_exceso_lph": float(x["exceso_lph"].quantile(0.95)),
                                         "sd_exceso_lph": float(x["exceso_lph"].std())}
                                     for f, x in sin.groupby("fuente")}
    return out


# --- 7. Tablas -----------------------------------------------------------------------------------------------

def _p(x):
    return "—" if x["valor"] is None else f"{x['valor']:.2f} [{x['ic95'][0]:.2f}-{x['ic95'][1]:.2f}] ({x['k']}/{x['n']})"


def _r(d, dec=2):
    if not d.get("n"):
        return "—"
    ic = d["MAE"]["ic95"]
    ic = f" [{ic[0]:.{dec}f}, {ic[1]:.{dec}f}]" if ic and None not in ic else " (sin IC)"
    return f"{d['MAE']['valor']:.{dec}f}{ic} · sesgo {d['SESGO']['valor']:+.{dec}f}"


def tablas_md(r):
    w = ["# Tablas D6 (pérdida proyectada por fuga)", "",
         f"Modelo `{r['version']}`. Umbral de P(fuga) elegido en el ajuste: {r['entrenamiento']['umbral_elegido']} "
         f"(F1 de ajuste {r['entrenamiento']['f1_ajuste']:.2f}). Factor de persistencia: "
         + ", ".join(f"{h} h = {v:.2f}" for h, v in r["entrenamiento"]["factor_persistencia"].items()) + ".", "",
         "Control: la reconstrucción de f2b.2 semilla 7 reproduce la tabla oro d6_noches "
         f"({r['control_oro']['noches_emparejadas']}/{r['control_oro']['noches_oro']} noches, diferencia máxima "
         f"{r['control_oro']['max_dif_descenso_cm']} cm, etiquetas iguales: {r['control_oro']['etiquetas_iguales']}).", ""]
    for f, x in r["flota"].items():
        w += [f"## Flota {f} (consistencia, {x['episodios_fuga']} episodios de fuga, evidencia {x['evidencia']})", "",
              f"Noches evaluables {x['noches_evaluables']} de {x['noches']}; con fuga {x['noches_fuga']}; deriva térmica "
              f"{x['noches_deriva']}.", "",
              "| Detector | Recall por episodio | Recall por noche | Precisión por noche | Alarmas en deriva | Falsas / 100 noches sin fuga |",
              "|---|---|---|---|---|---|"]
        for n, d in x["deteccion"].items():
            w.append(f"| {n} | {_p(d['recall_episodios'])} | {_p(d['recall_noches'])} | {_p(d['precision_noches'])} | "
                     f"{_p(d['alarmas_falsas_en_deriva'])} | {d['alarmas_falsas_por_100_noches_sin_fuga']} |")
        w += ["", "| Tasa real de la fuga | Recall por noche: modelo | Recall por noche: alerta_fuga f4.4 |", "|---|---|---|"]
        for b_, d in x["recall_por_tasa_real"].items():
            w.append(f"| {b_} | {_p(d['modelo'])} | {_p(d['base_c_alerta_f44'])} |")
        w += ["", "Error de la tasa en las noches con fuga (L/h, IC por episodio):", "",
              "| Predictor | MAE [IC 95 %] · sesgo |", "|---|---|"]
        for n, d in x["error_tasa_lph"].items():
            w.append(f"| {n} | {_r(d)} |")
        for h, d in x["perdida"].items():
            w += ["", f"Pérdida proyectada a {h} (L, IC por tanque y corrida):", "",
                  "| Predictor | Todas las noches | Solo noches con fuga |", "|---|---|---|"]
            for n in ("modelo", "base_a_sin_fuga", "base_b_noche_anterior", "base_c_alerta_f44"):
                w.append(f"| {n} | {_r(d[n], 1)} | {_r(d['solo_noches_con_fuga'][n], 1)} |")
            if "banda_p10_p90" in d:
                bd = d["banda_p10_p90"]
                w.append(f"\nBanda P10-P90: cobertura {_p(bd['cobertura'])}; en noches con fuga "
                         f"{_p(bd['cobertura_noches_con_fuga'])}; ancho mediano {bd['ancho_mediano_l']:.1f} L.")
        ss = x["sesgo_seleccion"]
        w += ["", f"Sesgo de selección: evaluables {_p(ss['evaluables_noches_fuga'])} de las noches con fuga y "
                  f"{_p(ss['evaluables_otras_noches'])} de las demás; episodios sin noche evaluable: "
                  f"{ss['episodios_sin_noche_evaluable']}.", ""]
    t = r["thingspeak"]
    w += ["## ThingSpeak real: descenso de la noche siguiente", "",
          f"Noches anteriores para la mediana (elegido en el diseño): k = {t['k_elegido']}.", "",
          "| Conjunto | Noches | Tanques | Unidad | Mediana de k noches | Sin fuga (0) | Noche anterior |", "|---|---|---|---|---|---|---|"]
    for c in ("diseno", "prueba", "fuera_de_alcance_prueba"):
        b_ = t[c]
        if not b_.get("n"):
            continue
        w.append(f"| {c} | {b_['n']} | {', '.join(b_['tanques'])} | {b_['unidad']} | {_r(b_['modelo_mediana_k'], 3)} | "
                 f"{_r(b_['base_a_sin_fuga'], 3)} | {_r(b_['base_b_noche_anterior'], 3)} |")
        if "modelo_mediana_k_litros" in b_:
            w.append(f"| {c} (litros) | {b_['modelo_mediana_k_litros']['n']} | con capacidad | L | "
                     f"{_r(b_['modelo_mediana_k_litros'], 1)} | {_r(b_['base_a_sin_fuga_litros'], 1)} | "
                     f"{_r(b_['base_b_noche_anterior_litros'], 1)} |")
    s_ = r["sensibilidad_minima"]
    w += ["", "## Sensibilidad mínima detectable", "", "| Fuente de incertidumbre | Mínima detectable |", "|---|---|"]
    for k in ("hc_sr04", "jsn_sr04t"):
        if k in s_ and "minima_lph" in s_[k]:
            w.append(f"| ruido {k} a {s_[k]['distancia_cm']:.0f} cm ({s_[k]['minutos']} min) | "
                     f"{s_[k]['minima_cm_h']:.3f} cm/h = {s_[k]['minima_lph']:.2f} L/h |")
        elif k in s_:
            w.append(f"| ruido {k} | {s_[k].get('nota')} |")
    for d, v in s_["dht11"].items():
        w.append(f"| DHT11 a {d} (cambio de 1 °C no visto; peor caso 2 °C) | {v['minima_lph']:.2f} L/h (peor caso "
                 f"{v['peor_caso_lph']:.2f}) |")
    for f, v in s_["consumo_nocturno_flota"].items():
        w.append(f"| consumo nocturno {f} (P95 del exceso sin fuga) | {v['p95_exceso_lph']:.2f} L/h |")
    return "\n".join(w) + "\n"


# --- 8. Principal ----------------------------------------------------------------------------------------------

def main():
    t0 = time.time()
    dT_jul = juliaca()
    det = Detector(Configuracion(str(REGLAS), str(RAIZ / "ingesta" / "tanques.yml")))
    version = det.cfg.reglas["version"]
    assert version == "f4.4", f"la línea base (c) debe ser f4.4, no {version}"
    corridas, runs = [], {}
    for modelo in CFG["flota"]:
        for uso in ("entrenamiento", "evaluacion"):
            for s in CFG[f"semillas_{uso}"]:
                n, run = corrida(modelo, s, det, dT_jul)
                corridas.append(n.assign(uso=uso))
                runs.setdefault(modelo, {}).setdefault(uso, []).append(run)
    flota = pd.concat(corridas, ignore_index=True)
    control = comprobar_oro(flota[flota["uso"] == "entrenamiento"])
    solapes = set(CFG["semillas_entrenamiento"]) & set(CFG["semillas_evaluacion"])
    assert not solapes, f"semillas de entrenamiento y evaluación se solapan: {solapes}"
    log(status="flota", noches=len(flota), control_oro=control)

    m, flota, info = entrenar(flota)
    flota = predecir(m, flota)
    res_flota = {f: evaluar_flota(flota, f) for f in ("aumentado_f2b2", "aumentado_f2b3")}

    ts = pronostico_thingspeak(noches_thingspeak())
    res_ts = evaluar_thingspeak(ts)

    p = plata_esp32()
    n_esp = noches_esp32(p, dT_jul)
    esp = {"estado": "pendiente: sin periodo de prueba real (geometria_desde = null)",
           "noches_diseno": n_esp[["noche", "cobertura", "sensor_modelo", "tasa_cm_h", "ruido_cm", "tasa_lph",
                                   "descenso_cm_compensado", "dT_dht_c", "llenado_en_franja", "horas_llenado",
                                   "sensores", "valida", "evaluable"]].to_dict("records")
           if len(n_esp) else []}
    sens = sensibilidad(p, flota[flota["conj"] == "entrenamiento"])
    # Noches con las que se entrenó, ajustó y evaluó: las lee batch/modelo_d6_mllib.py (mismo modelo con MLlib)
    cols = ["fuente", "runId", "sensorId", "noche", "ts", "uso", "conj", "particion", "etiqueta", "episodio_fuga",
            "frac_fuga_lenta", "evaluable", "exceso_lph", "p_fuga", "fuga"] + CFG["modelo"]["variables"]
    flota[list(dict.fromkeys(cols))].to_parquet(RAIZ / "batch" / "datos" / "d6_noches_modelo.parquet")

    # Modelo para F7
    fecha = datetime.now(LIMA).strftime("%Y%m%d")
    destino = RAIZ / "batch" / "modelos" / "d6" / f"{CFG['version']}-{fecha}"
    destino.mkdir(parents=True, exist_ok=True)
    modelo_json = {"version": CFG["version"], "franja_lima_h": CFG["franja_lima_h"], "noche_valida": CFG["noche_valida"],
                   "referencia": CFG["referencia"], "estabilidad_noches": CFG["estabilidad_noches"],
                   "clasificador": m.a_dict(), "sensibilidad_minima": sens}
    (destino / "modelo.json").write_text(json.dumps(modelo_json, ensure_ascii=False, indent=1, default=float),
                                         encoding="utf-8")
    manifiesto = {"version": CFG["version"], "generado": datetime.now(LIMA).isoformat(timespec="seconds"),
                  "particion": Particion().version(), "reglas_linea_base": version, "corridas": runs,
                  "config": CFG, "entrenamiento": {k: v for k, v in info.items() if k != "ajuste_clasificacion"}}
    (destino / "manifiesto.json").write_text(json.dumps(manifiesto, ensure_ascii=False, indent=1, default=str),
                                             encoding="utf-8")

    resultados = {"version": CFG["version"], "control_oro": control, "entrenamiento": info, "flota": res_flota,
                  "thingspeak": res_ts, "esp32": esp, "sensibilidad_minima": sens,
                  "modelo": str(destino.relative_to(RAIZ)).replace("\\", "/")}
    EVID.mkdir(parents=True, exist_ok=True)
    (EVID / "resultados_d6.json").write_text(json.dumps(resultados, ensure_ascii=False, indent=1, default=float),
                                             encoding="utf-8")
    shutil.copy(destino / "manifiesto.json", EVID / "manifiesto_modelos_d6.json")
    (EVID / "tablas_d6.md").write_text(tablas_md(json.loads(json.dumps(resultados, default=float))), encoding="utf-8")
    log(status="listo", s=round(time.time() - t0), modelo=str(destino))


if __name__ == "__main__":
    main()
