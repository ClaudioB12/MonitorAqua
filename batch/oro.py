"""
Tablas oro por dimensión (F6). Python puro (pandas): Spark las llama por sensor con applyInPandas desde
batch/capas.py. Entrada: las lecturas plata de UN sensor (una corrida si es aumentado). Nada se interpola entre
tramos: la serie se corta en tramos continuos y cada variable derivada se calcula dentro de su tramo.

  d2_autonomia  paso 5 min      nivel (% y litros), variación en 1 h, tendencia en 3 h, subida en curso, llegada detectada
  d4_llegadas   un evento/fila  llegada_agua y corte_prolongado del detector congelado de F4 (f4.3), intervalo entre llegadas
  d6_noches     una noche/fila  descenso nocturno (00:00-03:30 Lima) crudo y compensado, tasa, cobertura, llenado
  d8_termica    paso 1 h        aire de la cámara, agua real (ThingSpeak 3418755), Juliaca (Open-Meteo), agua estimada
                                provisional (primer orden, tau de F4) y horas de residencia
"""
import math
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

LIMA = timezone(timedelta(hours=-5))
MIN = 60_000
H = 3_600_000
PASO_D2 = 5 * MIN
GAP_SESION = 30 * MIN  # hueco que separa sesiones de conexión (para marcar artefactos de reanudación en d4)
GAP_MS = {"esp32": 2 * MIN, "aumentado": 3 * MIN}  # thingspeak: 4 × su intervalo mediano


def umbral_gap(fuente, ts):
    if fuente.startswith("aumentado"):
        return GAP_MS["aumentado"]
    if fuente == "esp32":
        return GAP_MS["esp32"]
    dif = np.diff(ts)
    return int(4 * np.median(dif)) if len(dif) else 30 * MIN


def tramos(ts, gap):
    """Id de tramo continuo: cambia cuando dos lecturas seguidas están separadas más que gap."""
    if len(ts) == 0:
        return np.array([], dtype=int)
    return np.concatenate([[0], np.cumsum(np.diff(ts) > gap)])


def hora_lima(ts_ms):
    d = datetime.fromtimestamp(ts_ms / 1000, LIMA)
    return d.hour + d.minute / 60, d.weekday(), d.date().isoformat()


def replay(detector, sensor_id, lecturas, origen):
    """Eventos del detector congelado (misma clase que en vivo) sobre lecturas plata ordenadas."""
    import time
    eventos, estado = [], None
    for i in range(0, len(lecturas), 5000):
        evs, estado = detector.procesar(sensor_id, lecturas[i:i + 5000], estado, origen, int(time.time() * 1000))
        eventos += evs
    return eventos


def _modo(serie):
    s = serie.dropna()
    return s.mode().iloc[0] if len(s) else None


def d2(pdf, fuente, sensor_id, run_id, eventos):
    v = pdf[pdf["nivelPct"].notna()].copy()
    if v.empty:
        return pd.DataFrame()
    v["bin"] = v["timestamp"] // PASO_D2 * PASO_D2
    g = v.groupby(["tramo", "bin"]).agg(nivel_pct=("nivelPct", "median"), litros=("litros", "median"),
                                        n_lecturas=("nivelPct", "size"),
                                        tipo_anomalia=("tipoAnomalia", _modo)).reset_index()
    g = g.sort_values("bin")
    llegadas = [e["inicioTs"] for e in eventos if e["tipo"] == "llegada_agua"]
    out = []
    for _, t in g.groupby("tramo"):
        t = t.copy()
        idx = (t["bin"] - t["bin"].iloc[0]) // PASO_D2
        serie = pd.Series(t["nivel_pct"].values, index=idx.values)
        hace1h = serie.reindex(idx.values - 12)
        t["delta_pct_1h"] = t["nivel_pct"].values - hace1h.values
        hace5 = serie.reindex(idx.values - 1)
        t["delta_pct_5min"] = t["nivel_pct"].values - hace5.values
        pend = []
        b, y = idx.values, t["nivel_pct"].values
        for j in range(len(t)):
            m = (b >= b[j] - 35) & (b <= b[j])
            if m.sum() >= 6:
                x = b[m] * 5 / 60  # horas
                pend.append(float(np.polyfit(x, y[m], 1)[0]))
            else:
                pend.append(np.nan)
        t["tendencia_pct_h_3h"] = pend
        out.append(t)
    r = pd.concat(out)
    r["en_subida"] = r["delta_pct_5min"] > 0.5
    r["llegada_detectada"] = r["bin"].apply(lambda x: any(x <= l < x + PASO_D2 for l in llegadas))
    hl = r["bin"].apply(hora_lima)
    r["hora_lima"] = [h[0] for h in hl]
    r["dia_semana"] = [h[1] for h in hl]
    r["es_anomalia"] = r["tipo_anomalia"].notna()
    return r.rename(columns={"bin": "ts"})


def d4(pdf, fuente, sensor_id, run_id, eventos, bitacora=(), todas_ts=None):
    """pdf: lecturas válidas. todas_ts: timestamps de TODAS las lecturas (válidas o no) = sesiones de conexión. bitacora: [(ts_ms, accion)] de este sensor (docs/evidencias/f4).
    artefacto_reanudacion: el evento cae en los primeros 15 min de una sesión que sigue a un hueco de más de 30 min;
    el detector congelado ve la reconexión como "llegada" o "corte", no es servicio real (no se tocan las reglas)."""
    ts = np.sort(np.asarray(todas_ts if todas_ts is not None else pdf["timestamp"].values))
    sesiones = pd.DataFrame({"timestamp": ts, "sesion": tramos(ts, GAP_SESION)})
    inicios = sesiones.groupby("sesion")["timestamp"].agg(["min", "max"]).sort_index()
    hueco_previo = (inicios["min"] - inicios["max"].shift()).fillna(float("inf"))
    filas = []
    previa = None
    for e in sorted(eventos, key=lambda e: e["inicioTs"]):
        if e["tipo"] not in ("llegada_agua", "corte_prolongado"):
            continue
        d = e.get("detalle") or {}
        h, dia, _ = hora_lima(e["inicioTs"])
        ventana = pdf[(pdf["timestamp"] >= e["inicioTs"]) & (pdf["timestamp"] <= e["finTs"] + 30 * MIN)]
        previos = inicios[inicios["min"] <= e["inicioTs"]]
        artefacto = bool(len(previos) and e["inicioTs"] - previos["min"].iloc[-1] < 15 * MIN
                         and hueco_previo[previos.index[-1]] > 30 * MIN)
        marca = [a for t, a in bitacora if abs(t - e["inicioTs"]) <= 5 * MIN]
        intervalo = None
        if e["tipo"] == "llegada_agua" and not artefacto:  # el intervalo solo entre llegadas que no son artefacto
            intervalo = (e["inicioTs"] - previa) / H if previa else None
            previa = e["inicioTs"]
        filas.append({"tipo": e["tipo"], "eventoId": e["eventoId"], "ts": e["inicioTs"], "fin_ts": e["finTs"],
                      "artefacto_reanudacion": artefacto, "bitacora": marca[0] if marca else None,
                      "hora_lima": h, "dia_semana": dia, "valor": e["valor"], "unidad": e["unidad"],
                      "subida_pct": e["valor"] if e["tipo"] == "llegada_agua" else None,
                      "caudal_lpm": d.get("caudalLlenadoLpm"), "fuera_franja": d.get("fueraDeFranja"),
                      "criterio": d.get("criterio"), "horas_sin_llegada": e["valor"] if e["tipo"] == "corte_prolongado" else None,
                      "intervalo_desde_anterior_h": intervalo, "version_reglas": e["versionRegla"],
                      "tipo_anomalia": _modo(ventana["tipoAnomalia"]) if "tipoAnomalia" in ventana else None})
    return pd.DataFrame(filas)


def d6(pdf, fuente, sensor_id, run_id, ini_h=0.0, fin_h=3.5):
    v = pdf[pdf["distanciaCompensadaCm"].notna()].copy()
    if "compensacionOrigen" in v:  # f4.4: sin saber si el firmware compensaba, la distancia no sirve para D6
        v = v[v["compensacionOrigen"] != "desconocida"]
    if v.empty:
        return pd.DataFrame()
    hl = v["timestamp"].apply(hora_lima)
    v["h"] = [x[0] for x in hl]
    v["noche"] = [x[2] for x in hl]
    v = v[(v["h"] >= ini_h) & (v["h"] < fin_h)]
    filas = []
    for noche, n in v.groupby("noche"):
        n = n.sort_values("timestamp")
        minutos = n["timestamp"].floordiv(MIN).nunique()
        a = n[n["timestamp"] - n["timestamp"].iloc[0] < 15 * MIN]
        b = n[n["timestamp"].iloc[-1] - n["timestamp"] < 15 * MIN]
        horas = max(0.25, (n["timestamp"].iloc[-1] - n["timestamp"].iloc[0]) / H)
        desc_c = b["distanciaCompensadaCm"].median() - a["distanciaCompensadaCm"].median()
        desc_r = b["distanciaCm"].median() - a["distanciaCm"].median()
        filas.append({"noche": noche, "ts": int(n["timestamp"].iloc[0]), "minutos_con_datos": int(minutos),
                      "cobertura": round(minutos / ((fin_h - ini_h) * 60), 3),
                      "descenso_cm_compensado": desc_c, "descenso_cm_crudo": desc_r,
                      # quién compensó: "detector" (flota: crudo = sin compensar) o "firmware" (ESP32: crudo = compensado)
                      "compensacion_origen": n["compensacionOrigen"].mode().iloc[0] if "compensacionOrigen" in n else None,
                      "tasa_cm_h_compensada": desc_c / horas, "distancia_media_cm": n["distanciaCompensadaCm"].mean(),
                      "temp_inicio_c": a["temperaturaC"].median(), "temp_fin_c": b["temperaturaC"].median(),
                      "hubo_subida": bool((n["nivelPct"].diff() > 2).any()) if "nivelPct" in n else None,
                      "frac_fuga_lenta": float((n["tipoAnomalia"] == "fuga_lenta").mean()) if "tipoAnomalia" in n else 0.0,
                      "frac_deriva_termica": float((n["tipoAnomalia"] == "deriva_termica").mean()) if "tipoAnomalia" in n else 0.0})
    r = pd.DataFrame(filas)
    r["etiqueta"] = np.where(r["frac_fuga_lenta"] > 0.5, "fuga_lenta",
                             np.where(r["frac_deriva_termica"] > 0.5, "deriva_termica", None))
    return r


def d8(pdf, fuente, sensor_id, run_id, eventos, juliaca, tau_h=25.2):
    if pdf["temperaturaC"].notna().sum() == 0 and pdf.get("tempAguaC", pd.Series(dtype=float)).notna().sum() == 0:
        return pd.DataFrame()
    v = pdf.copy()
    v["hora"] = v["timestamp"] // H * H
    agg = {"temp_aire_camara_c": ("temperaturaC", "mean"), "nivel_pct": ("nivelPct", "mean"),
           "n_lecturas": ("timestamp", "size"), "tramo": ("tramo", "min")}
    if "tempAguaC" in v:
        agg["temp_agua_real_c"] = ("tempAguaC", "mean")
    g = v.groupby("hora").agg(**agg).reset_index().sort_values("hora")
    if juliaca is not None and sensor_id.startswith(("esp32", "flota")):
        g = g.merge(juliaca, how="left", left_on="hora", right_on="hora")
    else:
        g["temp_juliaca_c"] = np.nan
    # Agua estimada provisional: primer orden desde el aire de la cámara, dentro de cada tramo (sin cruzar huecos)
    est, tw, prev_t, prev_tramo = [], None, None, None
    for _, r in g.iterrows():
        aire = r["temp_aire_camara_c"]
        if prev_tramo is not None and r["tramo"] != prev_tramo:
            tw = None
        if aire is not None and not (isinstance(aire, float) and math.isnan(aire)):
            tw = aire if tw is None else tw + (aire - tw) * (1 - math.exp(-((r["hora"] - prev_t) / H) / tau_h))
            prev_t = r["hora"]
        est.append(tw)
        prev_tramo = r["tramo"]
    g["temp_agua_estimada_provisional_c"] = est
    llegadas = sorted(e["inicioTs"] for e in eventos if e["tipo"] == "llegada_agua")
    res = []
    for h in g["hora"]:
        prev = [l for l in llegadas if l <= h]
        res.append((h - prev[-1]) / H if prev else np.nan)
    g["horas_residencia"] = res
    hl = g["hora"].apply(hora_lima)
    g["hora_lima"] = [x[0] for x in hl]
    return g.rename(columns={"hora": "ts"})
