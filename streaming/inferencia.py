"""
Inferencia en vivo de los modelos de F6 (F7), en Python puro: la consulta "predicciones" de Spark la llama en cada
micro-lote (foreachBatch) y las pruebas locales la usan igual.

  Modelos   batch/modelos/<dX>/<versión>-<fecha>/ (en el contenedor: /app/config/modelos). Se usa la versión indicada
            en VERSIONES; cada lote se mira la fecha de modificación del archivo y, si cambió, se recarga sin
            reiniciar el job. La versión (nombre de la carpeta) va como tag version_modelo en cada predicción.
  Estado    por tanque, en un JSON junto al checkpoint de la consulta: el historial mínimo que necesitan los modelos
            (nivel de los últimos minutos, llegadas, noches, aire por hora, filtros térmicos, pronósticos pendientes).
            Se escribe al final de cada lote con el batchId: si Spark repite un lote ya procesado, se ignora.
  Salida    measurement "predicciones", tags sensorId, origen, dimension, version_modelo y tipo:
              estimacion    lo que el modelo dice ahora (con su instante objetivo, objetivo_ts, si es un pronóstico)
              pronostico    el valor pronosticado escrito EN el instante objetivo, para compararlo con el real
              verificacion  cuando llega el instante objetivo: real, pronosticado y error (el error en vivo)
              motivo        por qué el modelo no aplica a este tanque (en vez de una cifra)
            y measurement "inferencia" (latencia y volumen por lote, recargas de modelos).

  D2  autonomía con el perfil de consumo (P50) hasta 12 h ("más de 12 h"), consumo medio como comparación, banda
      calibrada [aut_p90 / factor, aut_p10 · factor]; pronóstico del nivel a 60 min. ThingSpeak: advertencia.
  D4  horas desde la última llegada (subida sostenida, criterio de D2/D4) -> hora más probable, ventana P10-P90
      calibrada y probabilidad de llegada en 6/12/24 h.
  D6  una vez por noche (00:00-03:30 Lima): tasa (Theil-Sen), referencia, exceso, P(fuga) y pérdida a 24 h con su
      banda. Sin la de 7 días (no hay evidencia). Complementa a la regla alerta_fuga congelada.
  D8  agua estimada ahora y a 6/12 h con banda, probabilidad de cruzar 4 °C o 25 °C en 12 h y residencia (edad media
      del agua, tanque bien mezclado).
"""
import json
import math
import os
import statistics as st
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from fuga import ModeloFuga, theil_sen
from llegadas import ModeloLlegadas
from perfil_consumo import PerfilConsumo
from termico import ModeloTermico

MODELOS_DIR = Path(os.getenv("MODELOS_DIR", "/app/config/modelos"))
VERSIONES = {"D2": "d2/d2.2", "D4": "d4/d4.1", "D6": "d6/d6.1", "D8": "d8/d8.1"}
ARCHIVO = {"D2": "perfil.json", "D4": "modelo.json", "D6": "modelo.json", "D8": "modelo.json"}
H, MIN = 3_600_000, 60_000
LIMA = timezone(timedelta(hours=-5))
CADENCIA_MS = 15 * MIN                     # D2 y D4: una predicción cada 15 min de tiempo de evento por tanque
MOTIVO_CADA_MS = H                         # los motivos se repiten cada hora (para que el tablero los muestre)
SUBIDA_LLENADO_PP, QUIETO_PP, FIN_LLENADO_MS = 10.0, 0.2, 20 * MIN   # criterio de llenado de D2 y D4
UMBRAL_D2 = 10.0
OM_JULIACA = "open-meteo-juliaca"
# D8: Open-Meteo de la ubicación de cada tanque. Con él, la variante validada (aire de la cámara + Open-Meteo); sin él,
# "solo aire". La flota no tiene: sus fechas son simuladas (28-sep a 28-oct), anteriores al Open-Meteo del stream.
OM_DE_TANQUE = {"esp32-tanque-real-01": OM_JULIACA, "thingspeak-3418755": "open-meteo-canal3418755"}


def hora_lima(ts):
    d = datetime.fromtimestamp(ts / 1000, LIMA)
    return d, d.hour + d.minute / 60


# --- Registro de modelos ---------------------------------------------------------------------------------------

class Registro:
    def __init__(self, base=MODELOS_DIR):
        self.base = Path(base)
        self.modelos, self.version, self._mtime = {}, {}, {}
        self.recargas = []

    def refrescar(self):
        self.recargas = []
        for dim, prefijo in VERSIONES.items():
            carpetas = sorted(self.base.glob(prefijo + "-*"))
            if not carpetas:
                continue
            ruta = carpetas[-1] / ARCHIVO[dim]
            m = ruta.stat().st_mtime
            if self._mtime.get(dim) != m:
                self.modelos[dim] = json.loads(ruta.read_text(encoding="utf-8"))
                self.version[dim] = carpetas[-1].name
                if dim in self._mtime:
                    self.recargas.append({"dimension": dim, "version": carpetas[-1].name, "archivo": str(ruta)})
                self._mtime[dim] = m
                self._cache = {}
        return self

    def objeto(self, dim, clave, fabrica):
        """Objetos del modelo (PerfilConsumo, ModeloLlegadas...) construidos una vez por versión cargada."""
        self._cache = getattr(self, "_cache", {})
        k = (dim, clave)
        if k not in self._cache:
            self._cache[k] = fabrica()
        return self._cache[k]


# --- Estado ------------------------------------------------------------------------------------------------------

def cargar_estado(ruta):
    try:
        return json.loads(Path(ruta).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {"batch": -1, "tanques": {}, "om": {}}


def guardar_estado(ruta, estado):
    ruta = Path(ruta)
    ruta.parent.mkdir(parents=True, exist_ok=True)
    tmp = ruta.with_suffix(".tmp")
    tmp.write_text(json.dumps(estado, separators=(",", ":")), encoding="utf-8")
    os.replace(tmp, ruta)


def tanque_nuevo():
    return {"ult_ts": 0, "niv": [], "med15": None, "med_min": None,
            "llenando": False, "sost_min": None, "max_ll": None, "max_ll_ts": 0, "llegada_ts": None, "fin_ll_ts": None,
            "K": 0.0, "Uk": 0.0, "A": 0.0, "V": None,
            "noche": None, "noches": [], "ult_exceso": None, "ult_exceso_ts": None,
            "hora_act": None, "aire_acc": [0.0, 0], "agua_acc": [0.0, 0], "aire_h": [], "om_h": [], "hora_h": [],
            "F": {}, "horas_filtro": 0,
            "ult_d2": 0, "ult_d4": 0, "ult_motivo": {}, "pend": {"D2": [], "D4": None, "D6": None, "D8": []}}


# --- Inferencia ------------------------------------------------------------------------------------------------------

class Inferencia:
    def __init__(self, registro, detector):
        self.reg, self.det = registro, detector

    def procesar(self, filas, estado, batch_id):
        """filas: dicts con sensorId, origen, fuenteTipo, runId, timestamp, distanciaCm, nivelPct, nivelM,
        temperaturaC, tempAguaC. Devuelve {bucket_logico: [líneas]} y el resumen del lote."""
        t0 = time.time()
        self.reg.refrescar()
        cfg = self.det.cfg.refrescar()
        salida = {"real": [], "aumentado": []}
        if batch_id <= estado.get("batch", -1):   # lote repetido tras un reinicio: ya está en el estado
            return salida, {"lecturas": 0, "repetido": True, "duracion_ms": 0}
        om = estado["om"]
        if om and not isinstance(next(iter(om.values())), dict):  # estado anterior: solo Juliaca, sin ubicación
            estado["om"] = om = {OM_JULIACA: om}
        for r in sorted(filas, key=lambda r: r["timestamp"] or 0):
            if r.get("origen") == "open-meteo" and r.get("temperaturaC") is not None:
                om.setdefault(r["sensorId"], {})[str(r["timestamp"] // H * H)] = r["temperaturaC"]
        for serie in om.values():
            if len(serie) > 600:
                for k in sorted(serie)[:-600]:
                    del serie[k]
        n = 0
        for r in sorted(filas, key=lambda r: r["timestamp"] or 0):
            sid = r.get("sensorId")
            geom = cfg.tanques.get(sid)
            if not geom or r.get("origen") == "open-meteo" or r.get("timestamp") is None:
                continue
            clave = f"{r.get('runId') or ''}|{sid}"
            e = estado["tanques"].setdefault(clave, tanque_nuevo())
            if r["timestamp"] <= e["ult_ts"]:
                continue  # duplicado o fuera de orden
            n += 1
            origen_l = "aumentado" if r.get("fuenteTipo") == "aumentado" else "real"
            ctx = {"sid": sid, "geom": geom, "reglas": cfg.reglas, "run": r.get("runId") or "", "origen": r.get("origen"),
                   "origen_l": origen_l, "om": om.get(OM_DE_TANQUE.get(sid, OM_JULIACA), {}),
                   "om_propio": OM_DE_TANQUE.get(sid) if origen_l == "real" else None, "emitir": salida[origen_l]}
            self._lectura(e, r, ctx)
        estado["batch"] = batch_id
        resumen = {"lecturas": n, "puntos_real": len(salida["real"]), "puntos_aumentado": len(salida["aumentado"]),
                   "duracion_ms": round((time.time() - t0) * 1000), "recargas": self.reg.recargas,
                   "versiones": dict(self.reg.version)}
        return salida, resumen

    # -- una lectura ------------------------------------------------------------------------------------------------
    def _lectura(self, e, r, c):
        ts = r["timestamp"]
        e["ult_ts"] = ts
        valida, pct, nivel_cm, litros, dist, compensada = self.det.nivel(r, c["geom"], c["reglas"])
        if valida and pct is not None:
            self._nivel(e, ts, pct, c)
            if dist is not None:
                self._noche_acumular(e, ts, dist, r.get("temperaturaC"), c)
        self._noche_cerrar_si_termino(e, ts, c)
        self._termico(e, ts, r, c)
        if ts - e["ult_d2"] >= CADENCIA_MS and e["med15"] is not None:
            e["ult_d2"] = ts
            self._d2(e, ts, c)
            self._d4(e, ts, c)

    def _punto(self, c, dim, tipo, campos, ts):
        import influx
        tags = {"sensorId": c["sid"], "origen": c["origen"], "dimension": dim, "tipo": tipo,
                "version_modelo": self.reg.version.get(dim, "sin_modelo"), "origenLecturas": c["origen_l"]}
        if c["run"]:
            tags["runId"] = c["run"]
        lin = influx.linea("predicciones", tags, campos, ts)
        if lin:
            c["emitir"].append(lin)

    def _motivo(self, e, c, dim, texto, ts):
        if ts - e["ult_motivo"].get(dim, 0) >= MOTIVO_CADA_MS:
            e["ult_motivo"][dim] = ts
            self._punto(c, dim, "motivo", {"aplica": False, "motivo": texto}, ts)

    # -- nivel, llenados y residencia (D2, D4, D8) -----------------------------------------------------------------
    def _nivel(self, e, ts, pct, c):
        e["niv"] = [p for p in e["niv"] if ts - p[0] <= 15 * MIN] + [[ts, pct]]
        minuto = ts // MIN
        if e["med_min"] == minuto:
            return
        e["med_min"] = minuto
        med = st.median(p[1] for p in e["niv"])
        e["med15"] = med
        # Llenado (subida sostenida > 10 pp sobre el mínimo desde el último llenado; termina sin subir 20 min)
        if not e["llenando"]:
            e["sost_min"] = med if e["sost_min"] is None else min(e["sost_min"], med)
            if med - e["sost_min"] > SUBIDA_LLENADO_PP:
                e.update(llenando=True, max_ll=med, max_ll_ts=ts)
                self._llegada(e, ts, c)
                self._residencia_entra(e, med - e["sost_min"])
        else:
            if med > e["max_ll"] + QUIETO_PP:
                if e["V"] is not None and med > e["V"]:
                    self._residencia_entra(e, med - e["V"])
                e["max_ll"], e["max_ll_ts"] = med, ts
            elif ts - e["max_ll_ts"] > FIN_LLENADO_MS:
                e.update(llenando=False, sost_min=med, fin_ll_ts=ts)
        # Residencia: el consumo cambia el volumen, no la mezcla de edades
        if e["V"] is None:
            e.update(K=0.0, Uk=max(med, 1.0), A=0.0, V=max(med, 1.0))
        else:
            V = max(med, 1.0)
            dt_h = (ts - (e.get("V_ts") or ts)) / H
            e["A"] += max(dt_h, 0.0)
            tot = e["K"] + e["Uk"]
            if tot > 0 and V < tot:
                e["K"], e["Uk"] = e["K"] * V / tot, e["Uk"] * V / tot
            e["V"] = V
        e["V_ts"] = ts

    def _residencia_entra(self, e, d):
        if d <= 0:
            return
        e["A"] = e["A"] * e["K"] / (e["K"] + d) if e["K"] + d > 0 else 0.0
        e["K"] += d
        e["V"] = e["K"] + e["Uk"]

    def _llegada(self, e, ts, c):
        p = e["pend"].get("D4")
        if p and ts > p["creado"]:  # verificación de D4: llegada real frente a la hora más probable pronosticada
            self._punto(c, "D4", "verificacion", {"llegada_real_ts": ts, "llegada_pred_ts": p["moda_ts"],
                                                  "error_h": round((ts - p["moda_ts"]) / H, 3),
                                                  "dentro_ventana": p["ini"] <= ts <= p["fin"]}, ts)
            e["pend"]["D4"] = None
        e["llegada_ts"] = ts

    # -- D2 ---------------------------------------------------------------------------------------------------------
    def _d2(self, e, ts, c):
        m = self.reg.modelos.get("D2")
        nivel = e["med15"]
        # Verificación de pronósticos de nivel cuyo instante objetivo ya pasó
        pend = []
        for obj, pred, creado in e["pend"]["D2"]:
            if ts >= obj:
                self._punto(c, "D2", "verificacion", {"nivel_real_pct": round(nivel, 2), "nivel_pred_pct": round(pred, 2),
                                                      "error_pp": round(nivel - pred, 3), "creado_ts": creado}, obj)
            else:
                pend.append([obj, pred, creado])
        e["pend"]["D2"] = pend[-8:]
        if not m:
            return self._motivo(e, c, "D2", "sin modelo D2 cargado", ts)
        if c["geom"].get("por_medir"):
            return self._motivo(e, c, "D2", "geometría provisional (por_medir): el nivel en % es aproximado", ts)
        clave = next((k for k in m["tanques"] if k.split("|")[1] == c["sid"] and k.split("|")[2] == c["run"]), None)
        if not clave:
            return self._motivo(e, c, "D2", "sin perfil de consumo entrenado para este tanque", ts)
        perfil = self.reg.objeto("D2", clave, lambda: PerfilConsumo.desde_dict(m["tanques"][clave]))
        nivel_cal = "consistencia" if clave.startswith("aumentado") else f"{clave.split('|')[0]}_real"
        factor = m["calibracion_banda"].get(nivel_cal, {}).get("factor", 1.0)
        aut = {q: float(perfil.horas_autonomia(np.array([nivel]), np.array([ts]), UMBRAL_D2, q)[0])
               for q in ("p10", "p50", "p90")}
        consumo_medio = m["tanques"][clave].get("consumo_medio_pct_h") or perfil.consumo_medio_h()
        aut_medio = (nivel - UMBRAL_D2) / consumo_medio if consumo_medio and consumo_medio > 0 else None
        mas_de_12 = aut["p50"] > 12
        campos = {"aplica": True, "nivel_pct": round(nivel, 2), "autonomia_h": round(min(aut["p50"], 12.0), 2),
                  "autonomia_texto": "más de 12 h" if mas_de_12 else f"{aut['p50']:.1f} h",
                  "autonomia_p10_h": round(min(aut["p90"] / factor, 12.0), 2),
                  "autonomia_p90_h": round(min(aut["p10"] * factor, 12.0), 2),
                  "autonomia_consumo_medio_h": round(min(aut_medio, 72.0), 2) if aut_medio is not None else None,
                  "objetivo_ts": None if mas_de_12 else int(ts + aut["p50"] * H), "llenando": e["llenando"]}
        if clave.startswith("thingspeak"):
            campos["advertencia"] = "en ThingSpeak el perfil tiende a ser optimista (subestima el consumo)"
        self._punto(c, "D2", "estimacion", campos, ts)
        pred60 = float(perfil.pronostico(np.array([nivel]), np.array([ts]), 60)[0])
        obj = ts + 60 * MIN
        self._punto(c, "D2", "pronostico", {"nivel_pred_pct": round(pred60, 2), "horizonte_min": 60, "creado_ts": ts}, obj)
        e["pend"]["D2"].append([obj, pred60, ts])

    # -- D4 ---------------------------------------------------------------------------------------------------------
    def _d4(self, e, ts, c):
        m = self.reg.modelos.get("D4")
        if not m:
            return self._motivo(e, c, "D4", "sin modelo D4 cargado", ts)
        fuentes = m["fuentes_llegadas"]
        zona = c["geom"].get("zona")
        entrada, fuente_ll = None, None
        # ThingSpeak: las llegadas por subida sostenida (el detector f4.3 no veía sus llenados); el resto, el detector
        orden = ("subida_sostenida", "detector_f43") if c["sid"].startswith("thingspeak") else ("detector_f43",)
        for fuente in orden:
            for k, v in fuentes.get(fuente, {}).items():
                partes = k.split("|")
                if (partes[1] == c["sid"] or partes[1] == f"zona:{zona}") and partes[2] == c["run"]:
                    entrada, fuente_ll = v, fuente
                    break
            if entrada:
                break
        if not entrada:
            return self._motivo(e, c, "D4", "sin modelo de llegadas para este tanque (pocas llegadas observadas)", ts)
        if entrada.get("fuera_de_alcance"):
            return self._motivo(e, c, "D4", f"fuera del alcance de D4: {entrada['fuera_de_alcance']}", ts)
        if e["llegada_ts"] is None:
            return self._motivo(e, c, "D4", "esperando la primera llegada observada en el stream", ts)
        tipo = "weibull" if entrada.get("weibull") else "km"
        mod = self.reg.objeto("D4", (c["sid"], c["run"], tipo), lambda: ModeloLlegadas.desde_dict(entrada[tipo]))
        horas = (ts - e["llegada_ts"]) / H
        _, hl = hora_lima(ts)
        r = mod.predecir(int(horas), int(hl) % 24)
        q = entrada.get("ventana_q_h") or 0.0
        moda_ts = int(ts + r["moda_h"] * H)
        ini, fin = int(ts + max(0.0, r["p10_h"] - q) * H), int(ts + (r["p90_h"] + q) * H)
        self._punto(c, "D4", "estimacion", {
            "aplica": True, "horas_desde_llegada": round(horas, 2), "horas_moda": r["moda_h"], "objetivo_ts": moda_ts,
            "ventana_ini_ts": ini, "ventana_fin_ts": fin, "ventana_ini_h": round(max(0.0, r["p10_h"] - q), 2),
            "ventana_fin_h": round(r["p90_h"] + q, 2), "prob_6h": round(r["prob_6h"], 4),
            "prob_12h": round(r["prob_12h"], 4), "prob_24h": round(r["prob_24h"], 4), "modelo_tipo": tipo,
            "fuente_llegadas": fuente_ll}, ts)
        self._punto(c, "D4", "pronostico", {"llegada_prevista": 1.0, "horizonte_horas": float(r["moda_h"]), "creado_ts": ts},
                    moda_ts)
        e["pend"]["D4"] = {"creado": ts, "moda_ts": moda_ts, "ini": ini, "fin": fin}

    # -- D6 ---------------------------------------------------------------------------------------------------------
    def _noche_acumular(self, e, ts, dist, temp, c):
        m = self.reg.modelos.get("D6")
        ini_h, fin_h = (m or {}).get("franja_lima_h", [0.0, 3.5])
        d, h = hora_lima(ts)
        if not ini_h <= h < fin_h:
            return
        fecha = d.date().isoformat()
        n = e["noche"]
        if n is None or n["fecha"] != fecha:
            n = e["noche"] = {"fecha": fecha, "ini": int(datetime(d.year, d.month, d.day, tzinfo=LIMA).timestamp() * 1000
                                                            + ini_h * H),
                              "min": {}, "temp": {}, "llenado": False}
        k = str(ts // MIN)
        n["min"].setdefault(k, []).append(dist)
        if temp is not None and temp != -1:
            n["temp"].setdefault(k, []).append(temp)
        if e["llenando"] or (e.get("fin_ll_ts") and n["ini"] - e["fin_ll_ts"] < H):
            n["llenado"] = True

    def _noche_cerrar_si_termino(self, e, ts, c):
        n = e["noche"]
        m = self.reg.modelos.get("D6")
        if n is None:
            return
        ini_h, fin_h = (m or {}).get("franja_lima_h", [0.0, 3.5])
        if ts < n["ini"] + (fin_h - ini_h) * H:
            return
        e["noche"] = None
        fin = int(n["ini"] + (fin_h - ini_h) * H)
        geom = c["geom"]
        modo = geom.get("modo", "distancia" if "sensor_al_fondo_cm" in geom else "sin_nivel")
        if not m:
            return self._punto(c, "D6", "motivo", {"aplica": False, "motivo": "sin modelo D6 cargado"}, fin)
        if modo != "distancia":
            return self._punto(c, "D6", "motivo", {"aplica": False, "motivo": "el canal informa nivel, no distancia"}, fin)
        if geom.get("por_medir"):
            return self._punto(c, "D6", "motivo", {"aplica": False, "motivo":
                                                   "geometría provisional (por_medir): los litros por hora no son reales"}, fin)
        lpc = geom["capacidad_l"] / geom["altura_interior_cm"]
        claves = sorted(n["min"], key=int)
        t_h = np.array([(int(k) * MIN - n["ini"]) / H for k in claves])
        y = np.array([st.median(n["min"][k]) for k in claves])
        pend, mad = theil_sen(t_h, y)
        cobertura = len(claves) / ((fin_h - ini_h) * 60)
        nv = m["noche_valida"]
        if pend is None or cobertura < nv["cobertura_min"] or n["llenado"]:
            motivo = "noche no válida: " + ("hubo llenado" if n["llenado"] else f"cobertura {cobertura:.0%}")
            return self._punto(c, "D6", "motivo", {"aplica": False, "motivo": motivo}, fin)
        tasa = pend * lpc
        hist = [x[1] for x in e["noches"]]
        ref_n, ref_min = m["referencia"]["noches"], m["referencia"]["minimo"]
        if len(hist) < ref_min:
            e["noches"] = (e["noches"] + [[fin, tasa]])[-12:]
            return self._punto(c, "D6", "motivo", {"aplica": False, "motivo":
                                                   f"faltan noches de referencia ({len(hist)} de {ref_min})"}, fin)
        ref = float(np.median(hist[-ref_n:]))
        estab = float(np.std(hist[-m["estabilidad_noches"]:]))
        exceso = tasa - ref
        prev = e["ult_exceso"] if e["ult_exceso_ts"] and fin - e["ult_exceso_ts"] <= 2 * 24 * H else 0.0
        temps = [(int(k), st.median(v)) for k, v in n["temp"].items()]
        temps.sort()
        ini_t = [v for k, v in temps if (k - temps[0][0]) < 15] if temps else []
        fin_t = [v for k, v in temps if (temps[-1][0] - k) < 15] if temps else []
        dT = (st.median(ini_t) - st.median(fin_t)) if ini_t and fin_t else 0.0
        om0, om3 = c["om"].get(str(n["ini"] // H * H)), c["om"].get(str(n["ini"] // H * H + 3 * H))
        dTj = (om0 - om3) if om0 is not None and om3 is not None else 0.0
        h_ll = min(72.0, (n["ini"] - e["fin_ll_ts"]) / H) if e.get("fin_ll_ts") else 72.0
        x = {"exceso_lph": exceso, "exceso_prev_lph": prev, "estabilidad_lph": estab, "dT_dht_c": dT,
             "dT_juliaca_c": dTj, "log_horas_llenado": math.log1p(max(h_ll, 0.0)), "ruido_cm": mad}
        mod = self.reg.objeto("D6", "clasificador", lambda: ModeloFuga.desde_dict(m["clasificador"]))
        out = mod.proyectar(np.array([[x[v] for v in mod.variables]]), np.array([exceso]))
        # Verificación de la noche anterior: pérdida pronosticada a 24 h frente al exceso observado hoy × 24
        p = e["pend"].get("D6")
        if p and fin - p["creado"] <= 2 * 24 * H:
            real = max(exceso, 0.0) * 24
            self._punto(c, "D6", "verificacion", {"perdida_real_proxy_l": round(real, 2), "perdida_pred_l": p["pred"],
                                                  "error_l": round(real - p["pred"], 2), "creado_ts": p["creado"]}, fin)
        campos = {"aplica": True, "noche": n["fecha"], "tasa_lph": round(tasa, 3), "referencia_lph": round(ref, 3),
                  "exceso_lph": round(exceso, 3), "p_fuga": round(float(out["p_fuga"][0]), 4), "fuga": bool(out["fuga"][0]),
                  "perdida_24h_l": round(float(out["perdida_24h_l"][0]), 2), "objetivo_ts": fin + 24 * H,
                  "complemento_de": "alerta_fuga (regla congelada f4.4)"}
        if "p10_24h_l" in out:
            campos["perdida_24h_p10_l"] = round(float(out["p10_24h_l"][0]), 2)
            campos["perdida_24h_p90_l"] = round(float(out["p90_24h_l"][0]), 2)
        self._punto(c, "D6", "estimacion", campos, fin)
        self._punto(c, "D6", "pronostico", {"perdida_24h_pred_l": campos["perdida_24h_l"], "creado_ts": fin}, fin + 24 * H)
        e["pend"]["D6"] = {"creado": fin, "pred": campos["perdida_24h_l"]}
        e["noches"] = (e["noches"] + [[fin, tasa]])[-12:]
        e["ult_exceso"], e["ult_exceso_ts"] = exceso, fin

    # -- D8 ---------------------------------------------------------------------------------------------------------
    def _termico(self, e, ts, r, c):
        hora = ts // H * H
        if e["hora_act"] is None:
            e["hora_act"] = hora
        if hora > e["hora_act"]:
            self._cerrar_hora(e, c)
            e["hora_act"], e["aire_acc"], e["agua_acc"] = hora, [0.0, 0], [0.0, 0]
        t, w = r.get("temperaturaC"), r.get("tempAguaC")
        if t is not None and t != -1:
            e["aire_acc"][0] += t
            e["aire_acc"][1] += 1
        if w is not None:
            e["agua_acc"][0] += w
            e["agua_acc"][1] += 1

    def _cerrar_hora(self, e, c):
        hora = e["hora_act"]
        m = self.reg.modelos.get("D8")
        if e["aire_acc"][1] == 0:
            return self._motivo(e, c, "D8", "sin temperatura del aire en este canal", hora + H)
        if not m:
            return self._motivo(e, c, "D8", "sin modelo D8 cargado", hora + H)
        aire = e["aire_acc"][0] / e["aire_acc"][1]
        agua_real = e["agua_acc"][0] / e["agua_acc"][1] if e["agua_acc"][1] else None
        variante = "aire_y_om" if c["om_propio"] else "solo_aire"
        if e.get("variante_d8") != variante:  # cambio de variante (otro τ): los filtros térmicos empiezan de nuevo
            e.update(variante_d8=variante, F={}, horas_filtro=0, aire_h=[], om_h=[], hora_h=[])
        om = c["om"].get(str(hora))
        if variante == "aire_y_om" and om is None:
            if not e["om_h"] and not c["om"]:
                return self._motivo(e, c, "D8", f"esperando Open-Meteo de su ubicación ({c['om_propio']})", hora + H)
            om = e["om_h"][-1] if e["om_h"] else list(c["om"].values())[-1]   # atrasado: se sostiene el último
        mod = self.reg.objeto("D8", variante, lambda: ModeloTermico.desde_dict(m["modelos"][variante]))
        a = math.exp(-1.0 / mod.tau)
        for ent, u in (("aire", aire), ("om", om)):
            if ent in mod.entradas:
                e["F"][ent] = u if ent not in e["F"] else a * e["F"][ent] + (1 - a) * u
        e["horas_filtro"] += 1
        e["aire_h"] = (e["aire_h"] + [aire])[-168:]
        e["om_h"] = (e["om_h"] + [om if om is not None else aire])[-168:]
        e["hora_h"] = (e["hora_h"] + [int(hora // H % 24)])[-168:]
        w_now = sum(g * e["F"][ent] for g, ent in zip(mod.ganancias, mod.entradas)) + mod.constante
        # Verificación: pronósticos cuyo instante objetivo es esta hora, contra el agua medida (solo 3418755)
        pend = []
        for obj, pred, h, creado in e["pend"]["D8"]:
            if obj <= hora:
                if obj == hora and agua_real is not None:
                    self._punto(c, "D8", "verificacion", {"agua_real_c": round(agua_real, 2), "agua_pred_c": round(pred, 2),
                                                          "error_c": round(agua_real - pred, 3), "horizonte_horas": float(h),
                                                          "creado_ts": creado}, obj)
            else:
                pend.append([obj, pred, h, creado])
        e["pend"]["D8"] = pend[-30:]
        # Pronóstico a 1..12 h con el aire futuro = ciclo de las últimas 168 h + la anomalía actual
        U = {"aire": np.array(e["aire_h"]), "om": np.array(e["om_h"])}
        estados = {ent: np.full(len(e["aire_h"]), e["F"][ent]) for ent in mod.entradas}
        fut = mod.pronosticar(U, np.array(e["hora_h"]), len(e["aire_h"]) - 1, 12, estados)
        campos = {"aplica": True, "variante": variante, "aire_c": round(aire, 2), "agua_c": round(w_now, 2)}
        for h, val in ((0, w_now), (6, fut[5]), (12, fut[11])):
            lo, hi = mod.banda(val, h)
            sufijo = "" if h == 0 else f"_{h}h"
            campos[f"agua{sufijo}_c"] = round(float(val), 2)
            if lo is not None:
                campos[f"agua{sufijo}_p10_c"], campos[f"agua{sufijo}_p90_c"] = round(float(lo), 2), round(float(hi), 2)
            if h:
                campos[f"objetivo_{h}h_ts"] = int(hora + h * H)
                self._punto(c, "D8", "pronostico", {"agua_pred_c": round(float(val), 2), "horizonte_horas": float(h),
                                                    "creado_ts": hora},
                            hora + h * H)
                e["pend"]["D8"].append([int(hora + h * H), float(val), h, hora])
        umb = m.get("umbrales_configurados", {"bajo_c": 4.0, "alto_c": 25.0})
        campos["prob_cruce_bajo_12h"] = round(self._prob_cruce(mod, fut, umb["bajo_c"], "bajo"), 4)
        campos["prob_cruce_alto_12h"] = round(self._prob_cruce(mod, fut, umb["alto_c"], "alto"), 4)
        tot = e["K"] + e["Uk"]
        if tot > 0 and e["Uk"] / tot < 0.1:
            campos["residencia_h"] = round(e["A"], 2)
        else:
            campos["residencia_motivo"] = "edad desconocida: falta un llenado que renueve el agua"
        adv = ["estimación sin sensor de agua; error de transferencia de 1-2 °C (τ del recipiente sin medir)"]
        if e["horas_filtro"] < 48:
            adv.append(f"filtro en arranque ({e['horas_filtro']} h de aire)")
        if variante != m.get("principal"):
            adv.append(f"variante {variante}: sin Open-Meteo de este lugar en el stream; sesgo conocido de "
                       "-0.9 °C y MAE 0.91 °C en la prueba de F6 (la validada: 0.16 °C)")
        campos["advertencia"] = "; ".join(adv)
        self._punto(c, "D8", "estimacion", campos, hora + H)

    @staticmethod
    def _prob_cruce(mod, fut, umbral, lado):
        """Máximo sobre 1..12 h de P(agua al otro lado del umbral), con un error normal de ancho la banda P10-P90."""
        p = 0.0
        for h in range(1, 13):
            c = mod.conforme.get(str(min((0, 6, 12), key=lambda x: abs(x - h))), {})
            if c.get("q_bajo") is None:
                continue
            centro = fut[h - 1] + (c["q_alto"] + c["q_bajo"]) / 2
            sigma = max((c["q_alto"] - c["q_bajo"]) / 2.563, 1e-3)
            z = (umbral - centro) / sigma
            phi = 0.5 * (1 + math.erf(z / math.sqrt(2)))
            p = max(p, phi if lado == "bajo" else 1 - phi)
        return p
