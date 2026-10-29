"""
Detector de eventos de servicio (F4): lógica pura, sin Spark.

Procesa las lecturas de UN sensor en orden de tiempo de evento y mantiene un estado pequeño (dict serializable a
JSON) entre micro-lotes. Spark lo usa dentro de applyInPandasWithState (estado por sensorId); la validación con
datos aumentados reusa exactamente la misma clase.

Reglas (umbrales en streaming/reglas_servicio.yml, geometría en ingesta/tanques.yml; ambos se releen al cambiar):
  falla_sensor      N lecturas de nivel inválidas seguidas (-1, fuera de rango o más allá del fondo)
  llegada_agua      el nivel sube más de X % de la capacidad dentro de la ventana; caudal = subida × capacidad / Δt
  corte_prolongado  más de N horas sin llegada_agua
  autonomia_baja    el nivel cae bajo un umbral (%), con rearme
  alerta_fuga       descenso nocturno (sin llenado) sobre la distancia compensada con el DHT11, mayor que la
                    incertidumbre del DHT11 y que la tasa nocturna habitual del tanque
  umbral_termico    proyección de la temperatura del agua (inercia de primer orden desde el aire de la cámara)
                    que cruza el umbral en las próximas horas; versionRegla "provisional"
"""
import math
import os
import statistics as st
from datetime import datetime, timedelta, timezone

import yaml

LIMA = timezone(timedelta(hours=-5))
C20 = 331.3 + 0.606 * 20.0


def _cargar(ruta):
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)


class Configuracion:
    """reglas_servicio.yml + tanques.yml, recargados cuando cambia su fecha de modificación."""

    def __init__(self, ruta_reglas, ruta_tanques):
        self.rutas = (ruta_reglas, ruta_tanques)
        self._mtimes = None
        self.reglas, self.tanques = {}, {}
        self.refrescar()

    def refrescar(self):
        mtimes = tuple(os.path.getmtime(r) for r in self.rutas)
        if mtimes != self._mtimes:
            self.reglas = _cargar(self.rutas[0])
            self.tanques = (_cargar(self.rutas[1]) or {}).get("tanques", {})
            self._mtimes = mtimes
        return self


def hora_lima(ts_ms):
    d = datetime.fromtimestamp(ts_ms / 1000, LIMA)
    return d, d.hour + d.minute / 60 + d.second / 3600


class Detector:
    def __init__(self, config: Configuracion):
        self.cfg = config

    # --- nivel ------------------------------------------------------------------------------
    def nivel(self, lec, geom, reglas):
        """(valida, pct, nivel_cm, litros, distancia_usada, compensada) a partir de la lectura y la geometría."""
        modo = geom.get("modo", "distancia" if "sensor_al_fondo_cm" in geom else "sin_nivel")
        cap = geom.get("capacidad_l")
        if modo == "distancia":
            d = lec.get("distanciaCm")
            fondo, alto = geom["sensor_al_fondo_cm"], geom["altura_interior_cm"]
            if d is None or d == -1 or not 2 <= d <= 450 or d > fondo + reglas["falla_sensor"]["margen_fondo_cm"]:
                return (False,) + (None,) * 5
            t = lec.get("temperaturaC")
            comp = reglas["compensacion_termica"]
            lo, hi = comp["temp_valida_c"]
            compensada = bool(comp["activa"] and t is not None and t != -1 and lo <= t <= hi)
            dc = d * (331.3 + 0.606 * t) / C20 if compensada else d
            nivel_cm = fondo - dc
            pct = nivel_cm / alto * 100
            return True, pct, nivel_cm, (pct / 100 * cap if cap else None), dc, compensada
        if modo == "nivelPct":
            p = lec.get("nivelPct")
            if p is None or not 0 <= p <= 100:
                return (False,) + (None,) * 5
            return True, p, None, (p / 100 * cap if cap else None), None, False
        if modo == "nivelM":
            m = lec.get("nivelM")
            if m is None:
                return (False,) + (None,) * 5
            nivel_cm = m * 100
            pct = nivel_cm / geom["altura_interior_cm"] * 100
            return True, pct, nivel_cm, (pct / 100 * cap if cap else None), None, False
        # sin_nivel: solo cuenta para falla_sensor por validez de la lectura
        return (bool(lec.get("lectura_valida", True)),) + (None,) * 5

    # --- principal --------------------------------------------------------------------------
    def procesar(self, sensor_id, lecturas, estado, origen_lecturas, ahora_ms):
        cfg = self.cfg.refrescar()
        reglas = cfg.reglas
        geom = cfg.tanques.get(sensor_id, {"zona": "desconocida", "modo": "sin_nivel"})
        zona = geom.get("zona", "desconocida")
        e = estado or {"ult_ts": 0, "primera_ts": None, "buf": [], "ult3": [], "llenando": False,
                       "ultima_llegada_ts": None, "ultimo_llenado_fin_ts": None, "corte_alertado": False,
                       "inval": 0, "inval_inicio": None, "falla_alertada": False, "autonomia_alertada": False,
                       "tw": None, "tw_ts": None, "termico_eval_ts": 0, "termico_alertado": None,
                       "noche": None, "base_cm_h": reglas["alerta_fuga"]["base_inicial_cm_h"]}
        eventos = []

        def emitir(tipo, severidad, inicio, fin, valor, umbral, unidad, detalle, version=None):
            eventos.append({
                "eventoId": f"{tipo}-{sensor_id}-{int(inicio)}", "tipo": tipo, "sensorId": sensor_id, "zona": zona,
                "severidad": severidad, "inicioTs": int(inicio), "finTs": int(max(fin, inicio)),
                "valor": None if valor is None else round(float(valor), 3),
                "umbral": None if umbral is None else round(float(umbral), 3), "unidad": unidad,
                "origenLecturas": origen_lecturas, "detectadoTs": int(ahora_ms),
                "versionRegla": version or str(reglas["version"]), "detalle": detalle})

        for lec in lecturas:
            ts = lec.get("timestamp")
            if ts is None or ts <= e["ult_ts"]:
                continue  # repetida o fuera de orden respecto del estado
            e["hueco_h"] = (ts - e["ult_ts"]) / 3_600_000 if e["ult_ts"] else 0.0
            e["ult_ts"] = ts
            if e["primera_ts"] is None:
                e["primera_ts"] = ts
            valida, pct, nivel_cm, litros, dist, compensada = self.nivel(lec, geom, reglas)

            # falla_sensor: N lecturas de nivel inválidas seguidas
            fs = reglas["falla_sensor"]
            if not valida:
                e["inval"] += 1
                e["validas_desde"] = None
                if e["inval"] == 1:
                    e["inval_inicio"] = ts
                if e["inval"] >= fs["lecturas_invalidas_seguidas"]:
                    e["post_falla"] = True  # al volver, el nivel de antes de la falla no sirve de referencia
                    if not e["falla_alertada"]:
                        emitir("falla_sensor", "advertencia", e["inval_inicio"], ts, e["inval"],
                               fs["lecturas_invalidas_seguidas"], "lecturas",
                               {"distanciaCm": lec.get("distanciaCm"), "nivelPct": lec.get("nivelPct")})
                        e["falla_alertada"] = True
                continue
            e["inval"] = 0
            # Histéresis: un sensor que alterna -1 y lecturas válidas es UNA falla; se rearma tras un rato estable
            e["validas_desde"] = e.get("validas_desde") or ts
            if e["falla_alertada"] and ts - e["validas_desde"] >= fs.get("rearme_validas_min", 0) * 60000:
                e["falla_alertada"] = False
            if e.get("post_falla"):
                e.update(post_falla=False, buf=[], ult3=[], conf=0, buf_largo=[], conf_largo=0)
            if pct is None:
                continue  # sensor sin geometría: solo falla_sensor

            self._llegada(e, ts, pct, litros, geom, zona, reglas, emitir)
            self._corte(e, ts, reglas, emitir)
            self._autonomia(e, ts, pct, litros, reglas, emitir)
            if nivel_cm is not None:
                self._fuga(e, ts, nivel_cm, dist, compensada, geom, reglas, emitir)
            t_aire = lec.get("temperaturaC")
            lo, hi = reglas["compensacion_termica"]["temp_valida_c"]
            if t_aire is not None and t_aire != -1 and lo <= t_aire <= hi:
                self._termico(e, ts, t_aire, reglas, emitir)
        return eventos, e

    # --- reglas -----------------------------------------------------------------------------
    def _llegada(self, e, ts, pct, litros, geom, zona, reglas, emitir):
        r = reglas["llegada_agua"]
        e["ult3"] = (e["ult3"] + [pct])[-r.get("mediana_n", 3):]
        med3 = st.median(e["ult3"])  # mediana móvil: filtra saltos del sensor
        e["buf"] = [p for p in e["buf"] if ts - p[0] <= r["ventana_min"] * 60000] + [[ts, med3]]
        base_ts, base = min(e["buf"], key=lambda p: p[1])
        subida = med3 - base
        # La subida tiene que sostenerse en N medianas seguidas (un llenado real sube de forma continua)
        e["conf"] = e.get("conf", 0) + 1 if subida >= r["subida_min_pct"] else 0
        criterio, umbral = "rapido", r["subida_min_pct"]
        # f4.3: llenado lento. Con baja presión de red (lo común en Juliaca) el nivel sube despacio y nunca llega al
        # umbral de 15 min; se acumula la subida en una ventana larga (una mediana por minuto para acotar el estado)
        lento = r.get("lento")
        if lento:
            larga = [p for p in e.get("buf_largo", []) if ts - p[0] <= lento["ventana_min"] * 60000]
            if larga and larga[-1][0] // 60000 == ts // 60000:
                larga[-1] = [larga[-1][0], med3]
            else:
                larga.append([ts, med3])
            e["buf_largo"] = larga
            # Tendencia robusta: mediana de los primeros 10 min de la ventana contra la de los últimos 5 min
            # (el mínimo de 60 medianas ruidosas es un extremo bajo que infla la "subida")
            inicio = [p[1] for p in larga if p[0] - larga[0][0] <= 10 * 60000]
            base_l_ts, base_l = larga[0][0], st.median(inicio)
            subida_l = (st.median(p[1] for p in larga[-5:]) - base_l
                        if ts - larga[0][0] >= lento.get("ventana_minima_min", 30) * 60000 else 0.0)
            e["conf_largo"] = e.get("conf_largo", 0) + 1 if subida_l >= lento["subida_min_pct"] else 0
            if e["conf"] < r.get("confirmacion_n", 1) and e["conf_largo"] >= lento["confirmacion_n"]:
                criterio, umbral, base_ts, subida = "lento", lento["subida_min_pct"], base_l_ts, subida_l
        if not e["llenando"] and (e["conf"] >= r.get("confirmacion_n", 1) or criterio == "lento"):
            minutos = max(1.0, (ts - base_ts) / 60000)
            cap = geom.get("capacidad_l")
            franja = reglas["zonas"].get(zona)
            _, h = hora_lima(base_ts)
            tol = r["tolerancia_franja_h"]
            fuera = None if not franja else not (franja[0] - tol <= h <= franja[1] + tol)
            emitir("llegada_agua", "advertencia" if fuera else "info", base_ts, ts, subida, umbral, "%",
                   {"criterio": criterio,
                    "caudalLlenadoLpm": round(subida / 100 * cap / minutos, 2) if cap else None,
                    "fueraDeFranja": fuera, "franjaLima": franja, "horaInicioLima": round(h, 2),
                    "nivelPct": round(pct, 1), "litros": None if litros is None else round(litros)})
            e.update(llenando=True, llen_inicio=base_ts, llen_max=med3, llen_ult_subida=ts,
                     ultima_llegada_ts=base_ts, corte_alertado=False)
        elif e["llenando"]:
            if med3 > e["llen_max"] + 0.2:
                e["llen_max"], e["llen_ult_subida"] = med3, ts
            elif ts - e["llen_ult_subida"] > r["fin_sin_subida_min"] * 60000:
                e.update(llenando=False, ultimo_llenado_fin_ts=ts, buf=[[ts, med3]], buf_largo=[[ts, med3]],
                         conf=0, conf_largo=0)

    def _corte(self, e, ts, reglas, emitir):
        r = reglas["corte_prolongado"]
        ref = e["ultima_llegada_ts"] or e["primera_ts"]
        horas = (ts - ref) / 3_600_000
        if horas > r["horas_sin_llegada"] and not e["corte_alertado"] and not e["llenando"]:
            emitir("corte_prolongado", "critica" if horas >= r["critica_desde_h"] else "advertencia", ref, ts,
                   horas, r["horas_sin_llegada"], "h",
                   {"referencia": "ultima_llegada" if e["ultima_llegada_ts"] else "inicio_del_flujo",
                    # Si el sensor estuvo sin datos, el "corte" puede ser un hueco de lecturas y no falta de agua
                    "huecoDeDatosH": round(e.get("hueco_h", 0.0), 2),
                    "posibleHuecoDeDatos": e.get("hueco_h", 0.0) >= 1.0})
            e["corte_alertado"] = True

    def _autonomia(self, e, ts, pct, litros, reglas, emitir):
        r = reglas["autonomia_baja"]
        if pct < r["umbral_pct"] and not e["autonomia_alertada"]:
            emitir("autonomia_baja", "critica" if pct < r["critica_bajo_pct"] else "advertencia", ts, ts, pct,
                   r["umbral_pct"], "%", {"litros": None if litros is None else round(litros),
                                          "horasProyectadas": None})  # la proyección en horas llega en F7
            e["autonomia_alertada"] = True
        elif pct > r["rearme_pct"]:
            e["autonomia_alertada"] = False

    def _fuga(self, e, ts, nivel_cm, dist, compensada, geom, reglas, emitir):
        r = reglas["alerta_fuga"]
        d, h = hora_lima(ts)
        ini_h, fin_h = r["noche_lima_h"]
        noche = e["noche"]
        # La noche termina: se evalúa la ventana acumulada
        if noche and (d.date().isoformat() != noche["fecha"] or h >= fin_h):
            self._evaluar_noche(e, noche, geom, r, emitir)
            e["noche"] = noche = None
        if not ini_h <= h < fin_h:
            return
        fecha = d.date().isoformat()
        if noche is None:
            inicio_ventana = int(datetime(d.year, d.month, d.day, tzinfo=LIMA).timestamp() * 1000 + ini_h * 3_600_000)
            noche = e["noche"] = {"fecha": fecha, "inicio": inicio_ventana, "puntos": [], "min": None, "vals": [],
                                  "n": 0, "n_comp": 0, "llenado": False}
        if e["llenando"]:
            noche["llenado"] = True
        noche["n"] += 1
        noche["n_comp"] += int(bool(compensada))
        minuto = ts // 60000
        if noche["min"] != minuto:  # cierra el minuto anterior con su mediana
            if noche["vals"]:
                noche["puntos"].append([noche["min"] * 60000, st.median(v[0] for v in noche["vals"]),
                                        st.median(v[1] for v in noche["vals"])])
            noche["min"], noche["vals"] = minuto, []
        noche["vals"].append([nivel_cm, dist])

    def _evaluar_noche(self, e, noche, geom, r, emitir):
        if noche["vals"]:
            noche["puntos"].append([noche["min"] * 60000, st.median(v[0] for v in noche["vals"]),
                                    st.median(v[1] for v in noche["vals"])])
        pts = noche["puntos"]
        ini_h, fin_h = r["noche_lima_h"]
        minutos_ventana = (fin_h - ini_h) * 60
        previo = e.get("ultimo_llenado_fin_ts")
        if (len(pts) < 0.6 * minutos_ventana or noche["llenado"] or
                (previo and noche["inicio"] - previo < r["sin_llenado_previo_h"] * 3_600_000)):
            return  # cobertura insuficiente o hubo llenado: la noche no se evalúa
        primeros = [p for p in pts if p[0] - pts[0][0] < 15 * 60000]
        ultimos = [p for p in pts if pts[-1][0] - p[0] < 15 * 60000]
        descenso = st.median(p[1] for p in primeros) - st.median(p[1] for p in ultimos)
        horas = max(0.5, (pts[-1][0] - pts[0][0]) / 3_600_000)
        tasa = descenso / horas
        dist_media = st.fmean(p[2] for p in pts)
        umbral = max(r["descenso_min_cm"], r["factor_incertidumbre"] * r["incertidumbre_dht_frac"] * dist_media)
        if descenso >= umbral and tasa >= e["base_cm_h"] + r["margen_sobre_base_cm_h"]:
            cap, alto = geom.get("capacidad_l"), geom.get("altura_interior_cm")
            emitir("alerta_fuga", "critica" if tasa >= 2 * (e["base_cm_h"] + r["margen_sobre_base_cm_h"]) else "advertencia",
                   noche["inicio"], pts[-1][0], descenso, umbral, "cm",
                   {"tasaCmH": round(tasa, 3), "baseCmH": round(e["base_cm_h"], 3),
                    "litrosPerdidos": round(descenso / alto * cap, 1) if cap and alto else None,
                    "fraccionCompensada": round(noche["n_comp"] / noche["n"], 3) if noche["n"] else 0.0,
                    "minutosConDatos": len(pts)})
        else:
            e["base_cm_h"] = (1 - r["base_alfa"]) * e["base_cm_h"] + r["base_alfa"] * max(0.0, tasa)

    def _termico(self, e, ts, t_aire, reglas, emitir):
        r = reglas["umbral_termico"]
        tau = r["tau_h"]
        if e["tw"] is None:
            e["tw"], e["tw_ts"] = t_aire, ts  # arranque: el agua empieza a la temperatura del aire
        else:
            dt_h = (ts - e["tw_ts"]) / 3_600_000
            e["tw"] += (t_aire - e["tw"]) * (1 - math.exp(-dt_h / tau))
            e["tw_ts"] = ts
        tw = e["tw"]
        bajo, alto, hist = r["umbral_bajo_c"], r["umbral_alto_c"], r["histeresis_c"]
        if e["termico_alertado"] == "bajo" and tw > bajo + hist:
            e["termico_alertado"] = None
        if e["termico_alertado"] == "alto" and tw < alto - hist:
            e["termico_alertado"] = None
        if ts - e["termico_eval_ts"] < 10 * 60000 or e["termico_alertado"]:
            return
        e["termico_eval_ts"] = ts
        # Proyección: el aire sigue el ciclo horario de Juliaca con la anomalía actual; el agua lo sigue con tau
        ciclo = r["ciclo_aire_c"]
        _, h = hora_lima(ts)
        interp = lambda x: ciclo[int(x) % 24] + (ciclo[(int(x) + 1) % 24] - ciclo[int(x) % 24]) * (x - int(x))  # noqa: E731
        anomalia = t_aire - interp(h)
        tw_f = tw
        for paso in range(1, int(r["horizonte_h"]) + 1):
            tw_f += (interp(h + paso) + anomalia - tw_f) * (1 - math.exp(-1 / tau))
            cruza = ("bajo" if tw > bajo >= tw_f else "alto" if tw < alto <= tw_f else None)
            if cruza:
                emitir("umbral_termico", "advertencia", ts, ts, tw_f, bajo if cruza == "bajo" else alto, "°C",
                       {"tempAguaEstimadaC": round(tw, 2), "tempAireC": t_aire, "horaCruceTs": ts + paso * 3_600_000,
                        "direccion": cruza, "modelo": f"primer orden, tau {tau} h"}, version=r["version_regla"])
                e["termico_alertado"] = cruza
                return
