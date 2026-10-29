"""
Perfil de consumo por hora del día (y por día de la semana si hay datos) para D2 d2.2. Python puro + numpy: se aprende
en batch (batch/modelo_d2_perfil.py) y se usa igual en el streaming de F7.

  Aprender   de la historia de UN tanque (solo su partición de entrenamiento): en cada bloque de una hora de reloj
             (Lima) sin llenado, consumo = lo que bajó el nivel por hora (%/h, negativo -> 0). Por celda
             (día de semana, hora) se guardan P10, P50, P90 y la media. Si una celda tiene menos de min_muestras, o el
             tanque menos de min_dias_semana días de historia, se usa la celda de esa hora sin distinguir el día;
             si tampoco alcanza, el consumo global del tanque.
  Proyectar  desde un nivel y una hora, se resta el consumo esperado de cada paso de 5 min hasta llegar al umbral:
             horas de autonomía (P50) y la banda P90 (pesimista: se gasta más) - P10 (optimista).

  p = PerfilConsumo.desde_dict(json)          # el que guarda modelo_d2_perfil.py en batch/modelos/d2/<version>/
  p.horas_autonomia(nivel_pct, ts_ms, umbral, cuantil="p50")
  p.pronostico(nivel_pct, ts_ms, minutos)
"""
import numpy as np

LIMA_MS = -5 * 3_600_000
PASO_MS = 300_000
PASOS_DIA = 288
PASOS_SEMANA = 7 * PASOS_DIA
CUANTILES = ("p10", "p50", "p90", "media")


def paso_semana(ts_ms):
    """Índice del paso de 5 min dentro de la semana, desde el lunes 00:00 de Lima (el 1-ene-1970 fue jueves)."""
    t = np.asarray(ts_ms, dtype=np.int64) + LIMA_MS
    dias = t // 86_400_000
    return ((dias + 3) % 7) * PASOS_DIA + (t % 86_400_000) // PASO_MS


class PerfilConsumo:
    def __init__(self, tabla, nivel_detalle, tope_horas=72):
        """tabla: {cuantil: matriz 7x24 de consumo en %/h}."""
        self.tabla = {q: np.asarray(m, float) for q, m in tabla.items()}
        self.nivel_detalle = nivel_detalle
        self.tope_horas = tope_horas
        self._cum = {}
        for q, m in self.tabla.items():
            por_paso = np.repeat(m.reshape(-1), 12) / 12.0           # %/paso de 5 min, 2016 pasos por semana
            self._cum[q] = np.concatenate([[0.0], np.cumsum(np.tile(por_paso, 2))])

    # --- aprender ---------------------------------------------------------------------------------------------
    @classmethod
    def aprender(cls, dia_semana, hora, consumo, dias_historia, min_muestras=3, min_dias_semana=14, tope_horas=72):
        """Una entrada por bloque de una hora sin llenado del periodo de entrenamiento: dia_semana (0 = lunes),
        hora (0-23, Lima) y consumo (%/h >= 0). dias_historia: días que cubre ese entrenamiento."""
        d = np.asarray(dia_semana, int)
        h = np.asarray(hora, int)
        c = np.asarray(consumo, float)
        dias = int(dias_historia)
        por_dia = dias >= min_dias_semana
        tabla = {q: np.zeros((7, 24)) for q in CUANTILES}
        conteo = {"semana_hora": 0, "hora": 0, "global": 0}

        def stats(x):
            return {"p10": np.percentile(x, 10), "p50": np.percentile(x, 50), "p90": np.percentile(x, 90),
                    "media": float(np.mean(x))}
        glob = stats(c) if len(c) else {q: 0.0 for q in CUANTILES}
        for dia in range(7):
            for hora in range(24):
                celda = c[(d == dia) & (h == hora)]
                if por_dia and len(celda) >= min_muestras:
                    s, nivel = stats(celda), "semana_hora"
                else:
                    x = c[h == hora]
                    s, nivel = (stats(x), "hora") if len(x) >= min_muestras else (glob, "global")
                conteo[nivel] += 1
                for q in CUANTILES:
                    tabla[q][dia, hora] = s[q]
        return cls(tabla, {"celdas": conteo, "dias_historia": dias, "bloques": int(len(c)),
                           "por_dia_semana": por_dia}, tope_horas)

    # --- usar ---------------------------------------------------------------------------------------------------
    def horas_autonomia(self, nivel, ts_ms, umbral, cuantil="p50"):
        """Vectorizado: horas hasta el umbral restando el consumo esperado de cada paso; tope si no llega antes."""
        nivel = np.asarray(nivel, float)
        s = paso_semana(ts_ms)
        cum = self._cum[cuantil]
        falta = nivel - umbral
        objetivo = cum[s] + np.maximum(falta, 0)
        j = np.searchsorted(cum, objetivo, side="left")
        horas = (j - s) / 12.0
        horas = np.where(falta <= 0, 0.0, horas)
        return np.minimum(np.where(np.isnan(nivel), np.nan, horas), self.tope_horas)

    def pronostico(self, nivel, ts_ms, minutos, cuantil="p50"):
        s = paso_semana(ts_ms)
        cum = self._cum[cuantil]
        return np.asarray(nivel, float) - (cum[s + minutos // 5] - cum[s])

    def consumo_medio_h(self):
        return float(self.tabla["media"].mean())

    def a_dict(self):
        return {"tabla_pct_h": {q: np.round(m, 4).tolist() for q, m in self.tabla.items()},
                "detalle": self.nivel_detalle, "tope_horas": self.tope_horas}

    @classmethod
    def desde_dict(cls, d):
        return cls(d["tabla_pct_h"], d.get("detalle"), d.get("tope_horas", 72))
