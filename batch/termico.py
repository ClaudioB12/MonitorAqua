"""
Modelo térmico de D8, sin dependencias fuera de numpy (lo carga F7 igual que batch/fuga.py y batch/llegadas.py).

  Inercia de primer orden, paso de 1 h:  F(t) = a·F(t-1) + (1-a)·u(t),  a = exp(-1/τ)
  Agua estimada:  w(t) = Σ_k g_k · F_τ(u_k)(t) + c   (u_k: aire de la cámara o del tanque, aire de Open-Meteo)
  Nunca usa una medición del agua: en Juliaca no hay sensor de agua, así que el agua se SIMULA desde el aire.
  Pronóstico a h horas: se siguen los filtros con el aire futuro = ciclo horario de los últimos 7 días + la anomalía
  actual (la misma idea que la regla provisional umbral_termico). Banda P10-P90 por predicción conforme, por horizonte.
"""
import math

import numpy as np


def filtrar(u, tau, f0=None):
    a = math.exp(-1.0 / tau)
    u = np.asarray(u, float)
    f = np.empty(len(u))
    prev = u[0] if f0 is None else f0
    for i, x in enumerate(u):
        prev = a * prev + (1 - a) * x
        f[i] = prev
    return f


def ciclo_mas_anomalia(u, hora, i, h):
    """Aire futuro u(i+1..i+h): ciclo horario de las últimas 168 h (hasta i) + la anomalía actual, constante."""
    ini = max(0, i - 167)
    seg, hs = u[ini:i + 1], hora[ini:i + 1]
    ciclo = {k: float(np.nanmean(seg[hs == k])) for k in np.unique(hs)}
    base = ciclo.get(hora[i], u[i])
    anom = u[i] - base
    futuro = []
    for j in range(1, h + 1):
        hj = (hora[i] + j) % 24
        futuro.append(ciclo.get(hj, base) + anom)
    return np.array(futuro)


def cuantil_conforme(residuos, q):
    r = np.sort(np.asarray(residuos, float)[~np.isnan(residuos)])
    n = len(r)
    if n == 0:
        return None
    k = math.ceil((n + 1) * q) - 1 if q >= 0.5 else math.floor((n + 1) * q) - 1
    return float(r[min(max(k, 0), n - 1)])


class ModeloTermico:
    def __init__(self, entradas, tau, ganancias, constante, conforme=None, nombre=""):
        self.entradas, self.tau, self.constante = list(entradas), float(tau), float(constante)
        self.ganancias = [float(g) for g in ganancias]
        self.conforme = conforme or {}
        self.nombre = nombre

    @staticmethod
    def ajustar(U, w, mascara, entradas, taus, nombre=""):
        """U: {entrada: serie horaria continua}; w: agua medida (NaN si no hay); mascara: filas para ajustar.
        Para cada τ, ganancias y constante por mínimos cuadrados; gana el τ con menor error cuadrático."""
        ok = mascara & ~np.isnan(w)
        mejor = None
        for tau in taus:
            F = np.column_stack([filtrar(U[e], tau) for e in entradas] + [np.ones(len(w))])
            coef, *_ = np.linalg.lstsq(F[ok], w[ok], rcond=None)
            mse = float(np.mean((F[ok] @ coef - w[ok]) ** 2))
            if mejor is None or mse < mejor[0]:
                mejor = (mse, tau, coef)
        _, tau, coef = mejor
        return ModeloTermico(entradas, tau, coef[:-1], coef[-1], nombre=nombre)

    def estados(self, U):
        return {e: filtrar(U[e], self.tau) for e in self.entradas}

    def estimar(self, U):
        F = self.estados(U)
        return sum(g * F[e] for g, e in zip(self.ganancias, self.entradas)) + self.constante

    def pronosticar(self, U, hora, i, h, estados=None):
        """Agua en i+1..i+h desde el estado de los filtros en i, con el aire futuro = ciclo + anomalía."""
        F = estados or self.estados(U)
        a = math.exp(-1.0 / self.tau)
        out = np.full(h, self.constante)
        for g, e in zip(self.ganancias, self.entradas):
            f, fut = F[e][i], ciclo_mas_anomalia(np.asarray(U[e], float), hora, i, h)
            for j in range(h):
                f = a * f + (1 - a) * fut[j]
                out[j] += g * f
        return out

    def calibrar(self, residuos_por_h, alfa=0.2, min_n=24):
        for h, r in residuos_por_h.items():
            r = np.asarray(r, float)
            r = r[~np.isnan(r)]
            self.conforme[str(h)] = ({"n": int(len(r)), "q_bajo": cuantil_conforme(r, alfa / 2),
                                      "q_alto": cuantil_conforme(r, 1 - alfa / 2), "alfa": alfa}
                                     if len(r) >= min_n else {"n": int(len(r)), "q_bajo": None, "q_alto": None})

    def banda(self, pred, h):
        c = self.conforme.get(str(h), {})
        if c.get("q_bajo") is None:
            return None, None
        return pred + c["q_bajo"], pred + c["q_alto"]

    def a_dict(self):
        return {"nombre": self.nombre, "entradas": self.entradas, "tau_h": self.tau, "ganancias": self.ganancias,
                "constante": self.constante, "conforme": self.conforme}

    @classmethod
    def desde_dict(cls, d):
        return cls(d["entradas"], d["tau_h"], d["ganancias"], d["constante"], d.get("conforme"), d.get("nombre", ""))
