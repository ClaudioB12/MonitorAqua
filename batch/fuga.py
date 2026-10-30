"""
Modelo de fuga de D6, sin dependencias fuera de numpy (lo carga F7 igual que batch/llegadas.py en D4).

  tasa_robusta      pendiente de Theil-Sen (mediana de las pendientes entre pares) de las medianas por minuto de la
                    distancia en la franja nocturna (cm/h; > 0 = el agua baja). Un eco suelto o un escalón de un
                    sensor intermitente no la arrastran, a diferencia de "últimos 15 min - primeros 15 min".
  referencia        tasa habitual del tanque: mediana de sus noches anteriores válidas (consumo nocturno normal).
  exceso            tasa - referencia, en L/h con la geometría del tanque: lo que la fuga añade al consumo normal.
  ModeloFuga        regresión logística (IRLS con L2) que da P(fuga esta noche) a partir del exceso, su
                    estabilidad entre noches y las temperaturas. Proyección de la pérdida a H horas
                    = P(fuga) · max(exceso, 0) · H · f_H, donde f_H (factor de persistencia) es la fracción de esa
                    pérdida que de verdad ocurrió en el conjunto de ajuste: una fuga no dura para siempre (se repara
                    o el flotador se asienta). Banda P10-P90 por predicción conforme de Mondrian: cuantiles de los
                    residuos por separado para las noches que el modelo marca como fuga y para las demás; con una sola
                    banda para todas, los residuos de las noches normales (casi todos 0) la dejarían de ancho 0.
"""
import math

import numpy as np


def theil_sen(t_h, y):
    """Pendiente (unidades de y por hora) y residuos absolutos medianos. t_h en horas."""
    t_h, y = np.asarray(t_h, float), np.asarray(y, float)
    ok = ~(np.isnan(t_h) | np.isnan(y))
    t_h, y = t_h[ok], y[ok]
    if len(y) < 3:
        return None, None
    i, j = np.triu_indices(len(y), 1)
    dt = t_h[j] - t_h[i]
    m = dt > 0
    if not m.any():
        return None, None
    pend = float(np.median((y[j] - y[i])[m] / dt[m]))
    ordenada = float(np.median(y - pend * t_h))
    mad = float(np.median(np.abs(y - (ordenada + pend * t_h))))
    return pend, mad


def sigmoide(z):
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def logistica(X, y, l2=1.0, iteraciones=50, pesos=None):
    """Regresión logística por IRLS con penalización L2 (sin penalizar el intercepto). Devuelve los coeficientes con el
    intercepto primero. pesos (opcional) equilibra las clases."""
    X = np.column_stack([np.ones(len(X)), np.asarray(X, float)])
    y = np.asarray(y, float)
    w = np.ones(len(y)) if pesos is None else np.asarray(pesos, float)
    beta = np.zeros(X.shape[1])
    pen = np.full(X.shape[1], l2)
    pen[0] = 0.0
    for _ in range(iteraciones):
        p = sigmoide(X @ beta)
        s = w * p * (1 - p) + 1e-9
        grad = X.T @ (w * (y - p)) - pen * beta
        hess = (X * s[:, None]).T @ X + np.diag(pen)
        paso = np.linalg.solve(hess, grad)
        beta += paso
        if np.max(np.abs(paso)) < 1e-8:
            break
    return beta


def cuantil_conforme(residuos, q):
    """Cuantil con la corrección de muestra finita de la predicción conforme partida."""
    r = np.sort(np.asarray(residuos, float))
    n = len(r)
    if n == 0:
        return None
    k = math.ceil((n + 1) * q) - 1 if q >= 0.5 else math.floor((n + 1) * q) - 1
    return float(r[min(max(k, 0), n - 1)])


def elegir_umbral(p, y, opciones=np.arange(0.10, 0.91, 0.05)):
    """Umbral de P(fuga) con el mayor F1 (en el conjunto de AJUSTE, nunca en la evaluación)."""
    y = np.asarray(y, bool)
    mejor, f1_mejor = 0.5, -1.0
    for u in opciones:
        m = np.asarray(p) >= u
        vp = (m & y).sum()
        f1 = 2 * vp / (m.sum() + y.sum()) if m.sum() + y.sum() else 0.0
        if f1 > f1_mejor + 1e-12:
            mejor, f1_mejor = float(round(u, 2)), float(f1)
    return mejor, f1_mejor


class ModeloFuga:
    def __init__(self, variables, media, desv, beta, umbral, conforme, horizontes_h, factor_h=None):
        self.variables, self.media, self.desv = list(variables), np.asarray(media), np.asarray(desv)
        self.beta, self.umbral, self.conforme, self.horizontes_h = np.asarray(beta), umbral, conforme, horizontes_h
        self.factor_h = factor_h or {}

    @classmethod
    def ajustar(cls, X, y, variables, l2=1.0, horizontes_h=(24, 168)):
        X = np.asarray(X, float)
        media, desv = X.mean(0), X.std(0)
        desv[desv == 0] = 1.0
        y = np.asarray(y, float)
        pos = y.mean()
        pesos = np.where(y == 1, 0.5 / pos, 0.5 / (1 - pos)) if 0 < pos < 1 else None  # clases equilibradas
        beta = logistica((X - media) / desv, y, l2=l2, pesos=pesos)
        return cls(variables, media, desv, beta, 0.5, {}, list(horizontes_h))

    def probabilidad(self, X):
        Z = (np.asarray(X, float) - self.media) / self.desv
        return sigmoide(np.column_stack([np.ones(len(Z)), Z]) @ self.beta)

    def perdida_esperada(self, p, exceso_lph, h):
        return np.asarray(p) * np.maximum(np.asarray(exceso_lph, float), 0.0) * h * self.factor_h.get(str(h), 1.0)

    def calibrar(self, p, exceso_lph, verdad_por_h, alfa=0.2, min_n=10):
        """verdad_por_h: {H: pérdida real (L) en las H horas siguientes}, en el conjunto de AJUSTE. Primero el factor
        de persistencia (con las noches marcadas como fuga), después la banda conforme por clase predicha."""
        p = np.asarray(p)
        marca = p >= self.umbral
        for h, verdad in verdad_por_h.items():
            verdad = np.asarray(verdad, float)
            ok = ~np.isnan(verdad)
            self.factor_h[str(h)] = 1.0
            bruta = self.perdida_esperada(p, exceso_lph, h)
            den = bruta[ok & marca].sum()
            self.factor_h[str(h)] = float(verdad[ok & marca].sum() / den) if den > 0 else 1.0
            res = verdad - self.perdida_esperada(p, exceso_lph, h)
            self.conforme[str(h)] = {"alfa": alfa}
            for clase, m in (("fuga", marca), ("normal", ~marca)):
                r = res[ok & m]
                self.conforme[str(h)][clase] = ({"n": int(len(r)), "q_bajo": cuantil_conforme(r, alfa / 2),
                                                 "q_alto": cuantil_conforme(r, 1 - alfa / 2)}
                                                if len(r) >= min_n else {"n": int(len(r)), "q_bajo": None, "q_alto": None})

    def proyectar(self, X, exceso_lph):
        p = self.probabilidad(X)
        marca = p >= self.umbral
        out = {"p_fuga": p, "fuga": marca}
        for h in self.horizontes_h:
            pred = self.perdida_esperada(p, exceso_lph, h)
            out[f"perdida_{h}h_l"] = pred
            c = self.conforme.get(str(h), {})
            if all(c.get(k, {}).get("q_bajo") is not None for k in ("fuga", "normal")):
                qb = np.where(marca, c["fuga"]["q_bajo"], c["normal"]["q_bajo"])
                qa = np.where(marca, c["fuga"]["q_alto"], c["normal"]["q_alto"])
                out[f"p10_{h}h_l"] = np.maximum(0.0, pred + qb)
                out[f"p90_{h}h_l"] = np.maximum(0.0, pred + qa)
        return out

    def a_dict(self):
        return {"tipo": "logistica_irls_l2", "variables": self.variables, "media": self.media.tolist(),
                "desv": self.desv.tolist(), "beta": self.beta.tolist(), "umbral": self.umbral,
                "conforme": self.conforme, "horizontes_h": self.horizontes_h, "factor_h": self.factor_h}

    @classmethod
    def desde_dict(cls, d):
        return cls(d["variables"], d["media"], d["desv"], d["beta"], d["umbral"], d["conforme"], d["horizontes_h"],
                   d.get("factor_h"))
