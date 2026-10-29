"""
Modelo de supervivencia de la próxima llegada de agua para D4. Python puro + numpy: se ajusta en batch
(batch/modelo_d4.py) y se usa igual en el streaming de F7.

Tiempo discreto, en horas de reloj. En cada hora en la que el tanque está "en riesgo" (se conoce cuánto pasó desde la
última llegada), la probabilidad de que llegue agua en esa hora es

    h(e, hora) = 1 - exp(-A[e] * B[hora])

  A[e]     riesgo según las horas transcurridas desde la última llegada (e = 0, 1, ...; el último cubo agrupa la cola).
           "km": tabla de vida (Kaplan-Meier en tiempo discreto) estimada de los datos.
           "weibull": A[e] = H(e+1) - H(e) con H la hazard acumulada de una Weibull ajustada a los intervalos entre
           llegadas (con censura), si hay suficientes intervalos.
  B[hora]  cuánto más (o menos) probable es una llegada a esa hora del día (media ponderada 1).
  Ajuste   ajuste proporcional iterativo (IPF, modelo de Poisson con dos factores) sobre las horas en riesgo del
           entrenamiento: llegadas(e, hora) ~ exposición(e, hora) * A[e] * B[hora].

De la distribución de la próxima llegada salen la hora más probable (moda), la mediana, P10-P90 y la probabilidad de
que llegue agua en las próximas H horas.

  mod = ModeloLlegadas.desde_dict(json)
  r = mod.predecir(horas_desde_ultima, hora_lima_actual)   # dict con moda_h, p10_h, p50_h, p90_h, prob_6h, ...
"""
import math

import numpy as np


class ModeloLlegadas:
    def __init__(self, A, B, tipo, horizonte_h, detalle=None):
        self.A = np.asarray(A, float)
        self.B = np.asarray(B, float)
        self.tipo = tipo
        self.horizonte_h = int(horizonte_h)
        self.detalle = detalle or {}

    # --- ajuste ---------------------------------------------------------------------------------------------------
    @staticmethod
    def _tablas(e, hora, evento, e_max):
        e = np.minimum(np.asarray(e, int), e_max)
        hora = np.asarray(hora, int) % 24
        ev = np.asarray(evento, bool)
        E = np.zeros((e_max + 1, 24))
        D = np.zeros((e_max + 1, 24))
        np.add.at(E, (e, hora), 1)
        np.add.at(D, (e, hora), ev)
        return E, D

    @staticmethod
    def _normalizar_B(B, E, A):
        peso = (E * A[:, None]).sum(0)
        media = (B * peso).sum() / peso.sum() if peso.sum() else 1.0
        return B / media if media > 0 else np.ones(24)

    @classmethod
    def ajustar_km(cls, e, hora, evento, e_max=72, iteraciones=100, horizonte_h=96):
        E, D = cls._tablas(e, hora, evento, e_max)
        A, B = np.full(e_max + 1, 0.01), np.ones(24)
        for _ in range(iteraciones):
            den_a = (E * B[None, :]).sum(1)
            A = np.where(den_a > 0, D.sum(1) / np.where(den_a > 0, den_a, 1), 0.0)
            den_b = (E * A[:, None]).sum(0)
            B = np.where(den_b > 0, D.sum(0) / np.where(den_b > 0, den_b, 1), 0.0)
            B = cls._normalizar_B(B, E, A)
        # Cubos de e sin exposición: se usa el riesgo de la cola (último cubo con datos)
        con_datos = np.flatnonzero(E.sum(1) > 0)
        if len(con_datos):
            A[E.sum(1) == 0] = A[con_datos[-1]] if con_datos[-1] < len(A) else 0.0
        return cls(A, B, "km", horizonte_h, {"llegadas": int(D.sum()), "horas_en_riesgo": int(E.sum()),
                                              "e_max": e_max})

    @staticmethod
    def ajustar_weibull_intervalos(t, evento):
        """MLE de Weibull con censura a derecha (t en horas > 0). Para k fijo, lambda tiene forma cerrada."""
        t = np.maximum(np.asarray(t, float), 0.05)
        d = np.asarray(evento, bool)
        if d.sum() == 0:
            return None
        logt = np.log(t)

        def ll(k):
            lam_k = (t ** k).sum() / d.sum()
            lam = lam_k ** (1 / k)
            return d.sum() * math.log(k) - d.sum() * k * math.log(lam) + (k - 1) * logt[d].sum() - d.sum(), lam
        ks = np.exp(np.linspace(math.log(0.2), math.log(15), 200))
        mejores = [(ll(k)[0], k) for k in ks]
        k = max(mejores)[1]
        for paso in (0.05, 0.01, 0.002):  # refina
            cand = [k * (1 + paso * i) for i in range(-10, 11)]
            k = max((ll(c)[0], c) for c in cand if c > 0)[1]
        return {"k": float(k), "lambda_h": float(ll(k)[1]), "intervalos": int(len(t)), "eventos": int(d.sum())}

    @classmethod
    def ajustar_weibull(cls, e, hora, evento, intervalos_t, intervalos_evento, e_max=72, iteraciones=50,
                        horizonte_h=96):
        w = cls.ajustar_weibull_intervalos(intervalos_t, intervalos_evento)
        if w is None:
            return None
        E, D = cls._tablas(e, hora, evento, e_max)
        idx = np.arange(e_max + 1, dtype=float)
        Hc = lambda x: (x / w["lambda_h"]) ** w["k"]  # noqa: E731
        A = Hc(idx + 1) - Hc(idx)
        A[-1] = A[-2] if len(A) > 1 else A[-1]  # la cola sigue con el riesgo del último cubo
        B = np.ones(24)
        for _ in range(iteraciones):
            den_b = (E * A[:, None]).sum(0)
            B = np.where(den_b > 0, D.sum(0) / np.where(den_b > 0, den_b, 1), 0.0)
            B = cls._normalizar_B(B, E, A)
        return cls(A, B, "weibull", horizonte_h, {"weibull": w, "llegadas": int(D.sum()),
                                                  "horas_en_riesgo": int(E.sum()), "e_max": e_max})

    # --- predecir -------------------------------------------------------------------------------------------------
    def distribucion(self, e0, hora0):
        """f[k] = probabilidad de que la próxima llegada caiga en la hora k (0 = la hora en curso), k < horizonte."""
        k = np.arange(self.horizonte_h)
        e = np.minimum(int(e0) + k, len(self.A) - 1)
        tasa = self.A[e] * self.B[(int(hora0) + k) % 24]
        p = 1 - np.exp(-tasa)
        S = np.concatenate([[1.0], np.cumprod(1 - p)])
        return S[:-1] * p, S[-1]

    def predecir(self, e0, hora0, horizontes_prob=(6, 12, 24)):
        f, resto = self.distribucion(e0, hora0)
        F = np.cumsum(f)

        def cuantil(q):  # en horas desde el origen; si no se alcanza en el horizonte, el horizonte
            i = np.searchsorted(F, q)
            return float(i + 0.5) if i < len(F) else float(self.horizonte_h)
        out = {"moda_h": float(np.argmax(f) + 0.5) if f.max() > 0 else float(self.horizonte_h),
               "p10_h": cuantil(0.10), "p50_h": cuantil(0.50), "p90_h": cuantil(0.90),
               "prob_sin_llegada_en_horizonte": float(resto)}
        for H in horizontes_prob:
            out[f"prob_{H}h"] = float(F[min(H, len(F)) - 1])
        return out

    def a_dict(self):
        return {"tipo": self.tipo, "A": np.round(self.A, 6).tolist(), "B": np.round(self.B, 4).tolist(),
                "horizonte_h": self.horizonte_h, "detalle": self.detalle}

    @classmethod
    def desde_dict(cls, d):
        return cls(d["A"], d["B"], d["tipo"], d["horizonte_h"], d.get("detalle"))
