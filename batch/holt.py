"""
Suavizado exponencial de Holt (nivel + tendencia) para D2, sin dependencias: el mismo código sirve para entrenar en
batch (batch/modelo_d2.py) y para actualizar en streaming en F7, una lectura a la vez.

Se alimenta con el nivel en % de cada bloque de 5 min (la mediana del bloque, como la tabla oro d2_autonomia).
Si faltan bloques, el siguiente se trata como dt pasos más tarde: la predicción avanza con la tendencia y no se
interpola nada. Un hueco mayor que el umbral de tramo (oro.GAP_MS) reinicia el estado.

  h = Holt(alpha, beta)
  h.actualizar(ts_ms, nivel_pct)
  h.pronostico(minutos)              -> nivel % esperado dentro de N minutos
  h.horas_autonomia(umbral_pct)      -> horas hasta llegar al umbral según la tendencia (inf si no baja)
"""
import math

PASO_MS = 300_000  # 5 min, el paso de d2_autonomia
PASOS_POR_HORA = 3_600_000 // PASO_MS
TENDENCIA_MIN_PCT_H = 0.05  # bajo esto (en %/h) se considera que el nivel no baja: autonomía infinita


class Holt:
    def __init__(self, alpha, beta, hueco_max_ms=None):
        self.alpha, self.beta = alpha, beta
        self.hueco_max_ms = hueco_max_ms
        self.nivel = self.tendencia = self.ts = None  # tendencia en % por paso de 5 min

    def reiniciar(self):
        self.nivel = self.tendencia = self.ts = None

    def actualizar(self, ts_ms, y):
        if y is None or (isinstance(y, float) and math.isnan(y)):
            return
        if self.nivel is None or (self.hueco_max_ms and ts_ms - self.ts > self.hueco_max_ms):
            self.nivel, self.tendencia, self.ts = float(y), 0.0, ts_ms
            return
        dt = max(1.0, (ts_ms - self.ts) / PASO_MS)
        previsto = self.nivel + self.tendencia * dt
        nuevo = self.alpha * y + (1 - self.alpha) * previsto
        self.tendencia = self.beta * (nuevo - self.nivel) / dt + (1 - self.beta) * self.tendencia
        self.nivel, self.ts = nuevo, ts_ms

    def tendencia_pct_h(self):
        return None if self.tendencia is None else self.tendencia * PASOS_POR_HORA

    def pronostico(self, minutos):
        if self.nivel is None:
            return None
        return self.nivel + self.tendencia * (minutos * 60_000 / PASO_MS)

    def horas_autonomia(self, umbral_pct):
        return horas_hasta(self.nivel, self.tendencia_pct_h(), umbral_pct)

    def estado(self):
        """Estado serializable (para guardarlo entre micro-lotes en F7)."""
        return {"nivel": self.nivel, "tendencia": self.tendencia, "ts": self.ts}

    @classmethod
    def desde_estado(cls, alpha, beta, estado, hueco_max_ms=None):
        h = cls(alpha, beta, hueco_max_ms)
        h.nivel, h.tendencia, h.ts = estado.get("nivel"), estado.get("tendencia"), estado.get("ts")
        return h


def horas_hasta(nivel, pendiente_pct_h, umbral_pct):
    """Horas hasta que el nivel llegue al umbral bajando a la pendiente dada (%/h). 0 si ya está en o bajo el umbral;
    inf si la pendiente no es un descenso claro."""
    if nivel is None or pendiente_pct_h is None or math.isnan(nivel) or math.isnan(pendiente_pct_h):
        return float("nan")
    if nivel <= umbral_pct:
        return 0.0
    if pendiente_pct_h > -TENDENCIA_MIN_PCT_H:
        return float("inf")
    return (nivel - umbral_pct) / -pendiente_pct_h
