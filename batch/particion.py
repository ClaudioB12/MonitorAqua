"""
Partición de datos común a todos los modelos (F6), leída de batch/particion_modelos.yml. Python puro: la usan
batch/capas.py (columna "particion" de las tablas oro) y batch/evaluacion.py.

  asignar(fuente, sensorId, runId, ts_ms) -> "entrenamiento" | "embargo" | "prueba" | "consistencia"

  esp32       entrenamiento antes de max(congelamiento de F4, geometria_desde); prueba desde ahí. Mientras la
              geometría sea provisional no hay prueba: todo es entrenamiento (conjunto de diseño).
  thingspeak  por canal: prueba = últimos N días de ese canal; embargo de 24 h antes de la prueba.
  aumentados  por corrida: entrenamiento = primeros 21 días; embargo 24 h; el resto es "consistencia"
              (nunca métrica final).
"""
from datetime import datetime
from pathlib import Path

import yaml

RAIZ = Path(__file__).resolve().parents[1]
H = 3_600_000


def ms(texto):
    return int(datetime.fromisoformat(str(texto)).timestamp() * 1000)


class Particion:
    def __init__(self, ruta=None, tanques=None, limites_thingspeak=None, inicio_corridas=None):
        """limites_thingspeak: {sensorId: ts_ms máximo del canal}; inicio_corridas: {runId: ts_ms mínimo}."""
        self.cfg = yaml.safe_load(Path(ruta or RAIZ / "batch" / "particion_modelos.yml").read_text(encoding="utf-8"))
        tq = yaml.safe_load(Path(tanques or RAIZ / "ingesta" / "tanques.yml").read_text(encoding="utf-8"))["tanques"]
        e = self.cfg["esp32"]
        self.esp32_sensores = set(e["sensores"])
        self.inicio_prueba_esp32 = {}
        for sid in self.esp32_sensores:
            g = tq.get(sid, {})
            if g.get("por_medir") or not g.get("geometria_desde"):
                self.inicio_prueba_esp32[sid] = None  # sin geometría real: no hay periodo de prueba
            else:
                self.inicio_prueba_esp32[sid] = max(ms(e["congelamiento_reglas"]), ms(g["geometria_desde"]))
        self.limites_ts = limites_thingspeak or {}
        self.inicio_corridas = inicio_corridas or {}

    def version(self):
        return self.cfg["version"]

    def asignar(self, fuente, sensor_id, run_id, ts):
        if fuente.startswith("aumentado"):
            a = self.cfg["aumentados"]
            ini = self.inicio_corridas.get(run_id)
            if ini is None:
                return "consistencia"
            corte = ini + a["entrenamiento_dias"] * 24 * H
            if ts < corte:
                return "entrenamiento"
            return "embargo" if ts < corte + a["embargo_h"] * H else "consistencia"
        if fuente == "thingspeak":
            t = self.cfg["thingspeak"]
            fin = self.limites_ts.get(sensor_id)
            if fin is None:
                return "entrenamiento"
            inicio_prueba = fin - t["prueba_ultimos_dias"] * 24 * H
            if ts >= inicio_prueba:
                return "prueba"
            return "embargo" if ts >= inicio_prueba - t["embargo_h"] * H else "entrenamiento"
        if sensor_id in self.esp32_sensores:
            ini = self.inicio_prueba_esp32.get(sensor_id)
            return "prueba" if ini is not None and ts >= ini else "entrenamiento"
        return "sin_uso"  # fuentes sin dimensión propia (EA, USGS, Open-Meteo como variable externa)

    def resumen(self):
        return {"version": self.version(),
                "esp32_inicio_prueba_ms": self.inicio_prueba_esp32,
                "thingspeak_fin_ms": self.limites_ts, "aumentados_inicio_ms": self.inicio_corridas}
