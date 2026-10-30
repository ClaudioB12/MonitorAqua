"""
Evidencia de las dos verificaciones en vivo de F7 que necesitan tiempo (pendientes del 2026-10-30):

  D8  pronósticos a 6 y 12 h del agua de ThingSpeak 3418755 frente al agua real (DS18B20), una vez cumplidos los
      instantes objetivo (variante validada: aire de la cámara + Open-Meteo de su ubicación)
  D4  error de la hora de llegada en una llegada real observada en el stream

Consulta InfluxDB por la API de Grafana (mismas credenciales que infra/verificar_tableros.py) y guarda
docs/evidencias/f7/verificacion_en_vivo_<fecha>.json con los puntos y su resumen.

  .venv/Scripts/python infra/evidencia_f7_pendiente.py [--horas 48]
"""
import argparse
import json
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import verificar_tableros as vt

RAIZ = Path(__file__).resolve().parents[1]
CONSULTAS = {
    "D8_agua_real_vs_pronosticada": '''
from(bucket: "monitoraqua")
  |> range(start: -{h}h)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "verificacion")
  |> filter(fn: (r) => r._field == "agua_real_c" or r._field == "agua_pred_c" or r._field == "error_c" or r._field == "horizonte_horas")
  |> pivot(rowKey: ["_time", "sensorId", "version_modelo"], columnKey: ["_field"], valueColumn: "_value")
  |> group()''',
    "D8_estimaciones_3418755": '''
from(bucket: "monitoraqua")
  |> range(start: -{h}h)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D8" and r.tipo == "estimacion" and r.sensorId == "thingspeak-3418755")
  |> filter(fn: (r) => r._field == "agua_c" or r._field == "variante")
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> group()''',
    "D4_error_llegada_real": '''
from(bucket: "monitoraqua")
  |> range(start: -{h}h)
  |> filter(fn: (r) => r._measurement == "predicciones" and r.dimension == "D4" and r.tipo == "verificacion")
  |> pivot(rowKey: ["_time", "sensorId", "version_modelo"], columnKey: ["_field"], valueColumn: "_value")
  |> group()''',
}


def flux(q):
    cuerpo = {"queries": [{"refId": "A", "datasource": {"type": "influxdb", "uid": "influxdb"}, "query": q.strip()}],
              "from": "now-30d", "to": "now"}
    req = urllib.request.Request("http://localhost:43000/api/ds/query", data=json.dumps(cuerpo).encode(), method="POST",
                                 headers={"Authorization": vt.AUTH, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        frames = json.loads(r.read())["results"]["A"].get("frames", [])
    filas = []
    for f in frames:
        nombres = [c["name"] for c in f["schema"]["fields"]]
        for valores in zip(*f["data"]["values"]):
            filas.append(dict(zip(nombres, valores)))
    return filas


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--horas", type=int, default=48)
    a = ap.parse_args()
    out = {"generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), "ventana_h": a.horas}
    for nombre, q in CONSULTAS.items():
        filas = flux(q.replace("{h}", str(a.horas)))
        res = {"n": len(filas), "filas": filas}
        errores = [f.get("error_c", f.get("error_h")) for f in filas if f.get("error_c", f.get("error_h")) is not None]
        if errores:
            res["error_medio"] = sum(errores) / len(errores)
            res["error_abs_medio"] = sum(abs(x) for x in errores) / len(errores)
        out[nombre] = res
        print(nombre, "n =", len(filas), {k: round(v, 3) for k, v in res.items() if k.startswith("error")})
    destino = RAIZ / "docs" / "evidencias" / "f7" / f"verificacion_en_vivo_{datetime.now().strftime('%Y%m%d')}.json"
    destino.write_text(json.dumps(out, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    print("guardado en", destino.relative_to(RAIZ))


if __name__ == "__main__":
    main()
