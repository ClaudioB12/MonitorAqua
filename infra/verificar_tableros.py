"""
Verifica que cada panel y cada anotación de los tableros de F5 devuelva datos, ejecutando sus consultas con la API
de Grafana (/api/ds/query): mismas macros (v.timeRangeStart, v.windowPeriod) y mismo datasource que el tablero.

  .venv/Scripts/python infra/verificar_tableros.py [--json docs/evidencias/f5/verificacion_paneles.json]

Escenarios:
  BI, origen = real        últimas 24 h, bucket monitoraqua
  BI, origen = aumentado   2026-09-28 a 2026-10-28, bucket monitoraqua_aumentados
  Pipeline                 últimas 24 h
Resultado por panel: filas devueltas, "sin datos" (el panel muestra "No data") o el error de la consulta.
"""
import argparse
import base64
import json
import os
import urllib.request
from pathlib import Path

RAIZ = Path(__file__).resolve().parents[1]
GRAFANA = "http://localhost:43000"


def env():
    e = {}
    for l in (RAIZ / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in l and not l.lstrip().startswith("#"):
            k, v = l.split("=", 1)
            e[k.strip()] = v.strip()
    return e


E = env()
AUTH = "Basic " + base64.b64encode(f"{E.get('GRAFANA_ADMIN_USER', 'admin')}:{E.get('GRAFANA_ADMIN_PASSWORD', 'admin')}".encode()).decode()


def consultar(target, desde, hasta, variables):
    q = dict(target)
    for campo in ("query", "expr"):
        if campo in q:
            for k, v in variables.items():
                q[campo] = q[campo].replace(k, v)
    # Como Grafana: el intervalo (v.windowPeriod) sale del rango dividido por los puntos que caben en el panel
    span = (24 * 3600 * 1000 if desde.startswith("now-") else int(hasta) - int(desde))
    q.update({"intervalMs": max(60000, span // 800), "maxDataPoints": 800})
    cuerpo = json.dumps({"queries": [q], "from": desde, "to": hasta}).encode()
    req = urllib.request.Request(f"{GRAFANA}/api/ds/query", data=cuerpo, method="POST",
                                 headers={"Authorization": AUTH, "Content-Type": "application/json"})
    try:
        r = json.load(urllib.request.urlopen(req, timeout=60))
    except urllib.error.HTTPError as ex:
        return None, ex.read().decode()[:200]
    res = r["results"][q["refId"]]
    if res.get("error"):
        return None, res["error"][:200]
    filas = sum(len(f["data"]["values"][0]) if f["data"]["values"] else 0 for f in res.get("frames", []))
    return filas, None


def verificar(tablero, desde, hasta, variables, escenario):
    t = json.loads((RAIZ / "dashboards" / tablero).read_text(encoding="utf-8"))
    out = []
    elementos = [(p["title"], tg) for p in t["panels"] if p["type"] not in ("row", "text") for tg in p["targets"]]
    elementos += [(f"anotación: {a['name']}", a["target"]) for a in t["annotations"]["list"]]
    for titulo, tg in elementos:
        filas, error = consultar(tg, desde, hasta, variables)
        estado = "ERROR" if error else ("datos" if filas else "sin datos")
        out.append({"escenario": escenario, "panel": titulo, "refId": tg["refId"], "filas": filas, "estado": estado,
                    "error": error})
        print(f"  {estado:<9} {str(filas if filas is not None else '-'):>6}  {titulo} [{tg['refId']}]"
              + (f"  -> {error}" if error else ""))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--json")
    a = ap.parse_args()
    escenarios = [
        ("bi_salud_agua.json", "now-24h", "now", {"${origen}": "monitoraqua", "${sensorId:regex}": ".*"}, "BI · real · 24 h"),
        ("bi_salud_agua.json", "1790571600000", "1793163600000",
         {"${origen}": "monitoraqua_aumentados", "${sensorId:regex}": ".*"}, "BI · aumentado · 28-sep a 28-oct"),
        ("pipeline_salud.json", "now-24h", "now", {}, "Pipeline · 24 h"),
    ]
    todo = []
    for tablero, desde, hasta, variables, nombre in escenarios:
        print(f"\n== {nombre}")
        todo += verificar(tablero, desde, hasta, variables, nombre)
    resumen = {}
    for x in todo:
        resumen.setdefault(x["escenario"], {}).setdefault(x["estado"], 0)
        resumen[x["escenario"]][x["estado"]] += 1
    print("\nResumen:", json.dumps(resumen, ensure_ascii=False))
    if a.json:
        Path(a.json).parent.mkdir(parents=True, exist_ok=True)
        Path(a.json).write_text(json.dumps({"resumen": resumen, "paneles": todo}, ensure_ascii=False, indent=1),
                                encoding="utf-8")


if __name__ == "__main__":
    main()
