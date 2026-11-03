"""
Prueba de carga del stack (F8, criterio 5): publica tanques sintéticos a velocidades crecientes en un TÓPICO DE PRUEBA
y mide en InfluxDB/Grafana el lag, la duración de los micro-lotes y la latencia de la inferencia del job de Spark.

Nada de esto toca aqua-eventos-real, Firebase ni el bucket monitoraqua:
  tópico   aqua-carga-prueba (3 particiones, retención 1 día)
  bucket   monitoraqua_carga (7 días)
  job      aqua-streaming-carga: misma imagen que aqua-streaming, perfil "carga" de infra/compose.yml, con checkpoint,
           bronce y tanques.yml propios

Cada tanque de prueba (carga-00000, ...) reenvía las lecturas de un tanque de la flota (batch/datos/aumentados) con la
hora actual, una cada 5 s como el ESP32: así pasa por las 7 consultas (agregados, watermark, eventos de servicio,
inferencia), igual que un tanque real. Una etapa de E eventos/s son E x 5 tanques.

  .venv/Scripts/python infra/prueba_carga.py preparar             bucket, tópico y data/local/carga/tanques.yml
  docker compose -f infra/compose.yml --env-file .env --profile carga up -d aqua-streaming-carga
  CARGA_TOPICO=aqua-carga-prueba-2 CARGA_MASTER=local[6] (opcionales; los mismos para el job)
  .venv/Scripts/python infra/prueba_carga.py publicar [--etapas 100,500,1000,2000,5000] [--duracion 360] [--pausa 120]
  .venv/Scripts/python infra/prueba_carga.py medir docs/evidencias/f8/prueba_carga_<fecha>.json
  docker compose -f infra/compose.yml --env-file .env --profile carga stop aqua-streaming-carga
"""
import argparse
import json
import os
import statistics
import subprocess
import threading
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import verificar_stack as vs  # noqa: E402
import verificar_tableros as vt  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
TOPICO = os.getenv("CARGA_TOPICO", "aqua-carga-prueba")  # una corrida nueva = un tópico nuevo
BUCKET = "monitoraqua_carga"
INTERVALO_TANQUE_S = 5  # como el ESP32
CORRIDA_FLOTA = "r7-79a6b935"
DESTINO = RAIZ / "docs" / "evidencias" / "f8"


def influx_api(ruta, cuerpo=None, metodo="GET"):
    req = urllib.request.Request(vs.INFLUX + ruta, method=metodo,
                                 data=json.dumps(cuerpo).encode() if cuerpo is not None else None,
                                 headers={"Authorization": f"Token {vs.ENV.get('INFLUX_TOKEN', '')}",
                                          "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read() or b"{}")


def preparar(a):
    org = influx_api(f"/api/v2/orgs?org={vs.ENV.get('INFLUX_ORG', 'horeb')}")["orgs"][0]["id"]
    if not influx_api(f"/api/v2/buckets?name={BUCKET}").get("buckets"):
        influx_api("/api/v2/buckets", {"orgID": org, "name": BUCKET,
                                       "retentionRules": [{"type": "expire", "everySeconds": 7 * 86400}]}, "POST")
        print("bucket creado:", BUCKET)
    from kafka.admin import KafkaAdminClient, NewTopic
    admin = KafkaAdminClient(bootstrap_servers=vs.KAFKA)
    if TOPICO not in admin.list_topics():
        admin.create_topics([NewTopic(TOPICO, num_partitions=a.particiones, replication_factor=1,
                                      topic_configs={"retention.ms": str(86400 * 1000)})])
        print("tópico creado:", TOPICO, a.particiones, "particiones")
    admin.close()
    # Geometría de los tanques de prueba: la del tanque de la flota que reproducen
    base = yaml.safe_load((RAIZ / "ingesta" / "tanques.yml").read_text(encoding="utf-8"))
    plantillas = sorted(k for k in base["tanques"] if k.startswith("flota-"))
    base["tanques"].update({nombre(i): dict(base["tanques"][plantillas[i % len(plantillas)]])
                            for i in range(a.max_tanques)})
    destino = RAIZ / "data" / "local" / "carga"
    destino.mkdir(parents=True, exist_ok=True)
    (destino / "tanques.yml").write_text(yaml.safe_dump(base, allow_unicode=True, sort_keys=False), encoding="utf-8")
    print("tanques de prueba:", a.max_tanques, "->", (destino / "tanques.yml").relative_to(RAIZ))


def nombre(i):
    return f"carga-{i:05d}"


def series_flota():
    d = pd.read_parquet(RAIZ / "batch" / "datos" / "aumentados" / f"runId={CORRIDA_FLOTA}",
                        columns=["sensorId", "timestamp", "distanciaCm", "temperaturaC", "humedadPct"])
    d = d.sort_values(["sensorId", "timestamp"])
    return [g[["distanciaCm", "temperaturaC", "humedadPct"]].to_dict("records") for _, g in d.groupby("sensorId")]


def publicar(a):
    from kafka import KafkaProducer
    series = series_flota()
    prod = KafkaProducer(bootstrap_servers=vs.KAFKA, acks=1, linger_ms=20, batch_size=262144,
                         key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode())
    etapas, puntero = [], {}
    memoria, fin_muestreo = [], threading.Event()

    def muestrear():  # memoria de los contenedores de Spark cada 15 s (docker stats)
        while not fin_muestreo.wait(15):
            try:
                salida = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.Name}} {{.MemUsage}}"],
                                        capture_output=True, text=True, timeout=30).stdout
                memoria.append({"ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                                **{l.split()[0]: l.split()[1] for l in salida.splitlines() if "streaming" in l}})
            except Exception:  # noqa: BLE001 - una muestra perdida no detiene la prueba
                pass
    threading.Thread(target=muestrear, daemon=True).start()
    for k, tasa in enumerate(int(x) for x in a.etapas.split(",")):
        n_tanques = tasa * INTERVALO_TANQUE_S
        total = tasa * a.duracion
        t0 = time.time()
        inicio = datetime.now(timezone.utc)
        for j in range(total):
            i = j % n_tanques
            serie = series[i % len(series)]
            p = puntero.get(i, (i * 997) % len(serie))  # cada tanque arranca en otra hora de su plantilla
            puntero[i] = (p + 1) % len(serie)
            ahora = int(time.time() * 1000)
            ev = {"tipoEvento": "aqua.tanque.lectura", "eventId": f"carga-{tasa}-{j}", "sensorId": nombre(i),
                  "origen": "esp32-fisico", "fuenteTipo": "iot-propio", "timestamp": ahora, "ingestaTs": ahora,
                  **{c: v for c, v in serie[p].items() if v == v}}
            prod.send(TOPICO, key=ev["sensorId"], value=ev)
            if j % 250 == 0:
                espera = j / tasa - (time.time() - t0)
                if espera > 0:
                    time.sleep(espera)
        prod.flush()
        fin = datetime.now(timezone.utc)
        real = total / (time.time() - t0)
        etapas.append({"tasa_objetivo": tasa, "tanques": n_tanques, "eventos": total, "tasa_lograda": round(real, 1),
                       "inicio": inicio.isoformat(timespec="seconds"), "fin": fin.isoformat(timespec="seconds")})
        print(json.dumps(etapas[-1]), flush=True)
        if k < len(a.etapas.split(",")) - 1:
            time.sleep(a.pausa)
    prod.close()
    fin_muestreo.set()
    DESTINO.mkdir(parents=True, exist_ok=True)
    ruta = DESTINO / f"prueba_carga_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    ruta.write_text(json.dumps({"topico": TOPICO, "bucket": BUCKET, "pausa_s": a.pausa, "master": os.getenv("CARGA_MASTER", "local[*]"),
                                "etapas": etapas, "memoria": memoria},
                               indent=1), encoding="utf-8")
    print("etapas guardadas en", ruta.relative_to(RAIZ))


def flux(q):
    cuerpo = {"queries": [{"refId": "A", "datasource": {"type": "influxdb", "uid": "influxdb"}, "query": q.strip()}],
              "from": "now-7d", "to": "now"}
    req = urllib.request.Request("http://localhost:43000/api/ds/query", data=json.dumps(cuerpo).encode(),
                                 method="POST", headers={"Authorization": vt.AUTH, "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        frames = json.loads(r.read())["results"]["A"].get("frames", [])
    filas = []
    for f in frames:
        nombres = [c["name"] for c in f["schema"]["fields"]]
        etiquetas = f["schema"]["fields"][-1].get("labels", {})
        for valores in zip(*f["data"]["values"]):
            filas.append({**etiquetas, **dict(zip(nombres, valores))})
    return filas


def serie(medida, campo, ini, fin, extra=""):
    return flux(f'''
from(bucket: "{BUCKET}")
  |> range(start: {ini}, stop: {fin})
  |> filter(fn: (r) => r._measurement == "{medida}" and r._field == "{campo}"{extra})
  |> keep(columns: ["_time", "_value", "consulta"])''')


def medir(a):
    datos = json.loads(Path(a.archivo).read_text(encoding="utf-8"))
    for e in datos["etapas"]:
        # la etapa más la pausa siguiente: el drenaje del lag es parte de la medición
        fin = (datetime.fromisoformat(e["fin"]).timestamp() + datos["pausa_s"])
        ini, fin_iso = e["inicio"], datetime.fromtimestamp(fin, timezone.utc).isoformat(timespec="seconds")
        res = {}
        for consulta in ["predicciones", "rapidas_agg", "esp32_detalle", "eventos_servicio", "latencias", "bronce"]:
            filtro = f' and r.consulta == "{consulta}"'
            dur = [x["_value"] for x in serie("spark_progreso", "batchDurationMs", ini, fin_iso, filtro)]
            lag = [x["_value"] for x in serie("spark_progreso", "lagOffsets", ini, fin_iso, filtro)]
            filas = [x["_value"] for x in serie("spark_progreso", "numInputRows", ini, fin_iso, filtro)]
            if not dur:
                continue
            res[consulta] = {"lotes": len(dur), "filas": int(sum(filas)),
                             "duracion_lote_ms": {"mediana": statistics.median(dur), "max": max(dur)},
                             "lag_offsets": {"max": max(lag) if lag else None, "final": lag[-1] if lag else None}}
        inf = [x["_value"] for x in serie("inferencia", "duracion_ms", ini, fin_iso)]
        if inf:
            res["inferencia_ms"] = {"mediana": statistics.median(inf), "max": max(inf), "lotes": len(inf)}
        lat = [x["_value"] for x in serie("latencia_ingesta", "s", ini, fin_iso)]
        if lat:
            lat.sort()
            res["latencia_ingesta_s"] = {"p50": lat[len(lat) // 2], "p95": lat[int(len(lat) * 0.95)]}
        e["medicion"] = res
        print(e["tasa_objetivo"], "ev/s", json.dumps(res))
    Path(a.archivo).write_text(json.dumps(datos, indent=1), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("preparar")
    p.add_argument("--particiones", type=int, default=3)
    p.add_argument("--max-tanques", type=int, default=25000, dest="max_tanques")
    p = sub.add_parser("publicar")
    p.add_argument("--etapas", default="100,500,1000,2000,5000")
    p.add_argument("--duracion", type=int, default=360, help="segundos por etapa")
    p.add_argument("--pausa", type=int, default=120, help="segundos entre etapas (drenaje)")
    p = sub.add_parser("medir")
    p.add_argument("archivo")
    a = ap.parse_args()
    {"preparar": preparar, "publicar": publicar, "medir": medir}[a.cmd](a)


if __name__ == "__main__":
    main()
