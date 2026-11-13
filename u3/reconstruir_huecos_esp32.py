"""
U3 · D4 · Reconstrucción ROTULADA de los huecos del ESP32, paso 1 de 2: generar y PUBLICAR EN KAFKA.

Camino de los datos (no pasa por Firebase):
  Parquet bronce del ESP32 (lo que ya llegó por Kafka y Spark) -> este script (PySpark) -> Kafka
  aqua-eventos-reconstruidos -> u3/cargar_reconstruccion_spark.py (PySpark) -> Parquet aumentados + InfluxDB aumentados

No inventa lecturas del sensor real: genera una serie aparte, marcada como reconstruida, con
  sensorId     esp32-tanque-real-01-reconstruido   (nunca el sensorId real)
  origen       reconstruccion-esp32 · fuenteTipo aumentado · reconstruido = true
  runId        reconstruccion-esp32-<fecha de la corrida>
Contrato propio: contratos/evento_reconstruido.schema.json. Tópico propio: aqua-eventos-reconstruidos (ni
aqua-eventos-real ni aqua-eventos-aumentados, así la inferencia en vivo de F7 no la procesa). Nunca va a Firebase ni
al bucket real, y no entra en la métrica de ningún modelo.

Método: "días análogos". Para cada hueco, el nivel avanza con los cambios minuto a minuto de un día REAL completo del
mismo ESP32 (sus llenados y su consumo, a la misma hora del día), parte de la última lectura real antes del hueco y se
corrige linealmente para terminar en la primera lectura real después (puente). El aire y la humedad copian el ciclo
diario del día donante con el mismo ajuste en los extremos. El ruido de 5 s se toma de los residuos reales del sensor.
La semilla es fija: dos corridas dan exactamente lo mismo.

Uso (CMD, raíz del repo; detalle en docs/u3-d4.md, sección 12):
  docker run --rm --network lambda26-kafka-net --env-file .env -v "%cd%:/repo" -w /repo monitoraqua-streaming:latest ^
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --driver-memory 2g ^
    --conf spark.log.level=ERROR u3/reconstruir_huecos_esp32.py

Opciones:
  --desde/--hasta  periodo a revisar (ISO con zona). Por defecto: desde la instalación del JSN-SR04T hasta la última lectura.
  --min-hueco-min  duración mínima de un hueco para reconstruirlo (30).
  --seco           solo calcula y muestra los huecos; no publica en Kafka.
Responsable: Wilbert Alex Mayta Arotaype (D4).
"""
import argparse
import glob
import hashlib
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

RAIZ = Path(__file__).resolve().parents[1]
BRONCE = RAIZ / "data" / "local" / "bronce"
REAL = "esp32-tanque-real-01"
RECON = f"{REAL}-reconstruido"
ORIGEN = "reconstruccion-esp32"
TOPICO = os.getenv("KAFKA_TOPIC_RECONSTRUIDOS", "aqua-eventos-reconstruidos")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_INTERNO", "kafka:9092")
SENSOR_DESDE = "2026-10-29T09:22:23-05:00"      # instalación del JSN-SR04T (ingesta/tanques.yml -> sensor_desde)
LIMA = "America/Lima"
SEMILLA = 2026
COBERTURA_DONANTE = 0.75                         # un día real sirve de donante si tiene datos en >= 75 % de sus minutos
RESIDUO_MAX_CM = 3.0                             # ruido de 5 s: residuos reales del sensor, sin los ecos grandes
PASO_MS = 5000                                   # el ESP32 publica cada 5 s
MANIFIESTO = RAIZ / "docs" / "evidencias" / "u3-d4" / "reconstruccion_huecos_esp32.json"
CAMPOS_EVENTO = ["tipoEvento", "eventId", "sensorId", "origen", "fuenteTipo", "runId", "timestamp", "ingestaTs",
                 "distanciaCm", "temperaturaC", "humedadPct", "reconstruido", "metodo", "donanteDia", "esAnomalia"]


def sesion_spark(nombre):
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName(nombre).master("local[*]")
            # el bronce son muchos Parquet pequeños sobre una carpeta de Windows montada: agrupar al leer
            .config("spark.sql.files.openCostInBytes", "8192")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())


def cargar_bronce(spark, desde):
    """Lecturas del ESP32 real desde el Parquet bronce (escrito por el streaming a partir de Kafka)."""
    from pyspark.sql import functions as F
    dia_min = (desde - pd.Timedelta(days=1)).date().isoformat()
    rutas = sorted(p for p in glob.glob(str(BRONCE / "origen=esp32-fisico" / "fecha=*"))
                   if p.rsplit("fecha=", 1)[1] >= dia_min)
    if not rutas:
        sys.exit(f"No hay Parquet bronce del ESP32 en {BRONCE} desde {dia_min}.")
    df = (spark.read.option("basePath", str(BRONCE)).parquet(*rutas)
          .filter(F.col("sensorId") == REAL)
          .select(F.col("timestamp").cast("long"), F.col("distanciaCm").cast("double"),
                  F.col("temperaturaC").cast("double"), F.col("humedadPct").cast("double")))
    return df.toPandas(), len(rutas)


def preparar(df, desde, hasta):
    df = df.dropna(subset=["timestamp"]).drop_duplicates("timestamp").sort_values("timestamp")
    df["t"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(LIMA)
    df = df[df["t"] >= desde]
    if hasta is not None:
        df = df[df["t"] <= hasta]
    validas = df[(df["distanciaCm"] > 0) & (df["temperaturaC"] > 0) & (df["humedadPct"] > 0)].copy()
    return df.reset_index(drop=True), validas


def huecos(df, min_min):
    t = df["t"].reset_index(drop=True)
    dt = t.diff().dt.total_seconds()
    return [(t[i - 1], t[i]) for i in dt[dt > min_min * 60].index]


def series_minuto(validas):
    """Medianas por minuto (distancia, aire, humedad) en hora de Lima."""
    s = validas.set_index("t")[["distanciaCm", "temperaturaC", "humedadPct"]]
    return s.resample("1min").median()


def donantes(m):
    """Días reales con cobertura >= COBERTURA_DONANTE: su perfil minuto a minuto (huecos cortos interpolados)."""
    out = {}
    for dia, x in m.groupby(m.index.normalize()):
        if x["distanciaCm"].notna().mean() < COBERTURA_DONANTE or len(x) < 1300:
            continue
        x = x.copy()
        x.index = (x.index - dia).total_seconds().astype(int) // 60      # minuto del día 0..1439
        x = x.reindex(range(1440)).interpolate(limit_direction="both")
        x["distanciaCm"] = x["distanciaCm"].rolling(5, center=True, min_periods=1).median()
        out[dia.date().isoformat()] = x
    return out


def donante_para(dia_iso, claves):
    """Elección determinista del día donante para una fecha (semilla fija)."""
    h = int(hashlib.sha256(f"{SEMILLA}-{dia_iso}".encode()).hexdigest(), 16)
    return claves[h % len(claves)]


def reconstruir_hueco(ini, fin, m, perfiles, residuos, rng, rango):
    """Serie por minuto dentro de (ini, fin), puenteada entre la última lectura real antes y la primera después."""
    minutos = pd.date_range(ini.ceil("1min"), fin.floor("1min") - pd.Timedelta("1min"), freq="1min", tz=LIMA)
    if len(minutos) < 2:
        return None, []
    claves = sorted(perfiles)
    usados, dd, temp, hum, don = [], [], [], [], []
    for t in minutos:
        dia = t.date().isoformat()
        clave = donante_para(dia, claves)
        if not usados or usados[-1][0] != dia:
            usados.append((dia, clave))
        p = perfiles[clave]
        mi = t.hour * 60 + t.minute
        dd.append(p["distanciaCm"].iat[mi] - p["distanciaCm"].iat[mi - 1 if mi else 0])
        temp.append(p["temperaturaC"].iat[mi])
        hum.append(p["humedadPct"].iat[mi])
        don.append(clave)
    antes = m.loc[:ini].dropna().iloc[-1]
    despues = m.loc[fin:].dropna()
    despues = despues.iloc[0] if len(despues) else antes
    n = len(minutos)
    w = np.linspace(0, 1, n)
    dist = antes["distanciaCm"] + np.cumsum(dd)
    dist = dist + w * (despues["distanciaCm"] - dist[-1])               # puente: termina en la lectura real de después
    dist = np.clip(dist, *rango)
    temp, hum = np.array(temp), np.array(hum)
    temp = temp + (1 - w) * (antes["temperaturaC"] - temp[0]) + w * (despues["temperaturaC"] - temp[-1])
    hum = hum + (1 - w) * (antes["humedadPct"] - hum[0]) + w * (despues["humedadPct"] - hum[-1])
    minuto = pd.DataFrame({"t": minutos, "distanciaCm": dist, "temperaturaC": temp, "humedadPct": hum, "don": don})
    # lecturas cada 5 s, con el ruido real del sensor
    filas = []
    for r in minuto.itertuples(index=False):
        base = int(r.t.timestamp() * 1000)
        for k in range(12):
            ts = base + k * PASO_MS + int(rng.integers(0, 400))
            filas.append((ts, round(float(np.clip(r.distanciaCm + rng.choice(residuos), *rango)), 1),
                          round(float(r.temperaturaC + rng.normal(0, 0.05)), 1),
                          int(round(float(np.clip(r.humedadPct + rng.normal(0, 0.5), 1, 99)))), r.don))
    cols = ["timestamp", "distanciaCm", "temperaturaC", "humedadPct", "donanteDia"]
    return pd.DataFrame(filas, columns=cols), usados


def eventos(rec, run_id):
    """Eventos con el contrato contratos/evento_reconstruido.schema.json."""
    ev = rec.copy()
    ev["tipoEvento"] = "aqua.tanque.lectura"
    ev["sensorId"] = RECON
    ev["eventId"] = [f"{RECON}-{ts}" for ts in ev["timestamp"]]       # determinístico: re-publicar no duplica en Spark
    ev["origen"] = ORIGEN
    ev["fuenteTipo"] = "aumentado"
    ev["runId"] = run_id
    ev["ingestaTs"] = int(time.time() * 1000)
    ev["reconstruido"] = True
    ev["metodo"] = "dias-analogos"
    ev["esAnomalia"] = False
    ev["humedadPct"] = ev["humedadPct"].astype(float)
    return ev[CAMPOS_EVENTO]


def validar(ev):
    """Las reglas del contrato que importan para no mezclar con lo real (sin dependencias)."""
    problemas = []
    if ev["eventId"].duplicated().any():
        problemas.append("eventId repetido")
    if (ev["sensorId"] != RECON).any() or (ev["origen"] != ORIGEN).any() or (ev["fuenteTipo"] != "aumentado").any():
        problemas.append("sensorId/origen/fuenteTipo distinto del rotulado")
    if not ev["reconstruido"].all():
        problemas.append("reconstruido = false")
    if ev[["timestamp", "distanciaCm", "temperaturaC", "humedadPct"]].isna().any().any():
        problemas.append("medidas nulas")
    return problemas


def publicar_kafka(spark, ev):
    from pyspark.sql import functions as F
    sdf = spark.createDataFrame(ev)
    t0 = time.time()
    (sdf.select(F.col("sensorId").alias("key"), F.to_json(F.struct(*CAMPOS_EVENTO)).alias("value"))
        .write.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", TOPICO)
        .option("kafka.acks", "all").save())
    return {"topico": TOPICO, "publicados": len(ev), "segundos": round(time.time() - t0, 1)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde", default=SENSOR_DESDE)
    ap.add_argument("--hasta")
    ap.add_argument("--min-hueco-min", type=float, default=30)
    ap.add_argument("--seco", action="store_true")
    a = ap.parse_args()

    desde = pd.Timestamp(a.desde).tz_convert(LIMA)
    hasta = pd.Timestamp(a.hasta).tz_convert(LIMA) if a.hasta else None
    spark = sesion_spark("u3-d4-reconstruccion-publicar")
    t0 = time.time()
    df, n_rutas = cargar_bronce(spark, desde)
    print(f"Bronce del ESP32: {n_rutas} carpetas fecha=, {len(df):,} filas en {time.time() - t0:.0f} s")
    df, validas = preparar(df, desde, hasta)
    m = series_minuto(validas)
    perfiles = donantes(m)
    if not perfiles:
        sys.exit("No hay ningún día real con cobertura suficiente para servir de donante.")
    s = validas.set_index("t")["distanciaCm"]
    residuos = (s - s.rolling(9, center=True, min_periods=1).median()).dropna().to_numpy()
    residuos = residuos[np.abs(residuos) <= RESIDUO_MAX_CM]
    rango = tuple(float(v) for v in np.quantile(validas["distanciaCm"], [0.005, 0.995]).round(1))
    rng = np.random.default_rng(SEMILLA)

    lista = huecos(df, a.min_hueco_min)
    print(f"Lecturas reales del ESP32 en el periodo: {len(df):,} · días donantes: {sorted(perfiles)}")
    print(f"Huecos de más de {a.min_hueco_min:g} min: {len(lista)} · rango de distancia {rango} cm")
    partes, resumen = [], []
    for ini, fin in lista:
        rec, usados = reconstruir_hueco(ini, fin, m, perfiles, residuos, rng, rango)
        horas = (fin - ini).total_seconds() / 3600
        if rec is None:
            continue
        partes.append(rec)
        resumen.append({"desde": ini.isoformat(timespec="seconds"), "hasta": fin.isoformat(timespec="seconds"),
                        "horas": round(horas, 2), "lecturas": len(rec),
                        "donantes": [{"dia": d, "donante": c} for d, c in usados]})
        print(f"  {ini:%d/%m %H:%M} -> {fin:%d/%m %H:%M}  ({horas:6.1f} h): {len(rec):>7,} lecturas · "
              f"donantes {', '.join(sorted({c for _, c in usados}))}")
    if not partes:
        print("No hay huecos que reconstruir.")
        return
    run_id = f"reconstruccion-esp32-{datetime.now(timezone.utc):%Y%m%d}"
    ev = eventos(pd.concat(partes, ignore_index=True), run_id)
    problemas = validar(ev)
    if problemas:
        sys.exit(f"Los eventos no cumplen el contrato: {problemas}")
    print(f"\n{len(ev):,} eventos reconstruidos (runId {run_id}) cumplen contratos/evento_reconstruido.schema.json")

    kafka = {"publicado": False, "motivo": "--seco"}
    if not a.seco:
        kafka = {"publicado": True, **publicar_kafka(spark, ev)}
        print(f"Kafka: {kafka['publicados']:,} eventos en {TOPICO} ({kafka['segundos']} s)")
        print("Siguiente paso: u3/cargar_reconstruccion_spark.py (Kafka -> Parquet aumentados + InfluxDB aumentados)")
    spark.stop()

    manifiesto = {
        "runId": run_id, "sensorId": RECON, "sensorReal": REAL, "origen": ORIGEN, "metodo": "dias-analogos",
        "descripcion": "Reconstrucción rotulada de los huecos del ESP32: no son lecturas medidas. Viajan por un tópico "
                       "propio de Kafka, van solo a datos aumentados y no entran en la métrica de ningún modelo.",
        "camino": "bronce ESP32 -> PySpark -> Kafka " + TOPICO + " -> PySpark -> Parquet/InfluxDB aumentados",
        "fuente": "data/local/bronce/origen=esp32-fisico", "periodo": [desde.isoformat(), str(a.hasta or "última lectura")],
        "semilla": SEMILLA, "minHuecoMin": a.min_hueco_min, "diasDonantes": sorted(perfiles),
        "rangoDistanciaCm": list(rango), "lecturas": len(ev), "huecos": resumen, "kafka": kafka,
        "generadoUtc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    MANIFIESTO.parent.mkdir(parents=True, exist_ok=True)
    MANIFIESTO.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Resumen:", MANIFIESTO.relative_to(RAIZ))


if __name__ == "__main__":
    main()
