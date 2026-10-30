"""
Capas plata y oro para los modelos (F6), con Spark en batch.

Fuentes (todas leídas, ninguna modificada):
  batch/datos/fuentes/f6_esp32_firebase.json      historial completo del ESP32 (desde el 2-oct)
  data/local/bronce/                              Parquet bronce de F3 (todo lo que pasó por Kafka)
  batch/datos/fuentes/f6_thingspeak.json          7 canales (3418755 completo)
  batch/datos/fuentes/f6_open_meteo_juliaca.json  Juliaca horario (archivo + días recientes)
  batch/datos/aumentados/runId=*/                 flota extrapolada f2b.1 (r2026-45facae2) y f2b.2 (r7-79a6b935)

Salidas (no versionadas):
  batch/datos/plata/fuente=*/          deduplicado por eventId, sin sensores de prueba, inválidas MARCADAS (no
                                       borradas), distancia compensada y nivel en % y litros con la geometría vigente
                                       (geometria_version, geometria_por_medir); compensacionOrigen: la distancia
                                       la compensa el detector, ya llega compensada por el firmware (ESP32) o no se
                                       sabe (ingesta/tanques.yml -> compensacion_en_firmware)
  batch/datos/oro/{d2_autonomia,d4_llegadas,d6_noches,d8_termica}/   ver batch/oro.py; columna "particion"
                                       según la partición vigente (batch/particion.py -> VIGENTE)
  batch/datos/calidad.json             filas, % válidas, rangos de fechas, tramos y huecos (para docs/dataset-modelos.md)

  docker run --rm -v "$PWD:/repo" -w /repo -e PYTHONPATH=/repo/streaming:/repo/batch monitoraqua-streaming \
      spark-submit --driver-memory 3g batch/capas.py
"""
import hashlib
import json
import math
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType, StructField, StructType)

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "streaming"))
sys.path.insert(0, str(RAIZ / "batch"))
DATOS = RAIZ / "batch" / "datos"
FUENTES = DATOS / "fuentes"
BITACORA = RAIZ / "docs" / "evidencias" / "f4" / "bitacora_real.csv"
# Reglas con las que el detector genera los eventos de las tablas oro (d4_llegadas). Por defecto las vigentes; para
# reproducir las entradas de d2.1-d2.2 y d4.1 se fijan las de f4.3:
#   git show b400e7d:streaming/reglas_servicio.yml > /tmp/reglas_f43.yml  y  REGLAS_ORO=/tmp/reglas_f43.yml
REGLAS_ORO = os.getenv("REGLAS_ORO", str(RAIZ / "streaming" / "reglas_servicio.yml"))
CORRIDAS = {"r2026-45facae2": "aumentado_f2b1", "r7-79a6b935": "aumentado_f2b2"}
PRIORIDAD = {"firebase": 0, "thingspeak-api": 1, "open-meteo-archivo": 1, "open-meteo-pronostico": 2,
             "kafka-bronce": 3, "parquet": 0}

BASE = StructType([
    StructField("fuente", StringType()), StructField("origen", StringType()), StructField("sensorId", StringType()),
    StructField("eventId", StringType()), StructField("timestamp", LongType()), StructField("runId", StringType()),
    StructField("origen_dato", StringType()), StructField("distanciaCm", DoubleType()),
    StructField("nivelPct_fuente", DoubleType()), StructField("nivelM", DoubleType()),
    StructField("temperaturaC", DoubleType()), StructField("humedadPct", DoubleType()),
    StructField("tempAguaC", DoubleType()), StructField("caudalM3s", DoubleType()),
    StructField("esAnomalia", BooleanType()), StructField("tipoAnomalia", StringType()),
    StructField("episodioId", StringType()),
])
PLATA = StructType(BASE.fields + [
    StructField("lectura_valida", BooleanType()), StructField("motivo_invalidez", StringType()),
    StructField("distanciaCompensadaCm", DoubleType()), StructField("compensada", BooleanType()),
    StructField("nivelPct", DoubleType()), StructField("nivelCm", DoubleType()), StructField("litros", DoubleType()),
    StructField("geometria_version", StringType()), StructField("geometria_por_medir", BooleanType()),
    StructField("compensacionOrigen", StringType()), StructField("sensorModelo", StringType()),
])


def log(**c):
    print(json.dumps({"service": "f6-capas", **c}, ensure_ascii=False, default=str), flush=True)


def f(v):
    try:
        return None if v in (None, "") else float(v)
    except (TypeError, ValueError):
        return None


# --- Fuentes -------------------------------------------------------------------------------------------------

def fuente_esp32():
    d = json.loads((FUENTES / "f6_esp32_firebase.json").read_text(encoding="utf-8"))
    return pd.DataFrame([{"fuente": "esp32", "origen": "esp32-fisico", "sensorId": v.get("sensorId"), "eventId": k,
                          "timestamp": v.get("timestamp"), "origen_dato": "firebase",
                          "distanciaCm": f(v.get("distanciaCm")), "temperaturaC": f(v.get("temperaturaC")),
                          "humedadPct": f(v.get("humedadPct"))}
                         for k, v in d.items() if isinstance(v.get("timestamp"), int)])


def fuente_thingspeak():
    d = json.loads((FUENTES / "f6_thingspeak.json").read_text(encoding="utf-8"))
    extra = {"3418755": {"temperaturaC": "field2"}}  # aire dentro del tanque (equivalente al DHT11 de la cámara)
    filas = []
    for cid, c in d.items():
        campos = {**c["config"]["campos"], **extra.get(cid, {})}
        escalas = c["config"].get("escalas", {})
        for fe in c["feeds"]:
            ts = int(datetime.fromisoformat(fe["created_at"].replace("Z", "+00:00")).timestamp() * 1000)
            fila = {"fuente": "thingspeak", "origen": "thingspeak", "sensorId": f"thingspeak-{cid}",
                    "eventId": f"thingspeak_{cid}_{ts}", "timestamp": ts, "origen_dato": "thingspeak-api"}
            for medida, campo in campos.items():
                v = f(fe.get(campo))
                fila["nivelPct_fuente" if medida == "nivelPct" else medida] = (
                    round(v * escalas[medida], 4) if v is not None and medida in escalas else v)
            filas.append(fila)
    return pd.DataFrame(filas)


def fuente_open_meteo():
    d = json.loads((FUENTES / "f6_open_meteo_juliaca.json").read_text(encoding="utf-8"))
    horas = {}
    for origen_dato, bloque in (("open-meteo-pronostico", d["pronostico_past_days"]), ("open-meteo-archivo", d["archivo"])):
        h = bloque.get("hourly", {})
        for t, temp, hum in zip(h.get("time", []), h.get("temperature_2m", []), h.get("relative_humidity_2m", [])):
            if temp is None:
                continue
            ts = int(datetime.fromisoformat(t).replace(tzinfo=timezone.utc).timestamp() * 1000)
            horas[ts] = (origen_dato, temp, hum)  # el archivo (segundo) pisa al pronóstico donde existe
    return pd.DataFrame([{"fuente": "open-meteo", "origen": "open-meteo", "sensorId": "open-meteo-juliaca",
                          "eventId": f"open-meteo-horario_juliaca_{ts}", "timestamp": ts, "origen_dato": od,
                          "temperaturaC": float(t), "humedadPct": f(hh)} for ts, (od, t, hh) in horas.items()])


def a_spark(spark, pdf):
    for c in BASE.fieldNames():
        if c not in pdf:
            pdf[c] = None
    pdf = pdf[BASE.fieldNames()].astype(object).where(pdf[BASE.fieldNames()].notna(), None)
    return spark.createDataFrame(pdf.values.tolist(), BASE)


def bitacora():
    """[(ts_ms, accion, sensorId)] de la bitácora real de F4: etiqueta (no verificable, falla...) para d4."""
    b = pd.read_csv(BITACORA)
    return [(int(pd.Timestamp(t).value // 1_000_000), a, s) for t, a, s in zip(b["timestamp"], b["accion"], b["sensorId"])]


# --- Plata ---------------------------------------------------------------------------------------------------

def modelo_sensor(geom, ts):
    """Sensor de distancia vigente en ts según ingesta/tanques.yml -> sensorModelo (lista con desde/hasta), o None.
    Su rango_valido_cm marca en plata las lecturas que ese sensor no puede medir (el detector congelado no lo usa)."""
    if not geom or ts is None:
        return None
    for m in geom.get("sensorModelo") or []:
        desde = partir_ms(m.get("desde"))
        hasta = partir_ms(m.get("hasta"))
        if (desde is None or ts >= desde) and (hasta is None or ts < hasta):
            return m
    return None


def partir_ms(iso):
    return int(datetime.fromisoformat(str(iso)).timestamp() * 1000) if iso else None


def a_plata(lotes):
    from detector_servicio import Configuracion, Detector, origen_compensacion
    from procesar_stream import MEDIDAS, RANGOS
    det = Detector(Configuracion(str(RAIZ / "streaming" / "reglas_servicio.yml"), str(RAIZ / "ingesta" / "tanques.yml")))
    cfg = det.cfg.refrescar()
    margen = cfg.reglas["falla_sensor"]["margen_fondo_cm"]
    versiones = {sid: hashlib.sha1(json.dumps(g, sort_keys=True).encode()).hexdigest()[:8]
                 for sid, g in cfg.tanques.items()}
    for pdf in lotes:
        salida = []
        for r in pdf.to_dict("records"):
            r = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()}
            med = {**r, "nivelPct": r.get("nivelPct_fuente")}
            motivo = None
            if not r.get("eventId") or r.get("timestamp") is None:
                motivo = "sin_eventId_o_timestamp"
            elif r.get("distanciaCm") == -1:
                motivo = "distanciaCm_-1"
            else:
                for m, (lo, hi) in RANGOS.items():
                    if med.get(m) is not None and not lo <= med[m] <= hi:
                        motivo = f"{m}_fuera_de_rango"
                        break
                if motivo is None and all(med.get(m) is None for m in MEDIDAS):
                    motivo = "sin_medidas"
            geom = cfg.tanques.get(r["sensorId"])
            modelo = modelo_sensor(geom, r.get("timestamp"))
            if motivo is None and modelo and r.get("distanciaCm") is not None:
                lo, hi = modelo["rango_valido_cm"]
                if not lo <= r["distanciaCm"] <= hi:
                    motivo = "distanciaCm_fuera_del_rango_del_sensor"
            nivel = (None,) * 6
            if geom and motivo is None:
                nivel = det.nivel(med, geom, cfg.reglas)
                if not nivel[0] and "sensor_al_fondo_cm" in geom and r.get("distanciaCm") is not None \
                        and r["distanciaCm"] > geom["sensor_al_fondo_cm"] + margen:
                    motivo = "distanciaCm_mas_alla_del_fondo"
            _, pct, ncm, lit, dc, comp = nivel
            r.update(lectura_valida=motivo is None, motivo_invalidez=motivo, distanciaCompensadaCm=dc,
                     compensada=bool(comp) if comp is not None else None, nivelPct=pct, nivelCm=ncm, litros=lit,
                     geometria_version=versiones.get(r["sensorId"]),
                     geometria_por_medir=bool(geom.get("por_medir")) if geom else None,
                     # f4.4: quién compensó la distancia (detector, firmware o desconocida; D6 excluye "desconocida")
                     compensacionOrigen=origen_compensacion(med, geom) if dc is not None else None,
                     sensorModelo=modelo["modelo"] if modelo else None)
            salida.append(r)
        yield pd.DataFrame(salida, columns=PLATA.fieldNames())


# --- Oro -----------------------------------------------------------------------------------------------------

def a_oro(juliaca_pdf, tau_h, bitacora):
    def por_sensor(pdf):
        import oro
        from detector_servicio import Configuracion, Detector
        det = Detector(Configuracion(REGLAS_ORO, str(RAIZ / "ingesta" / "tanques.yml")))
        pdf = pdf.sort_values("timestamp").reset_index(drop=True)
        fuente, sid, run = pdf["fuente"].iloc[0], pdf["sensorId"].iloc[0], pdf["runId"].iloc[0]
        validas = pdf[pdf["lectura_valida"]].copy()
        validas["tramo"] = oro.tramos(validas["timestamp"].values, oro.umbral_gap(fuente, validas["timestamp"].values))
        origen = "aumentado" if fuente.startswith("aumentado") else "real"
        lecturas = [{k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in r.items()}
                    for r in pdf[["timestamp", "distanciaCm", "nivelPct_fuente", "nivelM", "temperaturaC",
                                  "lectura_valida"]].rename(columns={"nivelPct_fuente": "nivelPct"}).to_dict("records")]
        eventos = oro.replay(det, sid, lecturas, origen) if sid in det.cfg.refrescar().tanques else []
        tablas = {"d2_autonomia": oro.d2(validas, fuente, sid, run, eventos),
                  "d4_llegadas": oro.d4(validas, fuente, sid, run, eventos,
                                        [(t, a) for t, a, s in bitacora if s == sid], pdf["timestamp"].values),
                  "d6_noches": oro.d6(validas, fuente, sid, run),
                  "d8_termica": oro.d8(validas, fuente, sid, run, eventos, juliaca_pdf, tau_h)}
        filas = []
        for nombre, t in tablas.items():
            if t is None or t.empty:
                continue
            for rec in t.to_dict("records"):
                filas.append({"tabla": nombre, "fuente": fuente, "sensorId": sid, "runId": run,
                              "payload": json.dumps(rec, default=lambda o: None if (isinstance(o, float) and math.isnan(o)) else (o.item() if hasattr(o, "item") else str(o)))})
        return pd.DataFrame(filas, columns=["tabla", "fuente", "sensorId", "runId", "payload"])
    return por_sensor


def main():
    spark = (SparkSession.builder.appName("monitoraqua-f6-capas").master("local[4]")
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    prueba = [s["sensorId"] for s in yaml.safe_load((RAIZ / "ingesta" / "sensores_prueba.yml").read_text(encoding="utf-8"))["sensores"]]

    # Fuentes -> esquema común
    partes = [a_spark(spark, fuente_esp32()), a_spark(spark, fuente_thingspeak()), a_spark(spark, fuente_open_meteo())]
    # Glob a los archivos: el _spark_metadata del sink de streaming guarda rutas del contenedor (/datos/bronce)
    bronce = spark.read.option("basePath", str(RAIZ / "data" / "local" / "bronce")) \
        .parquet(str(RAIZ / "data" / "local" / "bronce" / "origen=*" / "fecha=*" / "*.parquet"))
    partes.append(bronce.select(
        F.when(F.col("origen") == "esp32-fisico", "esp32").otherwise(F.col("origen")).alias("fuente"), "origen", "sensorId",
        "eventId", F.col("timestamp").cast("long"), F.lit(None).cast("string").alias("runId"),
        F.lit("kafka-bronce").alias("origen_dato"), "distanciaCm", F.col("nivelPct").alias("nivelPct_fuente"), "nivelM",
        "temperaturaC", "humedadPct", "tempAguaC", "caudalM3s", F.lit(None).cast("boolean").alias("esAnomalia"),
        F.lit(None).cast("string").alias("tipoAnomalia"), F.lit(None).cast("string").alias("episodioId")))
    for run, fuente in CORRIDAS.items():
        a = spark.read.parquet(str(DATOS / "aumentados" / f"runId={run}"))
        partes.append(a.select(
            F.lit(fuente).alias("fuente"), "origen", "sensorId", "eventId", F.col("timestamp").cast("long"),
            F.lit(run).alias("runId"), F.lit("parquet").alias("origen_dato"), "distanciaCm",
            F.col("nivelPct").alias("nivelPct_fuente"), F.lit(None).cast("double").alias("nivelM"), "temperaturaC",
            "humedadPct", F.lit(None).cast("double").alias("tempAguaC"), F.lit(None).cast("double").alias("caudalM3s"),
            "esAnomalia", "tipoAnomalia", "episodioId"))
    todo = partes[0]
    for p in partes[1:]:
        todo = todo.unionByName(p)
    filas_crudas = todo.groupBy("fuente", "origen_dato").count().toPandas()

    # Plata: deduplicado por eventId (prioridad: fuente original sobre la copia de Kafka), sin sensores de prueba
    prio = F.create_map(*[x for k, v in PRIORIDAD.items() for x in (F.lit(k), F.lit(v))])
    w = Window.partitionBy("eventId", F.coalesce("runId", F.lit(""))).orderBy(prio[F.col("origen_dato")], "timestamp")
    dedup = (todo.filter(~F.col("sensorId").isin(prueba))
             .withColumn("_n", F.row_number().over(w)).filter("_n = 1").drop("_n")
             # ThingSpeak llega por la API completa: la copia de Kafka de esos mismos canales sobra
             .filter(~((F.col("fuente") == "thingspeak") & (F.col("origen_dato") == "kafka-bronce")
                       & F.col("sensorId").isin([f"thingspeak-{c}" for c in json.loads((FUENTES / "f6_thingspeak.json").read_text(encoding="utf-8"))]))))
    plata = dedup.mapInPandas(a_plata, PLATA)
    plata.write.mode("overwrite").partitionBy("fuente").parquet(str(DATOS / "plata"))
    plata = spark.read.parquet(str(DATOS / "plata"))
    log(status="plata", filas=plata.count())

    # Partición: límites por canal y por corrida
    from particion import Particion
    lim_ts = {r["sensorId"]: r["m"] for r in plata.filter("fuente = 'thingspeak'").groupBy("sensorId")
              .agg(F.max("timestamp").alias("m")).collect()}
    ini_run = {r["runId"]: r["m"] for r in plata.filter(F.col("runId").isNotNull()).groupBy("runId")
               .agg(F.min("timestamp").alias("m")).collect()}
    part = Particion(limites_thingspeak=lim_ts, inicio_corridas=ini_run)

    # Oro: por sensor (y corrida) con applyInPandas; solo tanques (ESP32, ThingSpeak, flota)
    om = (plata.filter("fuente = 'open-meteo' and sensorId = 'open-meteo-juliaca'")  # no la ubicación de 3418755
          .select("timestamp", "temperaturaC").toPandas())
    juliaca = pd.DataFrame({"hora": om["timestamp"] // 3_600_000 * 3_600_000, "temp_juliaca_c": om["temperaturaC"]}) \
        .groupby("hora", as_index=False).mean()
    tau = yaml.safe_load((RAIZ / "streaming" / "reglas_servicio.yml").read_text(encoding="utf-8"))["umbral_termico"]["tau_h"]
    salida = (plata.filter(F.col("fuente").isin("esp32", "thingspeak") | F.col("fuente").startswith("aumentado"))
              .groupBy("fuente", "sensorId", "runId")
              .applyInPandas(a_oro(juliaca, tau, bitacora()), "tabla string, fuente string, sensorId string, runId string, payload string")
              .toPandas())
    resumen_oro = {}
    for tabla, g in salida.groupby("tabla"):
        filas = [{"fuente": r.fuente, "sensorId": r.sensorId, "runId": r.runId, **json.loads(r.payload)} for r in g.itertuples()]
        t = pd.DataFrame(filas)
        t["particion"] = [part.asignar(fu, s, ru, int(ts)) for fu, s, ru, ts in zip(t["fuente"], t["sensorId"], t["runId"], t["ts"])]
        t["particion_version"] = part.version()
        for c in t.columns:  # columnas sin ningún valor: tipo string para que Spark no falle al inferir
            if t[c].isna().all():
                t[c] = t[c].astype("string")
        sdf = spark.createDataFrame(t.astype(object).where(t.notna(), None))
        sdf.write.mode("overwrite").partitionBy("fuente").parquet(str(DATOS / "oro" / tabla))
        resumen_oro[tabla] = {"filas": len(t), "por_fuente": t.groupby("fuente").size().to_dict(),
                              "por_particion": t.groupby("particion").size().to_dict(), "columnas": list(t.columns)}
        log(status="oro", tabla=tabla, filas=len(t))

    # Calidad
    q = (plata.groupBy("fuente", "sensorId").agg(
        F.count("*").alias("filas"), F.sum(F.col("lectura_valida").cast("int")).alias("validas"),
        F.min("timestamp").alias("desde"), F.max("timestamp").alias("hasta"),
        F.countDistinct("origen_dato").alias("origenes_dato")).toPandas())
    motivos = plata.filter("not lectura_valida").groupBy("fuente", "motivo_invalidez").count().toPandas()
    esp = plata.filter("fuente = 'esp32'").select("timestamp", "origen_dato", "lectura_valida").toPandas().sort_values("timestamp")
    import oro
    esp["tramo"] = oro.tramos(esp["timestamp"].values, oro.GAP_MS["esp32"])
    tr = esp.groupby("tramo").agg(desde=("timestamp", "min"), hasta=("timestamp", "max"), lecturas=("timestamp", "size"),
                                  validas=("lectura_valida", "sum")).reset_index()
    tr["minutos"] = (tr["hasta"] - tr["desde"]) / 60000
    gaps = pd.Series(esp["timestamp"].values[1:] - esp["timestamp"].values[:-1]) / 60000
    atrasadas = (bronce.filter("origen = 'esp32-fisico'")
                 .filter((F.unix_millis(F.col("kafka_ts").cast("timestamp")) - F.col("timestamp")) > 5 * 60000).count())
    calidad = {
        "generado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "particion": part.resumen(),
        "filas_crudas_por_origen_dato": filas_crudas.to_dict("records"),
        "plata_por_sensor": q.to_dict("records"),
        "invalidas_por_motivo": motivos.to_dict("records"),
        "oro": resumen_oro,
        "esp32_tramos": {"umbral_hueco_min": oro.GAP_MS["esp32"] / 60000, "tramos": len(tr),
                         "tramos_mayores_30_min": tr[tr["minutos"] >= 30].to_dict("records"),
                         "huecos_mayores_5_min": int((gaps > 5).sum()), "huecos_mayores_1_h": int((gaps > 60).sum()),
                         "hueco_max_h": float(gaps.max() / 60) if len(gaps) else None},
        "esp32_llegada_tardia_a_kafka": {"lecturas_con_mas_de_5_min": atrasadas,
                                          "nota": "llegaron tarde a Kafka (puesta en marcha del puente, suspensiones de la PC). Plata usa el timestamp del ESP32 (Firebase): el atraso no crea huecos"},
    }
    (DATOS / "calidad.json").write_text(json.dumps(calidad, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    ev = RAIZ / "docs" / "evidencias" / "f6"
    ev.mkdir(parents=True, exist_ok=True)
    shutil.copy(DATOS / "calidad.json", ev / "calidad.json")
    log(status="listo", calidad=str(DATOS / "calidad.json"))
    spark.stop()


if __name__ == "__main__":
    main()
