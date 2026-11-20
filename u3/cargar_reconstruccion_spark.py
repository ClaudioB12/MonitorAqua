"""
U3 · D4 · Reconstrucción ROTULADA de los huecos del ESP32, paso 2 de 2: Kafka -> PySpark -> aumentados.

Lee con Spark (lectura batch del conector Kafka) el tópico aqua-eventos-reconstruidos que publicó
u3/reconstruir_huecos_esp32.py, aplica el contrato y la deduplicación por eventId (la entrega es al-menos-una-vez:
re-publicar no duplica) y escribe:
  - Parquet en batch/datos/aumentados/runId=<runId>/fecha=<día de Lima>/
  - InfluxDB, SOLO en el bucket monitoraqua_aumentados: nivel_tanque, lecturas_agg, estado_tanque y los eventos de
    servicio del detector congelado (f4.4), igual que streaming/cargar_historico_influx.py con la flota.
Nunca escribe en Firebase, en aqua-eventos-real ni en el bucket real.

Uso (CMD, raíz del repo):
  docker run --rm --network lambda26-kafka-net --env-file .env -e INFLUX_URL_INTERNA=http://influxdb:8086 ^
    -v "%cd%:/repo" -w /repo monitoraqua-streaming:latest ^
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 --driver-memory 2g ^
    --conf spark.log.level=ERROR u3/cargar_reconstruccion_spark.py

Opciones:
  --run-id   runId a cargar (por defecto, el más reciente del tópico).
  --seco     lee y valida desde Kafka y escribe el Parquet, pero no escribe en InfluxDB.
Responsable: Wilbert Alex Mayta Arotaype (D4).
"""
import argparse
import json
import os
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reconstruir_huecos_esp32 import (KAFKA_BOOTSTRAP, LIMA, MANIFIESTO, ORIGEN, RAIZ, REAL, RECON,  # noqa: E402
                                      TOPICO, sesion_spark)


def esquema():
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType, StructField, StructType)
    s, d, l, b = StringType(), DoubleType(), LongType(), BooleanType()
    return StructType([StructField(n, t) for n, t in [
        ("tipoEvento", s), ("eventId", s), ("sensorId", s), ("origen", s), ("fuenteTipo", s), ("runId", s),
        ("timestamp", l), ("ingestaTs", l), ("distanciaCm", d), ("temperaturaC", d), ("humedadPct", d),
        ("reconstruido", b), ("metodo", s), ("donanteDia", s), ("esAnomalia", b)]])


def leer_kafka(spark, run_id):
    from pyspark.sql import functions as F
    crudo = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", TOPICO).option("startingOffsets", "earliest").option("endingOffsets", "latest")
             .load())
    return procesar_crudo(crudo, run_id)


def procesar_crudo(crudo, run_id):
    """Mensajes de Kafka (value JSON, partition, offset) -> eventos únicos del runId que cumplen el contrato."""
    from pyspark.sql import functions as F
    ev = crudo.select(F.from_json(F.col("value").cast("string"), esquema()).alias("e"),
                      "partition", "offset").select("e.*", "partition", "offset").cache()
    total = ev.count()
    if total == 0:
        sys.exit(f"El tópico {TOPICO} está vacío: corre primero u3/reconstruir_huecos_esp32.py")
    if run_id is None:
        run_id = ev.agg(F.max("runId")).first()[0]
    ev = ev.filter(F.col("runId") == run_id)
    # contrato: solo lo rotulado como reconstruido pasa (protección contra mezclar con lo real)
    valido = ((F.col("sensorId") == RECON) & (F.col("origen") == ORIGEN) & (F.col("fuenteTipo") == "aumentado")
              & (F.col("reconstruido") == F.lit(True)) & F.col("eventId").isNotNull() & F.col("timestamp").isNotNull()
              & F.col("distanciaCm").isNotNull())
    del_run = ev.count()
    validos = ev.filter(valido)
    n_validos = validos.count()
    unicos = validos.dropDuplicates(["eventId"])
    n_unicos = unicos.count()
    offsets = {r["partition"]: [r["min"], r["max"]] for r in
               crudo.groupBy("partition").agg(F.min("offset").alias("min"), F.max("offset").alias("max")).collect()}
    info = {"topico": TOPICO, "mensajesTopico": total, "runId": run_id, "mensajesRun": del_run,
            "rechazadosContrato": del_run - n_validos, "duplicadosEventId": n_validos - n_unicos,
            "eventosUnicos": n_unicos, "offsetsPorParticion": offsets}
    return unicos.drop("partition", "offset"), info


def escribir_parquet(df, run_id):
    from pyspark.sql import functions as F
    destino = RAIZ / "batch" / "datos" / "aumentados" / f"runId={run_id}"
    if destino.exists():
        shutil.rmtree(destino)                                       # idempotente: misma corrida, mismo resultado
    (df.withColumn("fecha", F.date_format(F.from_utc_timestamp(F.timestamp_millis("timestamp"), LIMA), "yyyy-MM-dd"))
       .drop("runId").coalesce(1).write.partitionBy("fecha").parquet(str(destino)))
    return destino


def cargar_influx(rec):
    """Detector congelado + puntos nivel_tanque, estado_tanque, eventos_servicio y lecturas_agg, solo en el bucket
    de datos aumentados (como streaming/cargar_historico_influx.py hace con la flota)."""
    sys.path.insert(0, str(RAIZ / "streaming"))
    os.environ.setdefault("INFLUX_URL_INTERNA", "http://influxdb:8086")
    import influx
    import tablero
    from cargar_historico_influx import BUCKET_AUMENTADOS, FOTO_CADA_MS, asegurar_bucket, detector
    det = detector()
    cfg = det.cfg.refrescar()
    cfg.tanques[RECON] = dict(cfg.tanques[REAL])     # misma geometría (provisional) que el tanque real
    lecturas = rec.to_dict("records")
    lineas, estado, ultima_foto, eventos = [], None, 0, []
    for lec in lecturas:
        lineas.append(tablero.linea_nivel(det, lec, RECON, "aumentado"))
        lineas.append(influx.linea("lecturas_agg", {"sensorId": RECON, "origen": ORIGEN,
                                                    "fuenteTipo": "aumentado", "ventana": "1m"},
                                   {"n_total": 1, "n_validas": 1, "n_invalidas": 0, "es_dato_real": False},
                                   lec["timestamp"]))
    j = 0
    while j < len(lecturas):
        k = j
        while k < len(lecturas) and lecturas[k]["timestamp"] - lecturas[j]["timestamp"] < FOTO_CADA_MS:
            k += 1
        evs, estado = det.procesar(RECON, lecturas[j:k], estado, "aumentado", int(time.time() * 1000))
        eventos += evs or []
        if estado.get("ult_ts", 0) - ultima_foto >= FOTO_CADA_MS:
            lineas.append(tablero.linea_estado(RECON, estado, "aumentado"))
            ultima_foto = estado["ult_ts"]
        j = k
    lineas += [tablero.linea_evento(e) for e in eventos]
    info_bucket = asegurar_bucket(BUCKET_AUMENTADOS)
    n = influx.escribir([l for l in lineas if l], bucket=BUCKET_AUMENTADOS)
    tipos = {}
    for e in eventos:
        tipos[e.get("tipo")] = tipos.get(e.get("tipo"), 0) + 1
    return {"bucket": f"{BUCKET_AUMENTADOS} ({info_bucket})", "puntos": n, "eventos": tipos}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--seco", action="store_true")
    a = ap.parse_args()

    spark = sesion_spark("u3-d4-reconstruccion-cargar")
    t0 = time.time()
    df, kafka = leer_kafka(spark, a.run_id)
    print(f"Kafka {TOPICO}: {kafka['mensajesTopico']:,} mensajes · runId {kafka['runId']}: {kafka['mensajesRun']:,} · "
          f"rechazados por contrato {kafka['rechazadosContrato']:,} · eventId repetidos {kafka['duplicadosEventId']:,} "
          f"· únicos {kafka['eventosUnicos']:,} ({time.time() - t0:.0f} s)")
    if kafka["rechazadosContrato"]:
        print(f"AVISO: {kafka['rechazadosContrato']:,} mensajes del runId no cumplen el contrato reconstruido y se "
              "descartaron (no se cargan).")
    destino = escribir_parquet(df, kafka["runId"])
    print(f"Parquet: {destino.relative_to(RAIZ)}")

    influx_info = "no se escribió (--seco)"
    if not a.seco:
        rec = (df.select("sensorId", "timestamp", "distanciaCm", "temperaturaC", "humedadPct")
                 .orderBy("timestamp").toPandas())
        rec["humedadPct"] = rec["humedadPct"].round().astype(int)
        influx_info = cargar_influx(rec)
        print("InfluxDB:", json.dumps(influx_info, ensure_ascii=False))
    spark.stop()

    manifiesto = json.loads(MANIFIESTO.read_text(encoding="utf-8")) if MANIFIESTO.exists() else {}
    manifiesto["carga"] = {"kafka": kafka, "parquet": str(destino.relative_to(RAIZ)), "influx": influx_info,
                           "cargadoUtc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    MANIFIESTO.parent.mkdir(parents=True, exist_ok=True)
    MANIFIESTO.write_text(json.dumps(manifiesto, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Resumen:", MANIFIESTO.relative_to(RAIZ))


if __name__ == "__main__":
    main()
