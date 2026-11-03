"""
Escenarios de volumen y costo mensual (F8, criterio 5). Todo sale de dos tablas de este archivo:

  MEDIDO   constantes medidas en el stack el 2026-10-30 (fuente en docs/costos-escalado.md, tabla de volumen)
  PRECIOS  precios de lista de las páginas oficiales, consultados el 2026-10-29/30 (URL en docs/costos-escalado.md)

y de la capacidad de Spark medida en la prueba de carga (--capacidad, eventos/s por núcleo sin lag creciente).

  .venv/Scripts/python infra/costos_escenarios.py --capacidad 250 [--cadencia-s 5] [--md]
"""
import argparse
import math

MEDIDO = {
    "bytes_kafka_evento": 369,       # 9,417,396 B / 25,533 mensajes retenidos de aqua-eventos-real (kafka_volumen)
    "bytes_firebase_registro": 175,  # 5,290,404 B de JSON / 30,203 registros en /board1/lecturas/historial
    "bytes_parquet_fila": 386,       # bronce: 10,874,996 B / 28,200 filas (archivos chicos, sin compactar)
    "valores_influx_evento": 14.3,   # ESP32: ~197,000 valores/día en InfluxDB por 13,795 lecturas/día
    "bytes_linea_valor": 20,         # SUPUESTO: bytes de line protocol por valor (medida, tags, campo y tiempo)
    "series_por_tanque": 20,         # influxdb.cardinality: flota-zonaA-01 con predicciones (ESP32: 16)
    "eventos_api_dia": 5_320,        # ThingSpeak + EA + USGS + Open-Meteo en bronce, 29-oct
    "eventos_actual_dia": 19_115,    # todo aqua-eventos-real en bronce, 29-oct (ESP32 con huecos de Wi-Fi)
    "lectores_topico": 2,            # el job lee aqua-eventos-real dos veces (consultas generales + predicciones)
    "retencion_kafka_d": 7,
}
PRECIOS = {  # USD
    "confluent_basic_ecku_h": 0.14,          # primer eCKU gratis
    "confluent_standard_ecku_h": 0.75,
    "confluent_gb_red": 0.05,                # ingreso y egreso
    "confluent_gb_mes": 0.08,
    "msk_m7g_large_h": 0.204,                # us-east-1
    "msk_gb_mes": 0.10,
    "emr_vcpu_h": 0.052624,                  # EMR Serverless x86, us-east-1
    "emr_gb_h": 0.0057785,
    "gcp_dcu_h": 0.06,                       # Serverless for Apache Spark, estándar; 0.6 DCU/vCPU + 0.1 DCU/GiB
    "influx_mb_escrito": 0.0025,             # InfluxDB Cloud Usage-Based
    "influx_gb_h": 0.002,
    "influx_free_mb_5min": 5,                # plan Free: 5 MB cada 5 min, 10k series, 30 días
    "influx_free_series": 10_000,
    "grafana_pro_mes": 19,                   # desde; Free: 10k series activas, 3 usuarios
    "firebase_free_gb": 1,                   # Spark: 1 GB guardado, 10 GB/mes de descarga, 100 conexiones
    "firebase_gb_mes": 5,                    # Blaze, sobre 1 GB
    "firebase_gb_descarga": 1,               # Blaze, sobre ~10 GB/mes
}
HORAS_MES = 730
ESCENARIOS = [  # (nombre, tanques). Supuesto: un tanque monitoreado por conexión
    ("Actual (1 ESP32 + 7 ThingSpeak + APIs)", None),
    ("100 tanques", 100),
    ("Juliaca conservador: 35,160 viviendas con red pública (INEI 2017)", 35_160),
    ("Juliaca principal: 53,724 conexiones activas (SUNASS, oct-2023)", 53_724),
    ("Juliaca principal, diseño a escala", 53_724),
]
# Palancas del diseño a escala: lectura cada 60 s (el nivel de un tanque no cambia en 5 s de forma útil), el tanque
# publica directo a Kafka por un gateway MQTT/HTTP (sin Firebase) e InfluxDB guarda agregados de 1 min, no cada lectura
A_ESCALA = {"cadencia_s": 60, "firebase": False, "valores_influx_evento": 10}


def escenario(tanques, capacidad, cadencia_s, a_escala=False):
    m, p = dict(MEDIDO), PRECIOS
    if a_escala:
        cadencia_s = A_ESCALA["cadencia_s"]
        m["valores_influx_evento"] = A_ESCALA["valores_influx_evento"]
    ev_dia = m["eventos_actual_dia"] if tanques is None else tanques * 86_400 / cadencia_s + m["eventos_api_dia"]
    eps = ev_dia / 86_400
    n_tanques = 1 + 7 if tanques is None else tanques
    gb_dia_kafka = ev_dia * m["bytes_kafka_evento"] / 1e9
    gb_kafka_retenido = gb_dia_kafka * m["retencion_kafka_d"]
    # Spark: núcleos para el doble de la carga (holgura para recargas y picos), en ejecutores de 4 núcleos
    nucleos = max(2, math.ceil(2 * eps / capacidad))
    ejecutores = math.ceil(nucleos / 4)
    particiones = max(3, 3 * math.ceil(nucleos / 3))   # una tarea por partición y núcleo
    brokers = 1 if tanques is None else 3               # 3 = factor de replicación 3 en producción
    mb_influx_dia = ev_dia * m["valores_influx_evento"] * m["bytes_linea_valor"] / 1e6
    series = n_tanques * m["series_por_tanque"] + 100   # + métricas del pipeline
    gb_mes_parquet = ev_dia * 30 * m["bytes_parquet_fila"] / 1e9
    gb_mes_firebase = ev_dia * 30 * m["bytes_firebase_registro"] / 1e9
    r = {"tanques": n_tanques, "eventos_dia": ev_dia, "eps": eps, "particiones": particiones, "brokers": brokers,
         "nucleos_spark": nucleos, "ejecutores": ejecutores, "gb_mes_kafka_ingreso": gb_dia_kafka * 30,
         "gb_kafka_retenido": gb_kafka_retenido, "gb_mes_parquet": gb_mes_parquet, "gb_mes_firebase": gb_mes_firebase,
         "mb_dia_influx": mb_influx_dia, "series_influx": series}
    # --- Costos mensuales ---
    red = gb_dia_kafka * 30 * (1 + m["lectores_topico"])
    ecku = 1 if eps < 1_000 else math.ceil(eps / 5_000)  # SUPUESTO: un eCKU por cada ~5,000 ev/s (~2 MB/s)
    r["usd_confluent_basic"] = (max(0, ecku - 1) * p["confluent_basic_ecku_h"] * HORAS_MES + red * p["confluent_gb_red"]
                                + gb_kafka_retenido * p["confluent_gb_mes"])
    r["usd_msk"] = brokers * p["msk_m7g_large_h"] * HORAS_MES + gb_kafka_retenido * brokers * p["msk_gb_mes"]
    vcpu, gb_mem = nucleos + 2, (nucleos + 2) * 4                     # + driver de 2 vCPU; 4 GB por vCPU
    r["usd_emr"] = HORAS_MES * (vcpu * p["emr_vcpu_h"] + gb_mem * p["emr_gb_h"])
    r["usd_gcp_spark"] = HORAS_MES * (0.6 * vcpu + 0.1 * gb_mem) * p["gcp_dcu_h"]
    r["influx_free_alcanza"] = (mb_influx_dia / 288 <= p["influx_free_mb_5min"]) and series <= p["influx_free_series"]
    r["usd_influx"] = 0.0 if r["influx_free_alcanza"] else (mb_influx_dia * 30 * p["influx_mb_escrito"]
                                                            + mb_influx_dia * 30 / 1e3 * 0.1 * p["influx_gb_h"] * HORAS_MES)
    r["usd_grafana"] = p["grafana_pro_mes"]   # el equipo son 4 personas: Free solo admite 3 usuarios activos
    if a_escala:
        r["gb_mes_firebase"], r["firebase_free_alcanza"], r["usd_firebase"] = 0.0, True, 0.0
        return r
    r["firebase_free_alcanza"] = gb_mes_firebase * 3 <= p["firebase_free_gb"]   # 3 meses de historial
    r["usd_firebase"] = 0.0 if r["firebase_free_alcanza"] else (
        max(0, gb_mes_firebase * 3 - p["firebase_free_gb"]) * p["firebase_gb_mes"]
        + max(0, gb_mes_firebase - 10) * p["firebase_gb_descarga"])
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--capacidad", type=float, required=True, help="eventos/s por núcleo (prueba de carga)")
    ap.add_argument("--cadencia-s", type=float, default=5, help="segundos entre lecturas de un tanque (ESP32: 5)")
    a = ap.parse_args()
    filas = [(n, escenario(t, a.capacidad, a.cadencia_s, "a escala" in n)) for n, t in ESCENARIOS]
    print(f"cadencia {a.cadencia_s:g} s, capacidad {a.capacidad:g} ev/s por núcleo")
    claves = list(filas[0][1])
    print("| | " + " | ".join(n for n, _ in filas) + " |")
    print("|---|" + "---:|" * len(filas))
    for k in claves:
        vals = []
        for _, r in filas:
            v = r[k]
            vals.append(("sí" if v else "no") if isinstance(v, bool) else f"{v:,.2f}" if isinstance(v, float) and v < 100
                        else f"{v:,.0f}")
        print(f"| {k} | " + " | ".join(vals) + " |")


if __name__ == "__main__":
    main()
