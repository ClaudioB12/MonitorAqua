"""
Generador de datos aumentados: flota extrapolada de tanques (MonitorAqua, F2b).

Simula, minuto a minuto, el volumen de agua de cada tanque (consumo, llegadas de agua por franja, llenado
limitado por el flotador) y lo que mediría un HC-SR04 sobre él (ruido y saltos del perfil real, deriva
térmica por la velocidad del sonido, -1 de falla). Inyecta y etiqueta cinco tipos de anomalía.

Los parámetros salen de batch/perfiles/perfil_real.json y de ingesta/generador.yml (los que el perfil no
permite estimar están marcados como supuestos). Misma configuración + misma semilla = mismos eventos.

Salidas (ninguna toca Firebase ni aqua-eventos-real):
  --parquet  batch/datos/aumentados/runId=<id>/fecha=<YYYY-MM-DD>/   (reescribe la corrida: sin duplicados)
  --kafka    tópico aqua-eventos-aumentados (3 particiones, clave sensorId)

Uso:
  python ingesta/generador.py --parquet                 genera y guarda en Parquet
  python ingesta/generador.py --parquet --kafka         y además publica (tasa por defecto 2000 ev/s)
  python ingesta/generador.py --tanques 8 --dias 15 --intervalo-s 300 --semilla 7 --anomalias 0.08 --seco
"""
import argparse
import hashlib
import json
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
from comun import cargar_validador, errores_contrato  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
TOPICO = os.getenv("KAFKA_TOPIC_AUMENTADOS", "aqua-eventos-aumentados")
LIMA = timezone(timedelta(hours=-5))
VERSION = "f2b.1"  # entra en el runId: si cambia el modelo, cambia la corrida
TIPOS = ["fuga_lenta", "corte_prolongado", "deriva_termica", "llegada_atipica", "falla_sensor"]


def log(**campos):
    print(json.dumps({"service": "aqua-generador", **campos}, ensure_ascii=False, default=str), flush=True)


def velocidad_sonido(temp_c):
    return 331.3 + 0.606 * temp_c


# --- Configuración ---------------------------------------------------------------------------

def cargar_config(args):
    cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
    c = cfg["corrida"]
    for clave, valor in (("semilla", args.semilla), ("dias", args.dias), ("intervalo_s", args.intervalo_s),
                         ("proporcion_anomalias", args.anomalias)):
        if valor is not None:
            c[clave] = valor
    if args.tanques is not None:
        base = cfg["tanques"]
        # Más tanques: se replican las plantillas en orden, con sensorId nuevo
        cfg["tanques"] = [{**base[i % len(base)], "sensorId": f"flota-{base[i % len(base)]['zona']}-{i + 1:02d}"}
                          for i in range(args.tanques)]
    # Variantes para el análisis de sensibilidad (F4). Solo entran a la configuración (y al runId) si se pasan.
    if getattr(args, "factor_ruido", None) is not None:
        cfg["medicion"]["factor_ruido"] = args.factor_ruido
    if getattr(args, "caudal_llenado", None) is not None:
        cfg["suministro"]["caudal_llenado_lpm"] = args.caudal_llenado
    if getattr(args, "franja_atipica", None):
        cfg["anomalias"]["llegada_atipica"]["franja"] = list(args.franja_atipica)
    perfil = json.loads((RAIZ / cfg["perfil"]).read_text(encoding="utf-8"))
    # El modelo f2b.2 se activa con su propio YAML (version_modelo: f2b.2); sin esa clave, el runId de f2b.1 no cambia
    huella = hashlib.sha1(json.dumps({"version": cfg.get("version_modelo", VERSION), **cfg},
                                     sort_keys=True).encode()).hexdigest()[:8]
    run_id = f"r{c['semilla']}-{huella}"
    return cfg, perfil, run_id


def parametros_perfil(perfil, cfg):
    """Lo que se toma del perfil real (con su respaldo si falta)."""
    esp = perfil.get("esp32") or {}
    deltas = esp.get("delta_distanciaCm_lecturas_consecutivas_le_7s", {})
    clima = perfil.get("juliaca_clima_open_meteo") or {}
    ciclo = clima.get("ciclo_hora_lima", {})
    if len(ciclo) != 24:
        raise SystemExit("perfil_real.json no trae el ciclo horario de Juliaca: corre batch/perfilar.py")
    m = cfg["medicion"]
    # factor_ruido multiplica el ruido del perfil real: σ de las lecturas y frecuencia de los saltos > 2 cm
    # (los -1 no cambian: son fallas etiquetadas, no ruido)
    f = m.get("factor_ruido", 1.0)
    return {
        "ruido_sigma_cm": f * (deltas.get("ruido_abs_le_2cm", {}).get("desv_std", 0.3) if m["intervalo_ruido_desde_perfil"] else 0.3),
        "saltos_tasa": min(0.5, f * (deltas.get("saltos_abs_gt_2cm", {}).get("tasa", 0.02) if m["saltos_tasa_desde_perfil"] else 0.0)),
        "temp_hora": np.array([ciclo[str(h)]["temperaturaC_media"] for h in range(24)]),
        "hum_hora": np.array([ciclo[str(h)]["humedadPct_media"] for h in range(24)]),
        "min_diaria_desv": clima.get("minima_diaria_C", {}).get("desv_std", 1.5),
    }


# --- Programación de llegadas y anomalías ----------------------------------------------------

def programar(cfg, run_id, tiempos, hora_local, dia_idx, rng):
    c, sumi, anom = cfg["corrida"], cfg["suministro"], cfg["anomalias"]
    n_t, n = len(cfg["tanques"]), len(tiempos)
    paso_h = c["intervalo_s"] / 3600
    pasos = lambda horas: max(1, int(round(horas / paso_h)))  # noqa: E731
    dias = int(dia_idx.max()) + 1
    inicio_dia = np.searchsorted(dia_idx, np.arange(dias))

    # Llegadas normales: una por día con probabilidad p, dentro de la franja de la zona
    llegadas = []  # por tanque: {dia: (paso_inicio, paso_fin)}
    for tq in cfg["tanques"]:
        lo, hi = sumi["zonas"][tq["zona"]]["franja"]
        ll = {}
        for d in range(dias):
            if rng.random() < sumi["probabilidad_llegada_diaria"]:
                ini = inicio_dia[d] + pasos(rng.uniform(lo, hi))
                ll[d] = (ini, min(n, ini + pasos(sumi["horas_servicio"])))
        llegadas.append(ll)

    etiqueta = np.full((n_t, n), -1, dtype=np.int8)      # índice en TIPOS, -1 = normal
    episodio = np.full((n_t, n), -1, dtype=np.int32)     # número de episodio
    fuga_lph = np.zeros((n_t, n))
    frio_extra = np.zeros((n_t, n))
    falla_modo = np.zeros((n_t, n), dtype=np.int8)       # 1 = -1, 2 = salto imposible
    episodios = []

    def libre(t, a, b):
        return a >= 0 and b <= n and b > a and not (etiqueta[t, a:b] >= 0).any()

    def sin_llegada(t, a, b):
        # Una fuga o una helada que se superponen a un llenado quedan tapadas por él: se evitan
        return not any(x < b and a < y for x, y in llegadas[t].values())

    def cabe(asignados, largo, cuota, tipo=None):
        # El primer episodio de un tipo siempre entra; los siguientes no pueden pasar la cuota en más de 10 %,
        # salvo que el tipo pida un mínimo de episodios (f2b.2: fugas largas, para tener con qué medir el recall)
        if tipo and sum(1 for ep in episodios if ep["tipo"] == tipo) < anom.get(tipo, {}).get("episodios_min", 0):
            return True
        return asignados == 0 or asignados + largo <= cuota * 1.1

    def marcar(t, a, b, tipo):
        etiqueta[t, a:b] = TIPOS.index(tipo)
        episodio[t, a:b] = len(episodios)
        episodios.append({"episodioId": f"{run_id}-{tipo}-{len(episodios) + 1:04d}", "tipo": tipo,
                          "sensorId": cfg["tanques"][t]["sensorId"], "inicio": int(tiempos[a]),
                          "fin": int(tiempos[b - 1]), "eventos": int(b - a)})

    total = n_t * n
    aislados = (cfg["medicion"]["falla_distancia_tasa"] + cfg["medicion"]["falla_dht_tasa"]) * total
    presupuesto = max(0.0, c["proporcion_anomalias"] * total - aislados)

    omitidas_sin_etiqueta = [0]
    for tipo in TIPOS:
        # f2b.2: toda llegada que no ocurre (también las "normales" del 8 % de días sin agua) es un corte etiquetado,
        # desde la hora en que la franja de ese día empezaba hasta la siguiente llegada real
        if tipo == "corte_prolongado" and anom["corte_prolongado"].get("etiquetar_omitidas"):
            for t, tq in enumerate(cfg["tanques"]):
                lo = sumi["zonas"][tq["zona"]]["franja"][0]
                dias_con = sorted(llegadas[t])
                d = 0
                while d < dias:
                    if d in llegadas[t] or d == 0:
                        d += 1
                        continue
                    a = inicio_dia[d] + pasos(lo)
                    siguiente = next((x for x in dias_con if x > d), None)
                    b = llegadas[t][siguiente][0] if siguiente is not None else n
                    if libre(t, a, b):
                        marcar(t, a, b, "corte_prolongado")
                    else:
                        omitidas_sin_etiqueta[0] += 1  # cae dentro de otro episodio (p. ej. una fuga larga)
                    d = siguiente if siguiente is not None else dias
        cuota, intentos = anom["reparto"][tipo] * presupuesto, 0
        asignados = int((etiqueta == TIPOS.index(tipo)).sum())  # f2b.2: los cortes omitidos ya cuentan
        p = anom[tipo]
        minimo = anom.get(tipo, {}).get("episodios_min", 0)
        while (asignados < cuota or sum(1 for ep in episodios if ep["tipo"] == tipo) < minimo) and intentos < 5000:
            intentos += 1
            t = int(rng.integers(n_t))
            if tipo == "fuga_lenta":
                a = int(rng.integers(n))
                b = a + pasos(rng.uniform(*p["duracion_h"]))
                if libre(t, a, b) and (p.get("permitir_con_llenado") or sin_llegada(t, a, b))                         and cabe(asignados, b - a, cuota, tipo):
                    marcar(t, a, b, tipo)
                    fuga_lph[t, a:b] = rng.uniform(*p["litros_hora"])
                    asignados += b - a
            elif tipo == "deriva_termica":
                d = int(rng.integers(dias - 1))
                a = inicio_dia[d] + pasos(p["inicio_h"])
                largo = pasos(rng.uniform(*p["duracion_h"]))
                b = a + largo
                if libre(t, a, b) and sin_llegada(t, a, b) and cabe(asignados, b - a, cuota):
                    marcar(t, a, b, tipo)
                    # Helada atípica: el frío extra entra y sale en rampa (1/4 del episodio cada una)
                    forma = np.minimum(1.0, np.minimum(np.arange(1, largo + 1), np.arange(largo, 0, -1)) / (largo / 4))
                    frio_extra[t, a:b] = rng.uniform(*p["descenso_extra_c"]) * forma
                    asignados += b - a
            elif tipo == "falla_sensor":
                a = int(rng.integers(n))
                b = a + pasos(rng.uniform(*p["duracion_min"]) / 60)
                if libre(t, a, b) and cabe(asignados, b - a, cuota):
                    marcar(t, a, b, tipo)
                    falla_modo[t, a:b] = 1 if rng.random() < 0.5 else 2
                    asignados += b - a
            elif tipo == "corte_prolongado":
                dias_con = sorted(llegadas[t])
                if len(dias_con) < 3:
                    continue
                d = dias_con[int(rng.integers(1, len(dias_con) - 1))]
                k = int(rng.integers(p["dias_sin_llegada"][0], p["dias_sin_llegada"][1] + 1))
                quitados = [x for x in range(d, d + k) if x in llegadas[t]]
                siguiente = next((x for x in dias_con if x >= d + k), None)
                if not quitados or siguiente is None:
                    continue
                a, b = llegadas[t][quitados[0]][0], llegadas[t][siguiente][0]
                if libre(t, a, b) and cabe(asignados, b - a, cuota):
                    for x in quitados:
                        del llegadas[t][x]
                    marcar(t, a, b, tipo)  # desde que la llegada esperada no ocurre hasta la siguiente
                    asignados += b - a
            elif tipo == "llegada_atipica":
                dias_con = sorted(llegadas[t])
                if not dias_con:
                    continue
                d = dias_con[int(rng.integers(len(dias_con)))]
                ini = inicio_dia[d] + pasos(rng.uniform(*p["franja"]))
                fin = min(n, ini + pasos(cfg["suministro"]["horas_servicio"]))
                b = min(n, fin + pasos(p["margen_post_llenado_min"] / 60))
                normal = llegadas[t][d]
                if libre(t, ini, b) and cabe(asignados, b - ini, cuota) and not (etiqueta[t, normal[0]:normal[1]] >= 0).any():
                    llegadas[t][d] = (ini, fin)  # reemplaza la llegada normal de ese día
                    marcar(t, ini, b, tipo)
                    asignados += b - ini
    suministro = np.zeros((n_t, n), dtype=bool)
    for t, ll in enumerate(llegadas):
        for a, b in ll.values():
            suministro[t, a:b] = True
    return {"etiqueta": etiqueta, "episodio": episodio, "fuga_lph": fuga_lph, "frio_extra": frio_extra,
            "falla_modo": falla_modo, "suministro": suministro, "episodios": episodios,
            "omitidas_sin_etiqueta": omitidas_sin_etiqueta[0]}


# --- Simulación ------------------------------------------------------------------------------

def generar(cfg, perfil, run_id):
    c, med, cons = cfg["corrida"], cfg["medicion"], cfg["consumo"]
    rng = np.random.default_rng(c["semilla"])
    pp = parametros_perfil(perfil, cfg)
    inicio = datetime.fromisoformat(c["inicio"])
    n = int(c["dias"] * 86400 // c["intervalo_s"])
    tiempos = (int(inicio.timestamp() * 1000) + np.arange(n, dtype=np.int64) * c["intervalo_s"] * 1000)
    seg_local = (tiempos // 1000 - 5 * 3600) % 86400
    hora_local = seg_local / 3600.0
    dia_idx = ((tiempos - tiempos[0]) // 86_400_000).astype(int)
    prog = programar(cfg, run_id, tiempos, hora_local, dia_idx, rng)

    # Temperatura exterior de Juliaca: ciclo horario real (interpolado) + desvío diario
    h0 = np.floor(hora_local).astype(int) % 24
    frac = hora_local - np.floor(hora_local)
    t_ext = pp["temp_hora"][h0] * (1 - frac) + pp["temp_hora"][(h0 + 1) % 24] * frac
    t_ext = t_ext + rng.normal(0, pp["min_diaria_desv"], dia_idx.max() + 1)[dia_idx]
    hum = pp["hum_hora"][h0] * (1 - frac) + pp["hum_hora"][(h0 + 1) % 24] * frac
    t_media = float(pp["temp_hora"].mean())
    pesos = np.array(cons["pesos_hora"], dtype=float)
    pesos = pesos / pesos.mean()
    paso_min = c["intervalo_s"] / 60
    c_ref = velocidad_sonido(med["temperatura_referencia_c"])

    filas = []
    for t, tq in enumerate(cfg["tanques"]):
        cap, alt, off = tq["capacidad_l"], tq["altura_util_cm"], tq["sensor_sobre_lleno_cm"]
        # Consumo: base diaria por persona x peso de la hora x multiplicador lognormal por bloque de 1 h
        bloques = rng.lognormal(0, cons["variabilidad_horaria_sigma"], int(n * paso_min / 60) + 2)
        mult = bloques[(np.arange(n) * paso_min / 60).astype(int)]
        consumo = tq["personas"] * cons["litros_persona_dia"] / 1440 * paso_min * pesos[h0] * mult
        fuga = prog["fuga_lph"][t] * paso_min / 60
        entrada = cfg["suministro"]["caudal_llenado_lpm"] * paso_min * prog["suministro"][t]
        vol = np.empty(n)
        v = cap * tq["nivel_inicial_pct"] / 100
        for i in range(n):  # el flotador corta al llenarse; el tanque no baja de 0
            v = min(cap, max(0.0, v - consumo[i] - fuga[i] + entrada[i]))
            vol[i] = v
        dist_real = off + alt * (1 - vol / cap)

        # Temperatura en la cámara de aire: el ciclo exterior amortiguado, más la helada atípica si la hay
        t_cam = t_media + med["amortiguacion_camara"] * (t_ext - t_media) - prog["frio_extra"][t] \
            + rng.normal(0, 0.3)
        # El firmware convierte tiempo de vuelo con c a 20 °C: con aire más frío la distancia sale mayor
        dist = dist_real * c_ref / velocidad_sonido(t_cam) + rng.normal(0, pp["ruido_sigma_cm"], n)
        saltos = rng.random(n) < pp["saltos_tasa"]
        dist[saltos] += rng.choice([-1, 1], saltos.sum()) * rng.uniform(*med["saltos_cm"], saltos.sum())
        dist = np.maximum(2.0, np.round(dist / med["resolucion_cm"]) * med["resolucion_cm"])

        etiqueta, modo = prog["etiqueta"][t].copy(), prog["falla_modo"][t]
        dist[modo == 1] = -1
        imposibles = modo == 2
        dist[imposibles] = np.round(off + alt + rng.uniform(50, 150, imposibles.sum()), 1)  # más allá del fondo
        temp = np.round(t_cam + rng.normal(0, 0.2, n), 1)
        humedad = np.round(np.clip(hum + rng.normal(0, 2.0, n), 5, 100), 0)
        # -1 sueltos con la tasa real del ESP32 (falla_sensor puntual, sin episodio)
        sueltos_d = (rng.random(n) < med["falla_distancia_tasa"]) & (etiqueta < 0)
        sueltos_h = (rng.random(n) < med["falla_dht_tasa"]) & (etiqueta < 0) & ~sueltos_d
        dist[sueltos_d] = -1
        temp[sueltos_h] = -1
        humedad[sueltos_h] = -1
        etiqueta[sueltos_d | sueltos_h] = TIPOS.index("falla_sensor")
        nivel_pct = np.where(dist == -1, np.nan, np.round(np.clip((alt - (dist - off)) / alt * 100, 0, 100), 1))
        filas.append({"t": t, "dist": dist, "dist_real": np.round(dist_real, 2), "nivel": nivel_pct,
                      "temp": temp, "hum": humedad, "etiqueta": etiqueta, "episodio": prog["episodio"][t],
                      "suministro": prog["suministro"][t],  # agenda de llegadas: la usa el análisis de sensibilidad
                      "omitidas_sin_etiqueta": prog["omitidas_sin_etiqueta"]})
    return tiempos, filas, prog["episodios"]


def a_columnas(cfg, run_id, tiempos, filas, episodios):
    """Tabla columnar (dict de listas) con los eventos de todos los tanques, en orden temporal."""
    ingesta = int(time.time() * 1000)
    col = {k: [] for k in ("tipoEvento", "eventId", "sensorId", "origen", "fuenteTipo", "timestamp", "ingestaTs",
                           "distanciaCm", "distanciaRealCm", "nivelPct", "temperaturaC", "humedadPct",
                           "esAnomalia", "tipoAnomalia", "episodioId", "runId", "fecha")}
    # Fecha de partición: día de la lectura en hora de Lima (la misma para todos los tanques)
    fechas = [datetime.fromtimestamp(x / 1000, LIMA).date().isoformat() for x in tiempos]
    for f in filas:
        sid = cfg["tanques"][f["t"]]["sensorId"]
        for i, ts in enumerate(tiempos):
            e = int(f["etiqueta"][i])
            ep = int(f["episodio"][i])
            col["tipoEvento"].append("aqua.tanque.lectura")
            col["eventId"].append(f"aum-{run_id}-{sid}-{int(ts)}")
            col["sensorId"].append(sid)
            col["origen"].append("flota-extrapolada")
            col["fuenteTipo"].append("aumentado")
            col["timestamp"].append(int(ts))
            col["ingestaTs"].append(ingesta)
            col["distanciaCm"].append(float(f["dist"][i]))
            col["distanciaRealCm"].append(float(f["dist_real"][i]))
            col["nivelPct"].append(None if math.isnan(f["nivel"][i]) else float(f["nivel"][i]))
            col["temperaturaC"].append(float(f["temp"][i]))
            col["humedadPct"].append(float(f["hum"][i]))
            col["esAnomalia"].append(e >= 0)
            col["tipoAnomalia"].append(TIPOS[e] if e >= 0 else None)
            col["episodioId"].append(episodios[ep]["episodioId"] if e >= 0 and ep >= 0 else None)
            col["runId"].append(run_id)
            col["fecha"].append(fechas[i])
    return col


def evento(col, i):
    return {k: col[k][i] for k in col if k != "fecha"}


def huella(col):
    """Hash del contenido (sin ingestaTs, que es la hora de generación): igual semilla -> igual huella."""
    h = hashlib.sha256()
    for k in sorted(col):
        if k != "ingestaTs":
            h.update(json.dumps(col[k], separators=(",", ":")).encode())
    return h.hexdigest()[:16]


# --- Salidas ---------------------------------------------------------------------------------

def escribir_parquet(col, run_id):
    import pyarrow as pa
    import pyarrow.dataset as ds
    destino = RAIZ / "batch" / "datos" / "aumentados" / f"runId={run_id}"
    tabla = pa.table({k: v for k, v in col.items() if k != "runId"} | {"runId": col["runId"]})
    ds.write_dataset(tabla, destino, format="parquet", partitioning=["fecha"], partitioning_flavor="hive",
                     existing_data_behavior="delete_matching")
    archivos = sorted(destino.rglob("*.parquet"))
    return destino, archivos


def publicar_kafka(col, tasa, desde, crear):
    from kafka import KafkaConsumer, KafkaProducer
    from kafka.admin import KafkaAdminClient, NewTopic
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:49092")
    sonda = KafkaConsumer(bootstrap_servers=bootstrap)
    particiones = sonda.partitions_for_topic(TOPICO)
    sonda.close()
    if not particiones:
        if not crear:
            raise SystemExit(f"El tópico {TOPICO} no existe: usa --crear-topico (3 particiones) para no depender "
                             "de la auto-creación, que lo dejaría con 1 partición")
        admin = KafkaAdminClient(bootstrap_servers=bootstrap)
        admin.create_topics([NewTopic(TOPICO, num_partitions=3, replication_factor=1,
                                      topic_configs={"retention.ms": "604800000"})])
        admin.close()
        log(status="topico_creado", topico=TOPICO, particiones=3)
    # Sin timestamp_ms: Kafka marca cada mensaje con la hora de publicación (CreateTime), no con la simulada
    prod = KafkaProducer(bootstrap_servers=bootstrap, acks="all", linger_ms=50, batch_size=131072,
                         key_serializer=str.encode,
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"))
    por_part, errores = {}, []

    def ok(meta):
        por_part[meta.partition] = por_part.get(meta.partition, 0) + 1

    total = len(col["eventId"])
    t0 = time.time()
    for i in range(desde, total):
        ev = evento(col, i)
        prod.send(TOPICO, key=ev["sensorId"], value=ev).add_callback(ok).add_errback(errores.append)
        enviados = i - desde + 1
        if tasa and enviados % 500 == 0:
            espera = enviados / tasa - (time.time() - t0)
            if espera > 0:
                time.sleep(espera)
        if enviados % 20000 == 0:
            log(status="publicando", enviados=enviados, de=total - desde, evS=round(enviados / (time.time() - t0)))
    prod.flush()
    prod.close()
    return {"topico": TOPICO, "publicados": sum(por_part.values()), "por_particion": dict(sorted(por_part.items())),
            "errores": len(errores), "segundos": round(time.time() - t0, 1), "desde": desde}


# --- Principal -------------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Generador de datos aumentados (flota extrapolada)")
    ap.add_argument("--config", default=str(Path(__file__).with_name("generador.yml")))
    ap.add_argument("--tanques", type=int)
    ap.add_argument("--dias", type=float)
    ap.add_argument("--intervalo-s", type=int, dest="intervalo_s")
    ap.add_argument("--semilla", type=int)
    ap.add_argument("--anomalias", type=float, help="proporción objetivo de eventos anómalos (0.05 = 5 %%)")
    ap.add_argument("--factor-ruido", type=float, dest="factor_ruido",
                    help="multiplica el ruido del perfil real (σ y frecuencia de saltos); sensibilidad")
    ap.add_argument("--caudal-llenado", type=float, dest="caudal_llenado", help="L/min de llenado; sensibilidad")
    ap.add_argument("--franja-atipica", type=float, nargs=2, dest="franja_atipica", metavar=("DESDE", "HASTA"),
                    help="hora de Lima en que empiezan las llegadas atípicas; sensibilidad")
    ap.add_argument("--parquet", action="store_true", help="escribe batch/datos/aumentados/runId=<id>/")
    ap.add_argument("--kafka", action="store_true", help=f"publica en {TOPICO}")
    ap.add_argument("--crear-topico", action="store_true", help="crea el tópico (3 particiones) si falta")
    ap.add_argument("--tasa", type=int, default=2000, help="eventos/s hacia Kafka (0 = sin límite)")
    ap.add_argument("--desde", type=int, default=0, help="reanuda la publicación desde el evento N")
    ap.add_argument("--seco", action="store_true", help="solo genera y valida, sin salidas")
    ap.add_argument("--permitir-menos", action="store_true", help="acepta menos eventos que minimo_eventos")
    args = ap.parse_args()

    cfg, perfil, run_id = cargar_config(args)
    c = cfg["corrida"]
    n_esperado = len(cfg["tanques"]) * int(c["dias"] * 86400 // c["intervalo_s"])
    if n_esperado < c["minimo_eventos"] and not args.permitir_menos:
        raise SystemExit(f"La configuración produce {n_esperado} eventos (< {c['minimo_eventos']}). Aumenta "
                         "--tanques, --dias o baja --intervalo-s, o usa --permitir-menos de forma explícita.")

    t0 = time.time()
    tiempos, filas, episodios = generar(cfg, perfil, run_id)
    col = a_columnas(cfg, run_id, tiempos, filas, episodios)
    total = len(col["eventId"])
    anomalos = sum(col["esAnomalia"])
    por_tipo = {t: col["tipoAnomalia"].count(t) for t in TIPOS}

    validador = cargar_validador()
    muestra = np.random.default_rng(c["semilla"] + 1).choice(total, size=min(1000, total), replace=False)
    invalidos = [(col["eventId"][i], errores_contrato(validador, evento(col, i))) for i in muestra]
    invalidos = [x for x in invalidos if x[1]]
    resumen = {
        "runId": run_id, "eventos": total, "tanques": len(cfg["tanques"]), "dias": c["dias"],
        "intervalo_s": c["intervalo_s"], "semilla": c["semilla"],
        "periodo": [datetime.fromtimestamp(tiempos[0] / 1000, LIMA).isoformat(),
                    datetime.fromtimestamp(tiempos[-1] / 1000, LIMA).isoformat()],
        "anomalos": anomalos, "proporcion_anomalos": round(anomalos / total, 4), "por_tipo": por_tipo,
        "episodios": dict(sorted({t: sum(1 for e in episodios if e["tipo"] == t) for t in TIPOS}.items())),
        "falla_sensor_puntuales": sum(1 for i in range(total) if col["tipoAnomalia"][i] == "falla_sensor"
                                      and col["episodioId"][i] is None),
        "validacion_muestra": {"n": len(muestra), "invalidos": len(invalidos), "ejemplos": invalidos[:3]},
        "huella_contenido": huella(col), "segundos_generacion": round(time.time() - t0, 1),
    }
    log(status="generado", **resumen)
    if invalidos:
        raise SystemExit("Hay eventos que no cumplen el contrato: no se escribe nada")
    if args.seco:
        return

    if args.parquet:
        destino, archivos = escribir_parquet(col, run_id)
        (destino / "_episodios.json").write_text(json.dumps(episodios, ensure_ascii=False, indent=1), encoding="utf-8")
        (destino / "_resumen.json").write_text(json.dumps(resumen, ensure_ascii=False, indent=1, default=str),
                                               encoding="utf-8")
        log(status="parquet", destino=str(destino.relative_to(RAIZ)), archivos=len(archivos),
            particiones=len({a.parent.name for a in archivos}))
    if args.kafka:
        log(status="kafka", **publicar_kafka(col, args.tasa, args.desde, args.crear_topico))


if __name__ == "__main__":
    main()
