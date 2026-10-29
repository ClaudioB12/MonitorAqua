"""
Consumidor de eventos de servicio (F4): lee servicio-eventos en vivo, valida contra
contratos/servicio_evento.schema.json y muestra un resumen legible por evento.

Tiene su propio grupo de consumidores (aqua-servicio-consumidor) y confirma offsets: así kafka-exporter publica
su lag en Prometheus (kafka_consumergroup_lag{consumergroup="aqua-servicio-consumidor"}) para la fase 5.

  python ingesta/consumidor_eventos.py                 en vivo, desde lo último confirmado por el grupo
  python ingesta/consumidor_eventos.py --desde-inicio  todo el tópico (otro grupo, para no mover el del servicio)
"""
import argparse
import json
import os
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer

from comun import cargar_validador_servicio, errores_servicio, log

SERVICIO = "aqua-consumidor-eventos"
TOPICO = os.getenv("KAFKA_TOPIC_SERVICIO", "servicio-eventos")
GRUPO = os.getenv("GRUPO_CONSUMIDOR", "aqua-servicio-consumidor")
LIMA = timezone(timedelta(hours=-5))
ICONOS = {"info": "·", "advertencia": "!", "critica": "‼"}


def hora(ms):
    return datetime.fromtimestamp(ms / 1000, LIMA).strftime("%d-%m %H:%M")


def resumen(ev):
    d = ev.get("detalle") or {}
    extra = {
        "llegada_agua": lambda: f"sube {ev['valor']:.1f} % · caudal {d.get('caudalLlenadoLpm')} L/min"
                                + (" · FUERA DE FRANJA" if d.get("fueraDeFranja") else ""),
        "corte_prolongado": lambda: f"{ev['valor']:.1f} h sin llegada (umbral {ev['umbral']:.0f} h)"
                                    + (" · posible hueco de datos" if d.get("posibleHuecoDeDatos") else ""),
        "alerta_fuga": lambda: f"baja {ev['valor']:.2f} cm de noche ({d.get('tasaCmH')} cm/h, habitual {d.get('baseCmH')})"
                               + (f" · ~{d['litrosPerdidos']} L" if d.get("litrosPerdidos") else ""),
        "autonomia_baja": lambda: f"nivel {ev['valor']:.1f} % < {ev['umbral']:.0f} %"
                                  + (f" · {d['litros']} L" if d.get("litros") is not None else ""),
        "umbral_termico": lambda: f"agua ~{d.get('tempAguaEstimadaC')} °C, cruza {ev['umbral']} °C hacia las "
                                  f"{hora(d['horaCruceTs']) if d.get('horaCruceTs') else '?'} (provisional)",
        "falla_sensor": lambda: f"{ev['valor']:.0f} lecturas inválidas seguidas",
    }.get(ev["tipo"], lambda: "")()
    return (f"{ICONOS.get(ev['severidad'], '?')} [{ev['severidad']:<11}] {ev['tipo']:<16} {ev['sensorId']:<22} "
            f"{hora(ev['inicioTs'])}→{hora(ev['finTs'])}  {extra}  ({ev['origenLecturas']})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--desde-inicio", action="store_true")
    ap.add_argument("--json", action="store_true", help="log JSON en vez de líneas legibles")
    ap.add_argument("--salir-tras-s", type=int, help="termina si pasan estos segundos sin eventos (pruebas)")
    a = ap.parse_args()
    grupo = f"{GRUPO}-relectura" if a.desde_inicio else GRUPO
    consumer = KafkaConsumer(TOPICO, bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"), group_id=grupo,
                             auto_offset_reset="earliest" if a.desde_inicio else "latest", enable_auto_commit=True,
                             value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                             **({"consumer_timeout_ms": a.salir_tras_s * 1000} if a.salir_tras_s else {}))
    validador = cargar_validador_servicio()
    vistos = OrderedDict()  # eventoId ya mostrados: un reproceso de Spark puede reemitir el mismo evento
    log(SERVICIO, status="escuchando", topico=TOPICO, grupo=grupo)
    for m in consumer:
        ev = m.value
        errores = errores_servicio(validador, ev)
        if errores:
            log(SERVICIO, status="rechazado", particion=m.partition, offset=m.offset, errores=errores)
            continue
        if ev["eventoId"] in vistos:
            log(SERVICIO, status="repetido", eventoId=ev["eventoId"])
            continue
        vistos[ev["eventoId"]] = True
        if len(vistos) > 10000:
            vistos.popitem(last=False)
        if a.json:
            log(SERVICIO, status="evento", particion=m.partition, offset=m.offset, **ev)
        else:
            print(resumen(ev), flush=True)


if __name__ == "__main__":
    main()
