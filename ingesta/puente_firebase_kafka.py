"""
Puente Firebase Realtime Database -> Kafka (MonitorAqua, F2).

Escucha FIREBASE_PATH (/board1/lecturas/historial) por streaming REST (SSE) y publica cada
lectura nueva en KAFKA_TOPIC (aqua-eventos-real) con clave sensorId, después de completarla
(fuenteTipo, ingestaTs) y validarla contra contratos/evento_aqua.schema.json.

Al historial escriben dos productores:
  - el ESP32 (push id cronológico, solo medidas + sensorId + timestamp), y
  - el colector de APIs (clave origen_estacion_timestamp, evento ya en forma de contrato).
Como las claves de ambos no ordenan en el tiempo entre sí, el cursor del puente es el campo
`timestamp` (requiere ".indexOn": "timestamp" en las reglas de Firebase). Las lecturas de API
llegan con atraso, así que el stream se abre desde (cursor - VENTANA_H) y lo ya publicado se
salta con un registro de claves vistas que se persiste en ESTADO_PATH.

Uso:
  python puente_firebase_kafka.py                       sigue desde el estado guardado
                                                        (sin estado: solo lecturas nuevas)
  python puente_firebase_kafka.py --backfill-horas H    publica además lo de las últimas H horas
  python puente_firebase_kafka.py --backfill-horas H --once   publica eso y termina

Entrega al-menos-una-vez: si el proceso muere entre publicar y guardar el estado, una lectura
puede repetirse en Kafka; Spark deduplica por eventId (F3).
"""
import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests
from kafka import KafkaProducer

from comun import (FIREBASE_PATH, ORIGENES, URL_HISTORIAL, SesionFirebase, ahora_ms, cargar_validador,
                   describir_error, errores_contrato, faltan_variables_firebase, log)

SERVICIO = "aqua-puente"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "aqua-eventos-real")
ESTADO_PATH = Path(os.getenv("ESTADO_PATH", "/estado/puente.json"))
VENTANA_MS = int(float(os.getenv("VENTANA_H", "3")) * 3600 * 1000)
MEDIDAS_ESP32 = ("distanciaCm", "humedadPct", "temperaturaC")
ESPERA_MIN_S, ESPERA_MAX_S = 2, 60


class Estado:
    """Cursor (mayor timestamp publicado), piso y claves publicadas dentro de la ventana.

    piso: lecturas con timestamp anterior no se publican nunca. Al arrancar sin estado vale
    "ahora" (solo lecturas nuevas); --backfill-horas lo baja explícitamente.
    """

    def __init__(self, ruta: Path):
        self.ruta = ruta
        self.cursor = None
        self.piso = 0
        self.vistas: dict[str, int] = {}
        self._sucio = False
        self._guardado_en = 0.0
        if ruta.exists():
            d = json.loads(ruta.read_text(encoding="utf-8"))
            self.cursor, self.piso, self.vistas = d.get("cursor"), d.get("piso", 0), d.get("vistas", {})

    def fijar_piso(self, ts):
        self.piso = ts
        self.cursor = max(self.cursor or 0, ts)
        self._sucio = True

    def marcar(self, clave, ts):
        self.vistas[clave] = ts
        self.cursor = max(self.cursor or 0, ts)
        self._sucio = True

    def guardar(self, forzar=False):
        if not self._sucio or (not forzar and time.time() - self._guardado_en < 5):
            return
        limite = (self.cursor or 0) - VENTANA_MS
        self.vistas = {k: t for k, t in self.vistas.items() if t >= limite}
        self.ruta.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.ruta.with_suffix(".tmp")
        tmp.write_text(json.dumps({"cursor": self.cursor, "piso": self.piso, "vistas": self.vistas}),
                       encoding="utf-8")
        tmp.replace(self.ruta)
        self._sucio, self._guardado_en = False, time.time()


class Puente:
    def __init__(self):
        self.validador = cargar_validador()
        self.sesion = SesionFirebase(SERVICIO)
        self.estado = Estado(ESTADO_PATH)
        self.producer = None
        self.contadores = {"publicadas": 0, "rechazadas": 0, "repetidas": 0, "previas": 0}

    # --- Kafka -----------------------------------------------------------
    def conectar_kafka(self):
        if self.producer is not None:
            return
        # acks=all + reintentos: una lectura real no se repite, no conviene perderla
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            acks="all",
            retries=5,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )
        log(SERVICIO, status="kafka_connected", bootstrap=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC)

    def descartar_kafka(self):
        if self.producer is not None:
            try:
                self.producer.close(timeout=5)
            except Exception:  # noqa: BLE001
                pass
        self.producer = None

    # --- Conversión al contrato -------------------------------------------
    @staticmethod
    def a_evento(clave, lectura):
        if "origen" in lectura:
            # Escrito por el colector: ya viene en forma de contrato
            evento = dict(lectura)
        else:
            # Escrito por el ESP32 (firmware sin cambios): solo sensorId, distanciaCm,
            # temperaturaC, humedadPct y timestamp. El puente completa el resto del contrato.
            evento = {
                "tipoEvento": "aqua.tanque.lectura",
                "sensorId": str(lectura.get("sensorId") or "desconocido"),
                "origen": "esp32-fisico",
                "timestamp": lectura.get("timestamp"),
            }
            # El -1 (fallo del sensor) viaja tal cual: la regla de calidad vive en Spark (F3)
            for medida in MEDIDAS_ESP32:
                evento[medida] = lectura.get(medida)
        # eventId = clave de Firebase (push key del ESP32 o clave determinística del colector)
        evento["eventId"] = clave
        if "fuenteTipo" not in evento and evento.get("origen") in ORIGENES:
            evento["fuenteTipo"] = ORIGENES[evento["origen"]][0]
        evento.setdefault("ingestaTs", ahora_ms())
        return evento

    def procesar(self, clave, lectura):
        if clave in self.estado.vistas:
            self.contadores["repetidas"] += 1
            return
        if not isinstance(lectura, dict):
            log(SERVICIO, status="rejected", eventId=clave, errores=["la entrada no es un objeto"])
            self.contadores["rechazadas"] += 1
            return
        evento = self.a_evento(clave, lectura)
        errores = errores_contrato(self.validador, evento)
        ts = evento.get("timestamp") if isinstance(evento.get("timestamp"), int) else ahora_ms()
        if ts < self.estado.piso:
            self.contadores["previas"] += 1  # anterior al arranque (o al backfill): no es "nueva"
            return
        if errores:
            log(SERVICIO, status="rejected", eventId=clave, origen=evento.get("origen"), errores=errores)
            self.contadores["rechazadas"] += 1
            self.estado.marcar(clave, ts)  # no se reintenta: seguiría siendo inválida
            return
        meta = self.producer.send(KAFKA_TOPIC, key=evento["sensorId"], value=evento).get(timeout=15)
        self.estado.marcar(clave, ts)
        self.contadores["publicadas"] += 1
        log(SERVICIO, status="published", eventId=clave, origen=evento["origen"], sensorId=evento["sensorId"],
            ts=evento["timestamp"], partition=meta.partition, offset=meta.offset)

    def procesar_lote(self, entradas):
        # Orden por tiempo de evento, para que el cursor avance de forma monótona
        orden = sorted(entradas.items(), key=lambda kv: (kv[1] or {}).get("timestamp", 0)
                       if isinstance(kv[1], dict) else 0)
        for clave, lectura in orden:
            self.procesar(clave, lectura)
        self.estado.guardar()

    # --- Firebase ----------------------------------------------------------
    def leer_desde(self, desde_ms):
        r = requests.get(URL_HISTORIAL + ".json", timeout=60, params={
            "auth": self.sesion.token(), "orderBy": '"timestamp"', "startAt": desde_ms})
        r.raise_for_status()
        return r.json() or {}

    def escuchar(self):
        """Stream SSE desde (cursor - ventana). Vuelve al cortarse o cuando el token está por vencer."""
        desde = (self.estado.cursor or ahora_ms()) - VENTANA_MS
        params = {"auth": self.sesion.token(), "orderBy": '"timestamp"', "startAt": desde}
        with requests.get(URL_HISTORIAL + ".json", params=params, headers={"Accept": "text/event-stream"},
                          stream=True, timeout=(15, 90)) as r:  # Firebase envía keep-alive cada 30 s
            r.raise_for_status()
            log(SERVICIO, status="streaming", path=FIREBASE_PATH, desdeTs=desde, **self.contadores)
            tipo = None
            # chunk_size=1: con el valor por defecto una lectura corta queda retenida en el buffer
            for linea in r.iter_lines(chunk_size=1, decode_unicode=True):
                if self.sesion.por_vencer():
                    log(SERVICIO, status="reconnecting", motivo="token por vencer")
                    return
                if not linea:
                    continue
                if linea.startswith("event:"):
                    tipo = linea[6:].strip()
                    continue
                if not linea.startswith("data:"):
                    continue
                if tipo in ("cancel", "auth_revoked"):
                    raise RuntimeError(f"stream cerrado por Firebase: {tipo}")
                if tipo not in ("put", "patch"):
                    continue  # keep-alive
                cuerpo = json.loads(linea[5:])
                ruta, datos = cuerpo.get("path", "/"), cuerpo.get("data")
                if datos is None:
                    continue  # borrado
                if ruta == "/":
                    entradas = datos  # estado inicial de la ventana, o update multi-ruta
                elif ruta.count("/") == 1:
                    entradas = {ruta[1:]: datos}  # una lectura nueva
                else:
                    continue  # cambio dentro de una lectura ya existente
                self.procesar_lote(entradas)

    def backfill(self, horas):
        desde = ahora_ms() - int(horas * 3600 * 1000)
        self.estado.fijar_piso(min(self.estado.piso or desde, desde))
        entradas = self.leer_desde(desde)
        log(SERVICIO, status="backfill_start", horas=horas, lecturas=len(entradas))
        self.procesar_lote(entradas)
        self.estado.guardar(forzar=True)
        log(SERVICIO, status="backfill_done", **self.contadores)

    def correr(self, backfill_horas, once):
        espera = ESPERA_MIN_S
        primera = True
        while True:
            try:
                self.conectar_kafka()
                if primera:
                    if backfill_horas:
                        self.backfill(backfill_horas)
                        if once:
                            return
                    elif self.estado.cursor is None:
                        # Sin estado y sin backfill: solo lecturas nuevas desde ahora
                        self.estado.fijar_piso(ahora_ms())
                        log(SERVICIO, status="sin_estado", motivo="se ignora el historial existente",
                            piso=self.estado.piso)
                    primera = False
                self.escuchar()
                espera = ESPERA_MIN_S
            except KeyboardInterrupt:
                break
            except Exception as ex:  # noqa: BLE001 - Firebase o Kafka caídos: reintento con espera creciente
                log(SERVICIO, status="error", error=describir_error(ex), reintentoEnS=espera, **self.contadores)
                if "Kafka" in type(ex).__name__ or "kafka" in type(ex).__module__:
                    self.descartar_kafka()
                time.sleep(espera)
                espera = min(espera * 2, ESPERA_MAX_S)
            finally:
                self.estado.guardar(forzar=True)
        if self.producer:
            self.producer.flush()


def main():
    parser = argparse.ArgumentParser(description="Puente Firebase RTDB -> Kafka (MonitorAqua)")
    parser.add_argument("--backfill-horas", type=float, default=0, metavar="H",
                        help="publica también las lecturas con timestamp de las últimas H horas")
    parser.add_argument("--once", action="store_true", help="con --backfill-horas: termina al acabar")
    args = parser.parse_args()

    # Solo historial: /board1/lecturas/ultima, /board1/sensor y /board1/outputs no son eventos
    if not FIREBASE_PATH.rstrip("/").endswith("/lecturas/historial"):
        log(SERVICIO, status="config_error", motivo=f"FIREBASE_PATH debe ser .../lecturas/historial, no {FIREBASE_PATH}")
        sys.exit(1)
    faltan = faltan_variables_firebase()
    if faltan:
        log(SERVICIO, status="config_error", faltan=faltan, ayuda="completa .env")
        sys.exit(1)
    log(SERVICIO, status="starting", firebasePath=FIREBASE_PATH, kafkaBootstrap=KAFKA_BOOTSTRAP,
        kafkaTopic=KAFKA_TOPIC, ventanaH=VENTANA_MS / 3_600_000, estado=str(ESTADO_PATH),
        backfillHoras=args.backfill_horas)
    Puente().correr(args.backfill_horas, args.once)


if __name__ == "__main__":
    main()
