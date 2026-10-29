"""Valida ejemplos de cada fuente contra contratos/evento_aqua.schema.json.

Uso: python contratos/validar.py
Sale con código 1 si un ejemplo válido falla o si un ejemplo inválido pasa.
"""
import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).with_name("evento_aqua.schema.json")

# Para probar el completado real del puente (no una copia): ingesta/puente_firebase_kafka.py
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingesta"))
from puente_firebase_kafka import Puente  # noqa: E402

# Lo que el ESP32 escribe en /board1/lecturas/historial (export real, 2026-10-28):
# solo sensorId, distanciaCm, temperaturaC, humedadPct y timestamp; la push key es el eventId.
ESP32_PUSH_KEY = "-P53dfczInbKy2W8YMY6"
ESP32_CRUDO = {"distanciaCm": 5.3, "humedadPct": 53.0, "sensorId": "esp32-tanque-real-01",
               "temperaturaC": 20.7, "timestamp": 1793226881208}
ESP32_FALLO = {**ESP32_CRUDO, "distanciaCm": -1}

# (nombre, evento, debe_ser_valido)
EJEMPLOS = [
    # Ejemplo literal que hoy publica el ESP32 (sin fuenteTipo ni ingestaTs)
    ("esp32-fisico (real)", {
        "tipoEvento": "aqua.tanque.lectura", "eventId": "-P41ldQebFiPJZM8JIsh",
        "sensorId": "esp32-tanque-real-01", "distanciaCm": 3.9, "humedadPct": 38.0,
        "temperaturaC": 23.0, "origen": "esp32-fisico", "timestamp": 1792121672601,
    }, True),
    ("thingspeak", {
        "tipoEvento": "aqua.tanque.lectura", "eventId": "ts-1293177-48211",
        "sensorId": "thingspeak-1293177", "origen": "thingspeak", "fuenteTipo": "api-publica",
        "timestamp": 1792121600000, "ingestaTs": 1792121605120,
        "distanciaCm": 41.7, "nivelPct": 63.5,
    }, True),
    ("ea-uk", {
        "tipoEvento": "aqua.red.lectura", "eventId": "ea-1491TH-1792121400000",
        "sensorId": "ea-uk-1491TH", "origen": "ea-uk", "fuenteTipo": "api-publica",
        "timestamp": 1792121400000, "ingestaTs": 1792121610400,
        "nivelM": 0.824, "caudalM3s": 3.12, "lat": 51.5, "lon": -0.12,
    }, True),
    ("usgs", {
        "tipoEvento": "aqua.calidad.lectura", "eventId": "usgs-01646500-1792121100000",
        "sensorId": "usgs-01646500", "origen": "usgs", "fuenteTipo": "api-publica",
        "timestamp": 1792121100000, "ingestaTs": 1792121615900,
        "tempAguaC": 14.2, "ph": 7.6, "oxigenoMgL": 9.8, "lat": 38.95, "lon": -77.13,
    }, True),
    ("open-meteo", {
        "tipoEvento": "aqua.clima.lectura", "eventId": "om-juliaca-1792120800000",
        "sensorId": "open-meteo-juliaca", "origen": "open-meteo", "fuenteTipo": "api-publica",
        "timestamp": 1792120800000, "ingestaTs": 1792121620300,
        "temperaturaC": 8.4, "humedadPct": 41.0, "lat": -15.49, "lon": -70.13,
    }, True),
    # ESP32 tal como lo completa el puente (eventId, origen, fuenteTipo, tipoEvento, ingestaTs)
    ("esp32 completado por el puente", Puente.a_evento(ESP32_PUSH_KEY, ESP32_CRUDO), True),
    ("esp32 con distanciaCm=-1 (pasa tal cual)", Puente.a_evento(ESP32_PUSH_KEY, ESP32_FALLO), True),
    # Casos negativos: deben ser rechazados
    ("NEG: esp32 crudo sin completar", {**ESP32_CRUDO}, False),
    ("NEG: sin eventId", {
        "tipoEvento": "aqua.tanque.lectura", "sensorId": "esp32-tanque-real-01",
        "origen": "esp32-fisico", "timestamp": 1792121672601, "distanciaCm": 3.9,
    }, False),
    ("NEG: usgs con tipoEvento de tanque", {
        "tipoEvento": "aqua.tanque.lectura", "eventId": "usgs-x", "sensorId": "usgs-01646500",
        "origen": "usgs", "fuenteTipo": "api-publica", "timestamp": 1792121100000,
    }, False),
]


def main() -> int:
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validador = Draft202012Validator(schema)

    fallos = 0
    print(f"{'ejemplo':<38} {'esperado':<9} {'obtenido':<9} resultado")
    print("-" * 72)
    for nombre, evento, debe_ser_valido in EJEMPLOS:
        errores = sorted(validador.iter_errors(evento), key=lambda e: list(e.path))
        es_valido = not errores
        ok = es_valido == debe_ser_valido
        fallos += not ok
        print(f"{nombre:<38} {'válido' if debe_ser_valido else 'inválido':<9} "
              f"{'válido' if es_valido else 'inválido':<9} {'OK' if ok else 'FALLA'}")
        for e in errores:
            ruta = "/".join(map(str, e.path)) or "(raíz)"
            print(f"    - {ruta}: {e.message}")

    print("-" * 72)
    completado = Puente.a_evento(ESP32_PUSH_KEY, ESP32_FALLO)
    print("esp32 completado:", json.dumps(completado, ensure_ascii=False))
    chequeos = {
        "eventId = push key": completado["eventId"] == ESP32_PUSH_KEY,
        "origen = esp32-fisico": completado["origen"] == "esp32-fisico",
        "fuenteTipo = iot-propio": completado["fuenteTipo"] == "iot-propio",
        "tipoEvento = aqua.tanque.lectura": completado["tipoEvento"] == "aqua.tanque.lectura",
        "ingestaTs presente": isinstance(completado.get("ingestaTs"), int),
        "distanciaCm = -1 se conserva": completado["distanciaCm"] == -1,
    }
    for nombre, ok in chequeos.items():
        fallos += not ok
        print(f"  {'OK' if ok else 'FALLA'}  {nombre}")
    print("-" * 72)
    print(f"{len(EJEMPLOS)} ejemplos + {len(chequeos)} chequeos del puente: {fallos} fallo(s)")
    return 1 if fallos else 0


if __name__ == "__main__":
    sys.exit(main())
