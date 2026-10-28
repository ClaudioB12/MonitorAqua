"""Piezas compartidas por el puente y el colector: log JSON, sesión Firebase, contrato."""
import json
import os
import re
import threading
import time
from pathlib import Path

import requests
from jsonschema import Draft202012Validator

FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY", "")
FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL", "").rstrip("/")
FIREBASE_EMAIL = os.getenv("FIREBASE_EMAIL", "")
FIREBASE_PASSWORD = os.getenv("FIREBASE_PASSWORD", "")
FIREBASE_PATH = os.getenv("FIREBASE_PATH", "/board1/lecturas/historial")
URL_HISTORIAL = FIREBASE_DB_URL + FIREBASE_PATH

SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", Path(__file__).resolve().parents[1] / "contratos" / "evento_aqua.schema.json"))

# origen -> (fuenteTipo, tipoEvento), igual que la tabla de CLAUDE.md y los if/then del schema
ORIGENES = {
    "esp32-fisico": ("iot-propio", "aqua.tanque.lectura"),
    "thingspeak": ("api-publica", "aqua.tanque.lectura"),
    "ea-uk": ("api-publica", "aqua.red.lectura"),
    "usgs": ("api-publica", "aqua.calidad.lectura"),
    "open-meteo": ("api-publica", "aqua.clima.lectura"),
}

# El idToken dura 1 hora: se renueva 10 minutos antes de que venza.
MARGEN_RENOVACION_S = 600


def ahora_ms() -> int:
    return int(time.time() * 1000)


def log(servicio, **campos):
    print(json.dumps({"service": servicio, "ts": ahora_ms(), **campos}, ensure_ascii=False), flush=True)


def faltan_variables_firebase():
    return [v for v in ("FIREBASE_API_KEY", "FIREBASE_DB_URL", "FIREBASE_EMAIL", "FIREBASE_PASSWORD")
            if not os.getenv(v)]


def describir_error(ex):
    # Las excepciones de requests traen la URL con ?auth=<idToken>: solo se
    # registra el tipo de error y el código HTTP, nunca el mensaje completo.
    codigo = getattr(getattr(ex, "response", None), "status_code", None)
    if isinstance(ex, requests.RequestException):
        return type(ex).__name__ + (f" (HTTP {codigo})" if codigo else "")
    return f"{type(ex).__name__}: {ex}"


def cargar_validador():
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def errores_contrato(validador, evento):
    """Lista de errores legibles; vacía si el evento cumple el contrato."""
    return [f"{'/'.join(map(str, e.path)) or '(raíz)'}: {e.message}"
            for e in sorted(validador.iter_errors(evento), key=lambda e: list(e.path))]


def clave_firebase(texto):
    """Firebase no admite . $ # [ ] / en las claves."""
    return re.sub(r"[.$#\[\]/\s]", "-", texto)


class SesionFirebase:
    """Login Email/Password por REST y renovación del idToken."""

    def __init__(self, servicio):
        self.servicio = servicio
        self.id_token = None
        self.refresh_token = None
        self.expira_en = 0
        self._http = requests.Session()
        self._lock = threading.Lock()

    def _guardar(self, id_token, refresh_token, expires_in):
        self.id_token = id_token
        self.refresh_token = refresh_token
        self.expira_en = time.time() + int(expires_in)

    def iniciar(self):
        r = self._http.post(
            "https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword",
            params={"key": FIREBASE_API_KEY},
            json={"email": FIREBASE_EMAIL, "password": FIREBASE_PASSWORD, "returnSecureToken": True},
            timeout=15,
        )
        if r.status_code != 200:
            motivo = r.json().get("error", {}).get("message", r.status_code)
            raise RuntimeError(f"login rechazado: {motivo}")
        d = r.json()
        self._guardar(d["idToken"], d["refreshToken"], d["expiresIn"])
        log(self.servicio, status="authenticated", expiresInS=int(d["expiresIn"]))

    def renovar(self):
        r = self._http.post(
            "https://securetoken.googleapis.com/v1/token",
            params={"key": FIREBASE_API_KEY},
            data={"grant_type": "refresh_token", "refresh_token": self.refresh_token},
            timeout=15,
        )
        if r.status_code != 200:
            # Refresh token revocado (p. ej. cambio de clave): se vuelve a entrar
            self.iniciar()
            return
        d = r.json()
        self._guardar(d["id_token"], d["refresh_token"], d["expires_in"])
        log(self.servicio, status="token_refreshed", expiresInS=int(d["expires_in"]))

    def por_vencer(self):
        return time.time() > self.expira_en - MARGEN_RENOVACION_S

    def token(self):
        # El colector comparte la sesión entre hilos: una sola renovación a la vez
        with self._lock:
            if self.id_token is None:
                self.iniciar()
            elif self.por_vencer():
                self.renovar()
            return self.id_token
