"""
Carga de los YAML de configuración (tanques.yml, reglas_servicio.yml) una sola vez por archivo y versión (F8).

Hallazgo de la prueba de carga:
- Cada proceso de Python de Spark (los workers de applyInPandasWithState y el driver, que tiene dos detectores)
  parseaba tanques.yml por su cuenta con PyYAML puro.
- Con 25,000 tanques eran 5.0 s por carga y una copia en memoria por proceso.
- Con los 53,724 tanques de Juliaca, a producción le pasaría lo mismo.

Ahora:
- **libyaml:** el parseo usa CSafeLoader (`yaml.__with_libyaml__`), con el mismo resultado que safe_load.
- **Caché por proceso:** clave (ruta, mtime, tamaño). Los dos detectores del driver comparten el mismo objeto, que
  nadie modifica.
- **Caché entre procesos:** pickle en el /tmp del contenedor con la misma clave. El primer worker parsea; los demás
  solo deserializan.
- **Recarga:** cambia la fecha de modificación → cambia la clave → se relee, como antes.

detector_servicio.py (reglas congeladas f4.4) no se toca: servicio_spark y procesar_stream reemplazan su función de
carga, `detector_servicio._cargar`, por `cargar`, antes de crear la Configuracion.
"""
import hashlib
import os
import pickle
import tempfile

import yaml

_CARGADOR = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
_CACHE = {}
_DIR = os.path.join(tempfile.gettempdir(), "monitoraqua_config")


def cargar(ruta):
    st = os.stat(ruta)
    clave = (os.path.abspath(ruta), st.st_mtime_ns, st.st_size)
    if clave in _CACHE:
        return _CACHE[clave]
    archivo = os.path.join(_DIR, hashlib.sha1(repr(clave).encode()).hexdigest() + ".pickle")
    try:
        with open(archivo, "rb") as f:
            datos = pickle.load(f)
    except (OSError, pickle.PickleError, EOFError):
        with open(ruta, encoding="utf-8") as f:
            datos = yaml.load(f, Loader=_CARGADOR)  # noqa: S506 - CSafeLoader es el cargador seguro de libyaml
        try:
            os.makedirs(_DIR, exist_ok=True)
            temporal = f"{archivo}.{os.getpid()}"
            with open(temporal, "wb") as f:
                pickle.dump(datos, f, protocol=pickle.HIGHEST_PROTOCOL)
            os.replace(temporal, archivo)
        except OSError:
            pass  # sin caché en disco, solo la del proceso
    for vieja in [k for k in _CACHE if k[0] == clave[0]]:  # solo la versión vigente de cada archivo
        del _CACHE[vieja]
    _CACHE[clave] = datos
    return datos


def instalar():
    """Hace que detector_servicio.Configuracion cargue sus YAML con esta caché (llamar antes de crearla)."""
    import detector_servicio
    detector_servicio._cargar = cargar
