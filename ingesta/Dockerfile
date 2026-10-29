# Imagen común del puente Firebase -> Kafka y del colector de APIs (F2).
# Contexto de build: raíz del repo (necesita contratos/ para el schema).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    SCHEMA_PATH=/app/contratos/evento_aqua.schema.json

WORKDIR /app/ingesta

COPY ingesta/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contratos/*.schema.json /app/contratos/
COPY ingesta/*.py ingesta/estaciones.yml ./

CMD ["python", "puente_firebase_kafka.py"]
