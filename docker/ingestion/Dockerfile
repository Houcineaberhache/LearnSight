# =============================================================================
# LearnSight — Image d'exécution des producteurs Kafka (Étudiant 1)
# =============================================================================
# Utilisée par les services Compose `kafka-init` et `ingestion-producer`.
# Le code est monté ou copié, les données OULAD sont montées en lecture seule
# depuis l'hôte (elles ne sont jamais intégrées à l'image).
# =============================================================================

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dépendances d'ingestion (pandas, kafka-python, jsonschema, PyYAML, ...)
COPY ingestion/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code d'ingestion et contrats partagés
COPY ingestion /app/ingestion
COPY config /app/config

# Vérifie que les modules sont importables dès la construction
RUN python -c "import ingestion.producers.kafka_admin; import ingestion.utils.schema_validator"

ENTRYPOINT ["python", "-m"]
CMD ["ingestion.producers.kafka_admin", "--help"]
