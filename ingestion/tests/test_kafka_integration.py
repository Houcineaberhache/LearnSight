"""
test_kafka_integration.py — Test d'intégration ingestion -> Kafka -> consommation.

Ce test n'est exécuté que si un broker Kafka est joignable (par défaut
``localhost:29092``, surchargeable via ``KAFKA_BOOTSTRAP_SERVERS``). Sinon il
est automatiquement ignoré, ce qui permet de lancer la suite unitaire sans
infrastructure Docker.

Lancement explicite ::

    docker compose up -d kafka
    pytest ingestion/tests/test_kafka_integration.py -v -m integration
"""

import json
import os
import socket
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.utils.schema_validator import validate_message  # noqa: E402


def get_broker() -> str:
    """Retourne le premier broker configuré."""
    raw = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
    return raw.split(",")[0].strip()


def broker_is_available(broker: str, timeout: float = 0.5) -> bool:
    """Teste rapidement l'ouverture d'une connexion TCP vers le broker."""
    host, _, port = broker.partition(":")
    if not host or not port:
        return False
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


BROKER = get_broker()
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not broker_is_available(BROKER),
        reason=f"Broker Kafka non joignable sur {BROKER}",
    ),
]


@pytest.fixture
def topic_name():
    """Crée un topic éphémère et le supprime après le test."""
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic

    name = f"learnsight-it-{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=BROKER, client_id="learnsight-it-admin")
    admin.create_topics(
        [NewTopic(name=name, num_partitions=1, replication_factor=1)],
        validate_only=False,
    )
    try:
        yield name
    finally:
        try:
            admin.delete_topics([name], timeout_ms=5000)
        except Exception:  # noqa: BLE001 — nettoyage best effort
            pass
        finally:
            admin.close()


def test_produce_and_consume_vle_event(topic_name):
    """Vérifie le round-trip d'un événement VLE conforme au contrat."""
    from kafka import KafkaConsumer, KafkaProducer

    message = {
        "metadata": {
            "event_id": str(uuid.uuid4()),
            "event_type": "vle_click",
            "produced_at": datetime.now(timezone.utc).isoformat(),
            "source_system": "oulad_simulation_producer",
            "schema_version": "1.0",
        },
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 28400,
            "id_site": 546652,
            "date": -10,
            "sum_click": 4,
        },
    }

    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",
    )
    try:
        producer.send(topic_name, key="28400", value=message).get(timeout=10)
        producer.flush()
    finally:
        producer.close()

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=BROKER,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=10_000,
        group_id=f"learnsight-it-{uuid.uuid4().hex[:8]}",
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )
    try:
        received = next(iter(consumer), None)
    finally:
        consumer.close()

    assert received is not None, "Aucun message reçu depuis Kafka"
    assert received.value == message
    validate_message("clics_vle", received.value)
