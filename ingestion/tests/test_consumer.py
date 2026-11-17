"""
test_consumer.py — Tests du consommateur de vérification Kafka.

Le broker n'est pas requis : l'itérateur KafkaConsumer est simulé pour valider
la configuration, le comptage, la validation des contrats et le code de sortie.
"""

import importlib.util
import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.producers.kafka_consumer import (  # noqa: E402
    build_consumer_config,
    consume_topic,
    make_ephemeral_group_id,
)

KAFKA_INSTALLED = importlib.util.find_spec("kafka") is not None


def valid_vle_message() -> dict:
    return {
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


class FakeRecord:
    """Enregistrement Kafka minimal."""

    def __init__(self, value, partition=0, offset=0, key="28400"):
        self.value = value
        self.partition = partition
        self.offset = offset
        self.key = key


class FakeConsumer:
    """Remplace kafka.KafkaConsumer pour les tests hors ligne."""

    instances = []
    records = []

    def __init__(self, topic, **config):
        self.topic = topic
        self.config = config
        self.closed = False
        FakeConsumer.instances.append(self)

    def __iter__(self):
        return iter(FakeConsumer.records)

    def close(self):
        self.closed = True


class TestConsumerConfig(unittest.TestCase):
    """Tests de la configuration du consommateur."""

    def test_build_config_keys(self):
        config = build_consumer_config(
            topic="clics_vle",
            bootstrap_servers="localhost:29092",
            group_id="test-group",
            from_beginning=True,
            timeout_ms=5000,
        )
        self.assertEqual(config["topic"], "clics_vle")
        self.assertEqual(config["bootstrap_servers"], "localhost:29092")
        self.assertEqual(config["group_id"], "test-group")
        self.assertEqual(config["auto_offset_reset"], "earliest")
        self.assertFalse(config["enable_auto_commit"])
        self.assertEqual(config["consumer_timeout_ms"], 5000)
        self.assertTrue(callable(config["value_deserializer"]))

    def test_latest_offset_when_not_from_beginning(self):
        config = build_consumer_config(
            topic="evaluations",
            bootstrap_servers="localhost:9092",
            group_id="g",
            from_beginning=False,
            timeout_ms=1000,
        )
        self.assertEqual(config["auto_offset_reset"], "latest")

    def test_ephemeral_group_ids_are_unique(self):
        first = make_ephemeral_group_id()
        second = make_ephemeral_group_id()
        self.assertTrue(first.startswith("learnsight-verify-"))
        self.assertNotEqual(first, second)


@unittest.skipUnless(KAFKA_INSTALLED, "kafka-python non installé")
class TestConsumeTopic(unittest.TestCase):
    """Tests du rapport de consommation avec un KafkaConsumer simulé."""

    def setUp(self):
        FakeConsumer.instances.clear()
        FakeConsumer.records = [
            FakeRecord(valid_vle_message(), partition=0, offset=10),
            FakeRecord(valid_vle_message(), partition=1, offset=42),
        ]

    def test_valid_messages_exit_code_zero(self):
        with patch("kafka.KafkaConsumer", FakeConsumer):
            code = consume_topic(
                topic="clics_vle",
                bootstrap_servers="localhost:29092",
                max_messages=None,
                timeout_ms=1000,
                from_beginning=True,
                group_id="test-group",
                validate=True,
                show_messages=0,
            )
        self.assertEqual(code, 0)
        self.assertTrue(FakeConsumer.instances[0].closed)

    def test_invalid_message_exit_code_one(self):
        invalid = valid_vle_message()
        invalid["payload"]["sum_click"] = 0
        FakeConsumer.records = [FakeRecord(invalid)]

        with patch("kafka.KafkaConsumer", FakeConsumer):
            code = consume_topic(
                topic="clics_vle",
                bootstrap_servers="localhost:29092",
                max_messages=None,
                timeout_ms=1000,
                from_beginning=True,
                group_id="test-group",
                validate=True,
                show_messages=0,
            )
        self.assertEqual(code, 1)

    def test_max_messages_limit(self):
        with patch("kafka.KafkaConsumer", FakeConsumer):
            consume_topic(
                topic="clics_vle",
                bootstrap_servers="localhost:29092",
                max_messages=1,
                timeout_ms=1000,
                from_beginning=True,
                group_id="test-group",
                validate=False,
                show_messages=0,
            )
        # Le consommateur est fermé même après arrêt sur limite
        self.assertTrue(FakeConsumer.instances[0].closed)


if __name__ == "__main__":
    unittest.main(verbosity=2)
