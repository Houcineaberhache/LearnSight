"""
test_validator.py — Tests du validateur de contrats JSON Kafka.

Couvre le chargement des schémas, la validation des deux topics de streaming
(`clics_vle`, `evaluations`) et le rejet des messages non conformes.
"""

import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.utils.schema_validator import (  # noqa: E402
    TOPIC_SCHEMAS,
    MessageValidationError,
    is_valid,
    load_schema,
    resolve_schema_path,
    validate_batch,
    validate_message,
)


def make_metadata(event_type: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "source_system": "oulad_simulation_producer",
        "schema_version": "1.0",
    }


def valid_vle_message() -> dict:
    return {
        "metadata": make_metadata("vle_click"),
        "payload": {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": 28400,
            "id_site": 546652,
            "date": -10,
            "sum_click": 4,
        },
    }


class TestSchemaLoading(unittest.TestCase):
    """Tests de résolution et de chargement des schémas."""

    def test_all_topics_have_a_schema_file(self):
        for topic in TOPIC_SCHEMAS:
            self.assertTrue(resolve_schema_path(topic).exists())

    def test_load_schema_returns_draft07(self):
        schema = load_schema("clics_vle")
        self.assertEqual(schema["$schema"], "http://json-schema.org/draft-07/schema#")

    def test_unknown_topic_raises(self):
        with self.assertRaises(KeyError):
            resolve_schema_path("topic_inconnu")


class TestValidateMessage(unittest.TestCase):
    """Tests de validation des messages."""

    def test_valid_vle_message(self):
        validate_message("clics_vle", valid_vle_message())

    def test_invalid_sum_click_is_rejected(self):
        message = valid_vle_message()
        message["payload"]["sum_click"] = 0
        with self.assertRaises(MessageValidationError):
            validate_message("clics_vle", message)

    def test_missing_metadata_is_rejected(self):
        message = valid_vle_message()
        del message["metadata"]
        with self.assertRaises(MessageValidationError):
            validate_message("clics_vle", message)

    def test_invalid_uuid_is_rejected(self):
        message = valid_vle_message()
        message["metadata"]["event_id"] = "not-a-uuid"
        with self.assertRaises(MessageValidationError):
            validate_message("clics_vle", message)

    def test_unknown_topic_is_rejected(self):
        """Les profils étudiants sont un référentiel statique, pas un topic Kafka."""
        with self.assertRaises(KeyError):
            validate_message("student_profiles", {})

    def test_is_valid_returns_boolean(self):
        self.assertTrue(is_valid("clics_vle", valid_vle_message()))

        message = valid_vle_message()
        message["payload"]["id_student"] = "pas-un-entier"
        self.assertFalse(is_valid("clics_vle", message))


class TestValidateBatch(unittest.TestCase):
    """Tests du rapport agrégé de validation."""

    def test_batch_report_counts(self):
        valid = valid_vle_message()
        invalid = valid_vle_message()
        invalid["payload"]["sum_click"] = 0

        report = validate_batch("clics_vle", [valid, invalid, valid])
        self.assertEqual(report["total"], 3)
        self.assertEqual(report["valid"], 2)
        self.assertEqual(report["invalid"], 1)
        self.assertEqual(len(report["errors"]), 1)
        self.assertEqual(report["errors"][0]["index"], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
