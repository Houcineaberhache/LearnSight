"""
test_producers.py — Tests unitaires pour les producteurs Kafka LearnSight.

Ces tests valident le formatage des messages, la gestion des valeurs nulles,
les clés Kafka et les utilitaires partagés — sans Kafka réel (KafkaProducer mocké).
"""

import importlib.util
import math
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ajout du chemin racine du projet pour les imports
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

KAFKA_INSTALLED = importlib.util.find_spec("kafka") is not None


class TestBuildMetadata(unittest.TestCase):
    """Tests de la fonction build_metadata."""

    def setUp(self):
        from ingestion.utils.kafka_utils import build_metadata

        self.build_metadata = build_metadata

    def test_build_metadata_structure(self):
        """Vérifie que build_metadata retourne les cinq champs obligatoires."""
        meta = self.build_metadata("vle_click")
        self.assertIn("event_id", meta)
        self.assertIn("event_type", meta)
        self.assertIn("produced_at", meta)
        self.assertIn("source_system", meta)
        self.assertIn("schema_version", meta)

    def test_build_metadata_event_type(self):
        """Vérifie que event_type est correctement assigné."""
        meta = self.build_metadata("student_assessment")
        self.assertEqual(meta["event_type"], "student_assessment")

    def test_build_metadata_uuid_format(self):
        """Vérifie que event_id est un UUID valide."""
        import uuid

        meta = self.build_metadata("vle_click")
        # Doit être parsable comme UUID4
        parsed = uuid.UUID(meta["event_id"])
        self.assertEqual(parsed.version, 4)

    def test_build_metadata_source_system(self):
        """Vérifie la valeur du champ source_system."""
        meta = self.build_metadata("vle_click")
        self.assertEqual(meta["source_system"], "oulad_simulation_producer")

    def test_build_metadata_schema_version(self):
        """Vérifie la version du schéma."""
        meta = self.build_metadata("vle_click")
        self.assertEqual(meta["schema_version"], "1.0")

    def test_build_metadata_unique_ids(self):
        """Vérifie que deux appels génèrent des event_id distincts."""
        meta1 = self.build_metadata("vle_click")
        meta2 = self.build_metadata("vle_click")
        self.assertNotEqual(meta1["event_id"], meta2["event_id"])


class TestVLEMessageFormat(unittest.TestCase):
    """Tests de la construction des messages VLE."""

    def setUp(self):
        from ingestion.producers.vle_producer import build_vle_message

        self.build_vle_message = build_vle_message
        self.sample_row = {
            "code_module": "AAA",
            "code_presentation": "2013J",
            "id_student": "12345",
            "id_site": "54321",
            "date": "-10",
            "sum_click": "4",
        }

    def test_vle_message_format(self):
        """Vérifie que le message VLE contient les clés metadata et payload."""
        msg = self.build_vle_message(self.sample_row)
        self.assertIn("metadata", msg)
        self.assertIn("payload", msg)

    def test_vle_payload_fields(self):
        """Vérifie la présence de tous les champs obligatoires dans le payload."""
        msg = self.build_vle_message(self.sample_row)
        payload = msg["payload"]
        for field in ["code_module", "code_presentation", "id_student", "id_site", "date", "sum_click"]:
            self.assertIn(field, payload)

    def test_vle_payload_types(self):
        """Vérifie les types des champs du payload VLE."""
        msg = self.build_vle_message(self.sample_row)
        payload = msg["payload"]
        self.assertIsInstance(payload["code_module"], str)
        self.assertIsInstance(payload["id_student"], int)
        self.assertIsInstance(payload["id_site"], int)
        self.assertIsInstance(payload["date"], int)
        self.assertIsInstance(payload["sum_click"], int)

    def test_vle_metadata_event_type(self):
        """Vérifie que event_type est 'vle_click'."""
        msg = self.build_vle_message(self.sample_row)
        self.assertEqual(msg["metadata"]["event_type"], "vle_click")


class TestEvalMessageFormat(unittest.TestCase):
    """Tests de la construction des messages d'évaluation."""

    def setUp(self):
        from ingestion.producers.evaluations_producer import build_eval_message

        self.build_eval_message = build_eval_message
        self.sample_row = {
            "id_assessment": "1752",
            "id_student": "11391",
            "date_submitted": "18",
            "is_banked": "0",
            "score": "78.0",
        }
        self.lookup = {
            1752: {"assessment_type": "TMA", "weight": 15.0},
        }

    def test_eval_message_format(self):
        """Vérifie que le message d'évaluation contient les clés metadata et payload."""
        msg = self.build_eval_message(self.sample_row, self.lookup)
        self.assertIn("metadata", msg)
        self.assertIn("payload", msg)

    def test_eval_payload_required_fields(self):
        """Vérifie la présence des champs obligatoires dans le payload d'évaluation."""
        msg = self.build_eval_message(self.sample_row, self.lookup)
        payload = msg["payload"]
        for field in ["id_assessment", "id_student", "date_submitted", "is_banked", "score"]:
            self.assertIn(field, payload)

    def test_eval_message_format_enriched(self):
        """Vérifie l'enrichissement avec assessment_type et weight depuis le lookup."""
        msg = self.build_eval_message(self.sample_row, self.lookup)
        self.assertEqual(msg["payload"].get("assessment_type"), "TMA")
        self.assertAlmostEqual(msg["payload"].get("weight"), 15.0)

    def test_eval_metadata_event_type(self):
        """Vérifie que event_type est 'student_assessment'."""
        msg = self.build_eval_message(self.sample_row, self.lookup)
        self.assertEqual(msg["metadata"]["event_type"], "student_assessment")


class TestNullScoreHandling(unittest.TestCase):
    """Tests de la gestion des scores NaN/null."""

    def setUp(self):
        from ingestion.producers.evaluations_producer import build_eval_message

        self.build_eval_message = build_eval_message
        self.lookup = {}

    def test_null_score_handling_nan(self):
        """Vérifie que float('nan') est converti en None dans le payload JSON."""
        row = {
            "id_assessment": "100",
            "id_student": "999",
            "date_submitted": "10",
            "is_banked": "0",
            "score": float("nan"),
        }
        msg = self.build_eval_message(row, self.lookup)
        self.assertIsNone(msg["payload"]["score"])

    def test_null_score_handling_none(self):
        """Vérifie que None est conservé tel quel."""
        row = {
            "id_assessment": "100",
            "id_student": "999",
            "date_submitted": "10",
            "is_banked": "0",
            "score": None,
        }
        msg = self.build_eval_message(row, self.lookup)
        self.assertIsNone(msg["payload"]["score"])

    def test_valid_score_preserved(self):
        """Vérifie qu'un score valide est préservé comme float."""
        row = {
            "id_assessment": "100",
            "id_student": "999",
            "date_submitted": "10",
            "is_banked": "0",
            "score": "85.5",
        }
        msg = self.build_eval_message(row, self.lookup)
        self.assertAlmostEqual(msg["payload"]["score"], 85.5)


class TestKafkaKeyFormat(unittest.TestCase):
    """Tests du format de la clé Kafka (id_student en bytes)."""

    def test_kafka_key_format_vle(self):
        """Vérifie que la clé VLE est id_student encodé en bytes."""
        id_student = "12345"
        key = str(id_student).encode("utf-8")
        self.assertIsInstance(key, bytes)
        self.assertEqual(key, b"12345")

    def test_kafka_key_format_eval(self):
        """Vérifie que la clé évaluation est id_student encodé en bytes."""
        id_student = 67890
        key = str(id_student).encode("utf-8")
        self.assertIsInstance(key, bytes)
        self.assertEqual(key, b"67890")

    def test_kafka_key_decodable(self):
        """Vérifie que la clé peut être décodée en UTF-8."""
        key = b"11391"
        self.assertEqual(key.decode("utf-8"), "11391")


class TestCreateProducerConfig(unittest.TestCase):
    """Tests de la configuration du KafkaProducer (avec mock)."""

    @unittest.skipUnless(KAFKA_INSTALLED, "kafka-python non installé")
    @patch("ingestion.utils.kafka_utils.KafkaProducer")
    def test_create_producer_config(self, mock_kafka_cls):
        """Vérifie que KafkaProducer est appelé avec les bons paramètres."""
        import ingestion.utils.kafka_utils as kutils
        from ingestion.utils.kafka_utils import create_producer

        mock_kafka_cls.return_value = MagicMock()
        create_producer("localhost:9092")

        self.assertTrue(mock_kafka_cls.called)
        call_kwargs = mock_kafka_cls.call_args.kwargs

        self.assertEqual(call_kwargs["bootstrap_servers"], "localhost:9092")
        self.assertEqual(call_kwargs["compression_type"], "gzip")
        self.assertEqual(call_kwargs["acks"], "all")

    @unittest.skipUnless(KAFKA_INSTALLED, "kafka-python non installé")
    @patch("ingestion.utils.kafka_utils.KafkaProducer")
    def test_create_producer_returns_producer(self, mock_kafka_cls):
        """Vérifie que create_producer retourne bien une instance KafkaProducer."""
        from ingestion.utils.kafka_utils import create_producer

        mock_instance = MagicMock()
        mock_kafka_cls.return_value = mock_instance

        result = create_producer("localhost:9092")
        self.assertEqual(result, mock_instance)


class TestSendWithRetry(unittest.TestCase):
    """Tests de l'envoi Kafka avec retry."""

    def setUp(self):
        from ingestion.utils.kafka_utils import send_with_retry
        self.send_with_retry = send_with_retry
        self.producer = MagicMock()

    @patch("ingestion.utils.kafka_utils.time.sleep")
    def test_send_success_first_attempt(self, mock_sleep):
        """Vérifie qu'un envoi réussi ne nécessite aucun retry."""
        future = MagicMock()
        self.producer.send.return_value = future

        result = self.send_with_retry(
            self.producer, "clics_vle", b"11391", {"test": "message"}
        )

        self.assertTrue(result)
        self.producer.send.assert_called_once()
        future.get.assert_called_once_with(timeout=10)
        mock_sleep.assert_not_called()

    @patch("ingestion.utils.kafka_utils.time.sleep")
    def test_send_retry_then_success(self, mock_sleep):
        """Vérifie qu'un deuxième essai réussit après un échec."""
        future = MagicMock()
        self.producer.send.side_effect = [
            Exception("Kafka unavailable"),
            future,
        ]

        result = self.send_with_retry(
            self.producer, "clics_vle", b"11391", {"test": "message"}
        )

        self.assertTrue(result)
        self.assertEqual(self.producer.send.call_count, 2)
        mock_sleep.assert_called_once_with(2)

    @patch("ingestion.utils.kafka_utils.time.sleep")
    def test_send_returns_false_after_all_retries(self, mock_sleep):
        """Vérifie que la fonction retourne False après tous les échecs."""
        self.producer.send.side_effect = Exception("Kafka unavailable")

        result = self.send_with_retry(
            self.producer,
            "clics_vle",
            b"11391",
            {"test": "message"},
            retries=3,
        )

        self.assertFalse(result)
        self.assertEqual(self.producer.send.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 3)

class TestLoadEnvConfig(unittest.TestCase):
    """Tests du chargement de la configuration Kafka."""

    def setUp(self):
        from ingestion.utils.kafka_utils import load_env_config
        self.load_env_config = load_env_config

    @patch.dict("os.environ", {}, clear=True)
    @patch("os.path.isfile", return_value=False)
    def test_load_env_config_defaults(self, mock_isfile):
        """Vérifie les valeurs par défaut sans variables d'environnement."""
        config = self.load_env_config()

        self.assertEqual(config["bootstrap_servers"], "localhost:9092")
        self.assertEqual(config["topic_clics_vle"], "clics_vle")
        self.assertEqual(config["topic_evaluations"], "evaluations")

    @patch.dict(
        "os.environ",
        {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:29092",
            "KAFKA_TOPIC_CLICS_VLE": "custom_clics",
            "KAFKA_TOPIC_EVALUATIONS": "custom_evaluations",
        },
        clear=True,
    )
    @patch("os.path.isfile", return_value=False)
    def test_load_env_config_custom_values(self, mock_isfile):
        """Vérifie la lecture des variables d'environnement personnalisées."""
        config = self.load_env_config()

        self.assertEqual(config["bootstrap_servers"], "localhost:29092")
        self.assertEqual(config["topic_clics_vle"], "custom_clics")
        self.assertEqual(config["topic_evaluations"], "custom_evaluations")

if __name__ == "__main__":
    unittest.main(verbosity=2)
