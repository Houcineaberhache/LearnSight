"""
test_kafka_admin.py — Tests de la configuration des topics Kafka.

Vérifie la cohérence entre le fichier ``config/kafka/kafka_topics.yml`` et la
configuration utilisée par ``kafka_admin.py`` (partitions, rétention).
"""

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ingestion.producers.kafka_admin import (  # noqa: E402
    TOPICS_CONFIG,
    load_topics_config,
)

YAML_INSTALLED = importlib.util.find_spec("yaml") is not None


class TestTopicsConfig(unittest.TestCase):
    """Tests de la configuration des topics."""

    def test_expected_topics_are_configured(self):
        self.assertEqual(set(TOPICS_CONFIG), {"clics_vle", "evaluations"})

    def test_clics_and_evaluations_are_partitioned(self):
        self.assertEqual(TOPICS_CONFIG["clics_vle"]["num_partitions"], 3)
        self.assertEqual(TOPICS_CONFIG["evaluations"]["num_partitions"], 3)

    def test_click_retention_is_seven_days(self):
        self.assertEqual(
            TOPICS_CONFIG["clics_vle"]["config"]["retention.ms"], "604800000"
        )

    @unittest.skipUnless(YAML_INSTALLED, "PyYAML non installé")
    def test_load_topics_config_from_yaml_file(self):
        yaml_content = """
topics:
  custom_topic:
    partitions: 2
    replication_factor: 1
    retention_ms: 3600000
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "topics.yml"
            path.write_text(yaml_content, encoding="utf-8")
            config = load_topics_config(path)

        self.assertIn("custom_topic", config)
        self.assertEqual(config["custom_topic"]["num_partitions"], 2)
        self.assertEqual(config["custom_topic"]["config"]["retention.ms"], "3600000")


if __name__ == "__main__":
    unittest.main(verbosity=2)
