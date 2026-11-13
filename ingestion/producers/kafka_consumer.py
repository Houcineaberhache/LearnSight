"""
kafka_consumer.py — Vérification des messages produits dans Kafka.

Outil de contrôle qualité côté ingestion : il consomme un topic, affiche un
aperçu des messages et valide chaque événement contre le schéma JSON du
contrat LearnSight.

Le groupe de consommateurs est éphémère par défaut afin de ne pas perturber
les offsets des autres composants (Spark, Airflow, etc.).

Usage::

    # Vérifier les 10 premiers messages du topic clics_vle
    python -m ingestion.producers.kafka_consumer --topic clics_vle --max-messages 10

    # Vérifier tout le topic evaluations et valider les contrats
    python -m ingestion.producers.kafka_consumer --topic evaluations --validate

    # Attendre de nouveaux messages pendant 30 secondes
    python -m ingestion.producers.kafka_consumer --topic clics_vle --timeout 30000
"""

import argparse
import json
import logging
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

# Ajout du dossier parent au path pour les imports internes
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.utils.kafka_utils import load_env_config  # noqa: E402
from ingestion.utils.schema_validator import (  # noqa: E402
    TOPIC_SCHEMAS,
    MessageValidationError,
    validate_message,
)

# ---------------------------------------------------------------------------
# Configuration du logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("kafka_consumer")


# ---------------------------------------------------------------------------
# Configuration du consommateur
# ---------------------------------------------------------------------------

def build_consumer_config(
    topic: str,
    bootstrap_servers: str,
    group_id: str,
    from_beginning: bool,
    timeout_ms: int,
) -> Dict[str, Any]:
    """Construit les paramètres du KafkaConsumer.

    Séparé de la création effective pour être testable sans broker.

    Args:
        topic: Topic à consommer.
        bootstrap_servers: Adresse du broker.
        group_id: Groupe de consommateurs (éphémère par défaut).
        from_beginning: Lire depuis le début (``earliest``) ou la fin (``latest``).
        timeout_ms: Délai d'inactivité avant arrêt de l'itération.

    Returns:
        Dictionnaire d'arguments pour ``kafka.KafkaConsumer``.
    """
    return {
        "topic": topic,
        "bootstrap_servers": bootstrap_servers,
        "group_id": group_id,
        "auto_offset_reset": "earliest" if from_beginning else "latest",
        "enable_auto_commit": False,
        "consumer_timeout_ms": timeout_ms,
        "value_deserializer": lambda raw: json.loads(raw.decode("utf-8")),
        "key_deserializer": lambda raw: raw.decode("utf-8") if raw is not None else None,
    }


def make_ephemeral_group_id() -> str:
    """Génère un group_id unique pour une session de vérification."""
    return f"learnsight-verify-{uuid.uuid4().hex[:8]}"


def consume_topic(
    topic: str,
    bootstrap_servers: str,
    max_messages: Optional[int],
    timeout_ms: int,
    from_beginning: bool,
    group_id: Optional[str],
    validate: bool,
    show_messages: int,
) -> int:
    """Consomme un topic, valide les messages et affiche un rapport.

    Args:
        topic: Topic Kafka à vérifier.
        bootstrap_servers: Adresse du broker.
        max_messages: Nombre maximal de messages à lire (None = jusqu'au timeout).
        timeout_ms: Délai d'inactivité avant arrêt.
        from_beginning: Lire depuis le début du topic.
        group_id: Groupe de consommateurs (None = groupe éphémère).
        validate: Valider chaque message contre le schéma JSON du topic.
        show_messages: Nombre de messages complets à afficher.

    Returns:
        Code de sortie : 0 si aucun message invalide, 1 sinon.
    """
    from kafka import KafkaConsumer  # import tardif : permet les tests sans broker

    config = build_consumer_config(
        topic=topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id or make_ephemeral_group_id(),
        from_beginning=from_beginning,
        timeout_ms=timeout_ms,
    )
    consumer_topic = config.pop("topic")

    logger.info("=== Vérification du topic '%s' ===", topic)
    logger.info("Broker           : %s", bootstrap_servers)
    logger.info("Groupe           : %s", config["group_id"])
    logger.info("Offset de départ : %s", config["auto_offset_reset"])
    logger.info("Timeout          : %d ms", timeout_ms)
    logger.info("Validation       : %s", validate)

    consumer = KafkaConsumer(consumer_topic, **config)
    total = 0
    valid = 0
    invalid = 0
    partitions = set()
    first_offset = None
    last_offset = None

    try:
        for record in consumer:
            total += 1
            partitions.add(record.partition)
            if first_offset is None:
                first_offset = record.offset
            last_offset = record.offset

            if validate:
                try:
                    validate_message(topic, record.value)
                    valid += 1
                except MessageValidationError as exc:
                    invalid += 1
                    logger.warning(
                        "Message invalide (partition=%s, offset=%s) : %s",
                        record.partition,
                        record.offset,
                        exc,
                    )
            else:
                valid += 1

            if total <= show_messages:
                logger.info(
                    "Message #%d | partition=%s offset=%s key=%s",
                    total,
                    record.partition,
                    record.offset,
                    record.key,
                )
                print(json.dumps(record.value, ensure_ascii=False, indent=2))

            if max_messages is not None and total >= max_messages:
                logger.info("Limite de %d messages atteinte.", max_messages)
                break
    finally:
        consumer.close()

    logger.info("=== Rapport de vérification ===")
    logger.info("Messages lus        : %d", total)
    logger.info("Messages valides    : %d", valid)
    logger.info("Messages invalides  : %d", invalid)
    logger.info("Partitions touchées : %s", sorted(partitions) or "aucune")
    logger.info(
        "Offsets             : %s → %s",
        first_offset if first_offset is not None else "n/a",
        last_offset if last_offset is not None else "n/a",
    )

    if total == 0:
        logger.warning(
            "Aucun message lu. Vérifiez que les producteurs ont publié et que "
            "--from-beginning est activé pour relire les anciens messages."
        )
    elif invalid == 0:
        logger.info("✓ Tous les messages respectent le contrat du topic '%s'.", topic)
    else:
        logger.error("✗ %d message(s) ne respectent pas le contrat.", invalid)

    return 0 if invalid == 0 else 1


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Consommateur de vérification des topics Kafka LearnSight.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--topic",
        required=True,
        choices=sorted(TOPIC_SCHEMAS),
        help="Topic à vérifier.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help="Adresse du broker (défaut : KAFKA_BOOTSTRAP_SERVERS ou localhost:9092).",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Nombre maximal de messages à lire (défaut : jusqu'au timeout).",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10_000,
        help="Délai d'inactivité en millisecondes avant arrêt.",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Lire depuis le début du topic (earliest).",
    )
    parser.add_argument(
        "--group-id",
        default=None,
        help="Groupe de consommateurs (défaut : groupe éphémère unique).",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Valider chaque message contre le schéma JSON du topic.",
    )
    parser.add_argument(
        "--show-messages",
        type=int,
        default=3,
        help="Nombre de messages complets à afficher.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Niveau de log.",
    )
    return parser.parse_args()


def main() -> None:
    """Point d'entrée principal du consommateur de vérification."""
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    config = load_env_config()
    bootstrap_servers = args.bootstrap_servers or config["bootstrap_servers"]

    exit_code = consume_topic(
        topic=args.topic,
        bootstrap_servers=bootstrap_servers,
        max_messages=args.max_messages,
        timeout_ms=args.timeout,
        from_beginning=args.from_beginning,
        group_id=args.group_id,
        validate=args.validate,
        show_messages=args.show_messages,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
