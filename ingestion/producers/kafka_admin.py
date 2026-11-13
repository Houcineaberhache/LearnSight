"""
kafka_admin.py — Administration des topics Kafka pour LearnSight.

Ce script permet de créer, lister et vérifier les topics Kafka nécessaires au
pipeline d'ingestion. Il utilise KafkaAdminClient de la bibliothèque kafka-python.

Usage::

    python kafka_admin.py --create
    python kafka_admin.py --list
    python kafka_admin.py --check
    python kafka_admin.py --bootstrap-servers kafka:9092 --create --list
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Configuration du logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("kafka_admin")

# ---------------------------------------------------------------------------
# Configuration par défaut des topics (fallback si PyYAML est absent)
# ---------------------------------------------------------------------------
DEFAULT_TOPICS_CONFIG: Dict[str, Dict[str, Any]] = {
    "clics_vle": {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "604800000"},  # 7 jours
    },
    "evaluations": {
        "num_partitions": 3,
        "replication_factor": 1,
        "config": {"retention.ms": "604800000"},
    },
}

TOPICS_CONFIG_PATH = (
    Path(__file__).resolve().parents[2] / "config" / "kafka" / "kafka_topics.yml"
)


def load_topics_config(path: Path = TOPICS_CONFIG_PATH) -> Dict[str, Dict[str, Any]]:
    """Charge la configuration des topics depuis ``kafka_topics.yml``.

    Le fichier YAML est la source de vérité partagée avec l'équipe. Si PyYAML
    n'est pas installé ou si le fichier est absent, les valeurs par défaut
    codées dans ce module sont utilisées afin de ne jamais bloquer l'ingestion.

    Args:
        path: Chemin du fichier de configuration YAML.

    Returns:
        Dictionnaire ``{topic: {num_partitions, replication_factor, config}}``.
    """
    try:
        import yaml  # type: ignore
    except ImportError:
        logger.debug("PyYAML absent — configuration par défaut utilisée.")
        return DEFAULT_TOPICS_CONFIG

    if not path.exists():
        logger.debug("Fichier %s absent — configuration par défaut utilisée.", path)
        return DEFAULT_TOPICS_CONFIG

    try:
        with open(path, "r", encoding="utf-8") as handle:
            document = yaml.safe_load(handle) or {}
    except (OSError, yaml.YAMLError) as exc:  # type: ignore[attr-defined]
        logger.warning("Lecture de %s impossible (%s) — valeurs par défaut.", path, exc)
        return DEFAULT_TOPICS_CONFIG

    topics = document.get("topics") or {}
    if not topics:
        logger.warning("Aucun topic dans %s — valeurs par défaut.", path)
        return DEFAULT_TOPICS_CONFIG

    loaded: Dict[str, Dict[str, Any]] = {}
    for name, cfg in topics.items():
        cfg = cfg or {}
        loaded[name] = {
            "num_partitions": int(cfg.get("partitions", 1)),
            "replication_factor": int(cfg.get("replication_factor", 1)),
            "config": {"retention.ms": str(cfg.get("retention_ms", -1))},
        }

    logger.debug("Configuration des topics chargée depuis %s.", path)
    return loaded


TOPICS_CONFIG = load_topics_config()


# ---------------------------------------------------------------------------
# Fonctions d'administration
# ---------------------------------------------------------------------------

def check_connectivity(bootstrap_servers: str) -> bool:
    """Vérifie la connectivité au broker Kafka.

    Args:
        bootstrap_servers: Adresse du broker Kafka.

    Returns:
        True si le broker est accessible, False sinon.
    """
    try:
        from kafka import KafkaAdminClient  # type: ignore

        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="learnsight-admin-check",
            request_timeout_ms=5000,
        )
        admin.close()
        logger.info("✓ Broker Kafka accessible : %s", bootstrap_servers)
        return True
    except Exception as exc:  # noqa: BLE001
        logger.error("✗ Broker Kafka inaccessible (%s) : %s", bootstrap_servers, exc)
        return False


def list_topics(bootstrap_servers: str) -> List[str]:
    """Liste les topics présents sur le broker Kafka.

    Args:
        bootstrap_servers: Adresse du broker Kafka.

    Returns:
        Liste des noms de topics existants.
    """
    from kafka import KafkaAdminClient  # type: ignore

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="learnsight-admin-list",
    )
    try:
        topics = admin.list_topics()
        # Filtrer les topics internes Kafka
        public_topics = [t for t in topics if not t.startswith("__")]
        logger.info("Topics existants (%d) :", len(public_topics))
        for topic in sorted(public_topics):
            logger.info("  - %s", topic)
        return public_topics
    finally:
        admin.close()


def create_topics(bootstrap_servers: str, topics: List[str] | None = None) -> None:
    """Crée les topics Kafka s'ils n'existent pas déjà.

    Args:
        bootstrap_servers: Adresse du broker Kafka.
        topics: Liste des topics à créer (None = tous les topics configurés).
    """
    from kafka import KafkaAdminClient  # type: ignore
    from kafka.admin import NewTopic  # type: ignore
    from kafka.errors import TopicAlreadyExistsError  # type: ignore

    topics_to_create = topics or list(TOPICS_CONFIG.keys())

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="learnsight-admin-create",
    )
    try:
        existing = admin.list_topics()
        new_topics = []

        for topic_name in topics_to_create:
            if topic_name in existing:
                logger.info("Topic déjà existant (ignoré) : %s", topic_name)
                continue

            cfg = TOPICS_CONFIG.get(topic_name)
            if cfg is None:
                logger.warning("Configuration inconnue pour le topic : %s", topic_name)
                continue

            new_topics.append(
                NewTopic(
                    name=topic_name,
                    num_partitions=cfg["num_partitions"],
                    replication_factor=cfg["replication_factor"],
                    topic_configs=cfg.get("config", {}),
                )
            )
            logger.info(
                "Topic à créer : %s (partitions=%d, replication=%d)",
                topic_name,
                cfg["num_partitions"],
                cfg["replication_factor"],
            )

        if not new_topics:
            logger.info("Aucun nouveau topic à créer.")
            return

        admin.create_topics(new_topics=new_topics, validate_only=False)
        logger.info("✓ %d topic(s) créé(s) avec succès.", len(new_topics))

    except TopicAlreadyExistsError:
        logger.info("Topics déjà existants — aucune action requise.")
    except Exception as exc:  # noqa: BLE001
        logger.error("Erreur lors de la création des topics : %s", exc)
        raise
    finally:
        admin.close()


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Administration des topics Kafka pour LearnSight.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=None,
        help=(
            "Adresse du broker Kafka. "
            "Utilise KAFKA_BOOTSTRAP_SERVERS si non fourni (défaut: localhost:9092)."
        ),
    )
    parser.add_argument(
        "--create",
        action="store_true",
        help="Crée les topics configurés s'ils n'existent pas.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Liste les topics existants sur le broker.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Vérifie la connectivité au broker Kafka.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Niveau de log.",
    )
    return parser.parse_args()


def main() -> None:
    """Point d'entrée principal du script d'administration Kafka."""
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    # Résolution du bootstrap_servers
    import os

    bootstrap_servers = args.bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    )

    if not any([args.create, args.list, args.check]):
        logger.error("Aucune action spécifiée. Utilisez --create, --list ou --check.")
        sys.exit(1)

    logger.info("=== Administration Kafka LearnSight ===")
    logger.info("Broker : %s", bootstrap_servers)

    if args.check:
        ok = check_connectivity(bootstrap_servers)
        if not ok:
            sys.exit(1)

    if args.create:
        create_topics(bootstrap_servers)

    if args.list:
        list_topics(bootstrap_servers)


if __name__ == "__main__":
    main()
