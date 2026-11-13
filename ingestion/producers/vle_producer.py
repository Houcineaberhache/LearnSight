"""
vle_producer.py — Producteur Kafka pour les événements de clics VLE (OULAD).

Ce script lit le fichier studentVle.csv (données nettoyées) et simule un
flux de streaming en envoyant chaque ligne sous forme de message JSON vers
le topic Kafka `clics_vle`.

Usage::

    python vle_producer.py --delay 0.01 --limit 1000 --data-dir data/processed
    python vle_producer.py --dry-run --limit 100
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd

# Ajout du dossier parent au path pour les imports internes
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ingestion.utils.kafka_utils import (  # noqa: E402
    build_metadata,
    create_producer,
    load_env_config,
    send_with_retry,
)
from ingestion.utils.schema_validator import (  # noqa: E402
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
logger = logging.getLogger("vle_producer")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
TOPIC = "clics_vle"
EVENT_TYPE = "vle_click"
CSV_FILENAME = "studentVle.csv"


# ---------------------------------------------------------------------------
# Construction du message
# ---------------------------------------------------------------------------

def build_vle_message(row: dict) -> dict:
    """Construit un message JSON conforme au schéma clics_vle pour une ligne CSV.

    Args:
        row: Dictionnaire représentant une ligne de studentVle.csv.

    Returns:
        Dictionnaire JSON prêt à être envoyé sur Kafka.
    """
    return {
        "metadata": build_metadata(EVENT_TYPE),
        "payload": {
            "code_module": str(row["code_module"]),
            "code_presentation": str(row["code_presentation"]),
            "id_student": int(row["id_student"]),
            "id_site": int(row["id_site"]),
            "date": int(row["date"]),
            "sum_click": int(row["sum_click"]),
        },
    }


# ---------------------------------------------------------------------------
# Producteur principal
# ---------------------------------------------------------------------------

def produce_vle_events(
    data_dir: str,
    bootstrap_servers: str,
    delay: float,
    limit: int | None,
    chunk_size: int,
    dry_run: bool,
    validate: bool = False,
    topic: str = TOPIC,
) -> int:
    """Lit studentVle.csv et envoie les événements sur Kafka.

    Args:
        data_dir: Chemin vers le dossier contenant les données nettoyées.
        bootstrap_servers: Adresse du broker Kafka.
        delay: Délai en secondes entre chaque message.
        limit: Nombre maximal de messages à envoyer (None = tous).
        chunk_size: Nombre de lignes lues par chunk pandas.
        dry_run: Si True, construit les messages sans les envoyer.
        validate: Si True (ou en dry-run), valide chaque message contre le schéma.
        topic: Nom du topic Kafka cible (par défaut ``clics_vle``).

    Returns:
        Nombre total de messages envoyés avec succès.
    """
    csv_path = Path(data_dir) / CSV_FILENAME
    if not csv_path.exists():
        logger.error("Fichier introuvable : %s", csv_path)
        sys.exit(1)

    producer = None
    if not dry_run:
        producer = create_producer(bootstrap_servers)

    total_sent = 0
    total_errors = 0
    total_invalid = 0
    start_time = time.time()

    try:
        for chunk_idx, chunk in enumerate(
            pd.read_csv(csv_path, chunksize=chunk_size, dtype=str)
        ):
            logger.info("Traitement du chunk %d (%d lignes)", chunk_idx + 1, len(chunk))

            for _, row in chunk.iterrows():
                if limit is not None and total_sent >= limit:
                    logger.info("Limite de %d événements atteinte.", limit)
                    return total_sent

                try:
                    message = build_vle_message(row.to_dict())
                except (ValueError, KeyError) as exc:
                    logger.warning("Ligne ignorée (erreur de formatage) : %s", exc)
                    total_errors += 1
                    continue

                if validate or dry_run:
                    try:
                        validate_message(topic, message)
                    except MessageValidationError as exc:
                        logger.warning("Message rejeté par le contrat : %s", exc)
                        total_invalid += 1
                        continue

                if dry_run:
                    logger.debug("[DRY-RUN] Message VLE : %s", message["metadata"]["event_id"])
                    total_sent += 1
                else:
                    key = str(row["id_student"]).encode("utf-8")
                    success = send_with_retry(producer, topic, key, message)
                    if success:
                        total_sent += 1
                    else:
                        total_errors += 1

                if delay > 0:
                    time.sleep(delay)

                # Log de progression toutes les 10 000 lignes
                if total_sent % 10_000 == 0 and total_sent > 0:
                    elapsed = time.time() - start_time
                    rate = total_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        "Progression : %d événements envoyés (%.1f msg/s) | Erreurs : %d | Invalides : %d",
                        total_sent,
                        rate,
                        total_errors,
                        total_invalid,
                    )
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
            logger.info("Producteur Kafka fermé proprement.")

    elapsed = time.time() - start_time
    logger.info(
        "Terminé — %d événements VLE envoyés en %.1fs (%d erreurs, %d invalides).",
        total_sent,
        elapsed,
        total_errors,
        total_invalid,
    )
    return total_sent


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Producteur Kafka pour les clics VLE (OULAD).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Répertoire contenant studentVle.csv.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.01,
        help="Délai en secondes entre chaque message.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Nombre maximal de messages à envoyer (défaut : illimité).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="Nombre de lignes lues par chunk pandas.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Construit et valide les messages sans les envoyer à Kafka.",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Valide chaque message contre le schéma JSON avant l'envoi.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Niveau de log.",
    )
    return parser.parse_args()


def main() -> None:
    """Point d'entrée principal du producteur VLE."""
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    config = load_env_config()
    bootstrap_servers = config["bootstrap_servers"]

    logger.info("=== Démarrage du producteur VLE ===")
    logger.info("Broker Kafka     : %s", bootstrap_servers)
    logger.info("Topic cible      : %s", config["topic_clics_vle"])
    logger.info("Dossier données  : %s", args.data_dir)
    logger.info("Délai            : %.3fs", args.delay)
    logger.info("Limite           : %s", args.limit or "illimitée")
    logger.info("Chunk size       : %d", args.chunk_size)
    logger.info("Validation       : %s", args.validate or args.dry_run)
    logger.info("Mode dry-run     : %s", args.dry_run)

    produce_vle_events(
        data_dir=args.data_dir,
        bootstrap_servers=bootstrap_servers,
        delay=args.delay,
        limit=args.limit,
        chunk_size=args.chunk_size,
        dry_run=args.dry_run,
        validate=args.validate,
        topic=config["topic_clics_vle"],
    )


if __name__ == "__main__":
    main()
