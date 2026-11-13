"""
evaluations_producer.py — Producteur Kafka pour les événements d'évaluations (OULAD).

Ce script lit studentAssessment.csv et assessments.csv (données nettoyées),
les joint sur id_assessment pour enrichir chaque événement avec le type et le
poids de l'évaluation, puis envoie les messages sur le topic Kafka `evaluations`.

Usage::

    python evaluations_producer.py --delay 0.01 --limit 500 --data-dir data/processed
    python evaluations_producer.py --dry-run --limit 100
"""

import argparse
import logging
import math
import sys
import time
from pathlib import Path
from typing import Optional

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
logger = logging.getLogger("evaluations_producer")

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
TOPIC = "evaluations"
EVENT_TYPE = "student_assessment"
CSV_ASSESSMENTS = "studentAssessment.csv"
CSV_META = "assessments.csv"


# ---------------------------------------------------------------------------
# Chargement et enrichissement des données
# ---------------------------------------------------------------------------

def load_assessments_lookup(data_dir: str) -> dict:
    """Charge assessments.csv et construit un dictionnaire de lookup.

    Args:
        data_dir: Chemin vers le dossier contenant assessments.csv.

    Returns:
        Dictionnaire {id_assessment: {"assessment_type": ..., "weight": ...}}.
    """
    meta_path = Path(data_dir) / CSV_META
    if not meta_path.exists():
        logger.warning(
            "Fichier assessments.csv introuvable (%s) — enrichissement désactivé.", meta_path
        )
        return {}

    df_meta = pd.read_csv(meta_path, dtype=str)
    lookup: dict = {}
    for _, row in df_meta.iterrows():
        try:
            aid = int(row["id_assessment"])
            lookup[aid] = {
                "assessment_type": str(row.get("assessment_type", "")),
                "weight": float(row["weight"]) if pd.notna(row.get("weight")) else None,
            }
        except (ValueError, KeyError):
            pass

    logger.info("Lookup évaluations chargé : %d entrées.", len(lookup))
    return lookup


# ---------------------------------------------------------------------------
# Construction du message
# ---------------------------------------------------------------------------

def build_eval_message(row: dict, lookup: dict) -> dict:
    """Construit un message JSON conforme au schéma evaluations pour une ligne CSV.

    Les scores NaN sont convertis en None (null JSON). L'enrichissement via
    assessments.csv (assessment_type, weight) est appliqué si disponible.

    Args:
        row: Dictionnaire représentant une ligne de studentAssessment.csv.
        lookup: Dictionnaire de metadata des évaluations (depuis assessments.csv).

    Returns:
        Dictionnaire JSON prêt à être envoyé sur Kafka.
    """
    id_assessment = int(row["id_assessment"])
    id_student = int(row["id_student"])
    date_submitted = int(row["date_submitted"])
    is_banked = str(row.get("is_banked", "0")).strip() in ("1", "True", "true")

    # Gestion des scores null / NaN
    score_raw = row.get("score")
    if score_raw is None or (isinstance(score_raw, float) and math.isnan(score_raw)):
        score = None
    else:
        try:
            score = float(score_raw)
        except (ValueError, TypeError):
            score = None

    meta = lookup.get(id_assessment, {})
    assessment_type = meta.get("assessment_type") or None
    weight = meta.get("weight")

    payload: dict = {
        "id_assessment": id_assessment,
        "id_student": id_student,
        "date_submitted": date_submitted,
        "is_banked": is_banked,
        "score": score,
    }
    if assessment_type:
        payload["assessment_type"] = assessment_type
    if weight is not None:
        payload["weight"] = weight

    return {
        "metadata": build_metadata(EVENT_TYPE),
        "payload": payload,
    }


# ---------------------------------------------------------------------------
# Producteur principal
# ---------------------------------------------------------------------------

def produce_eval_events(
    data_dir: str,
    bootstrap_servers: str,
    delay: float,
    limit: Optional[int],
    chunk_size: int,
    dry_run: bool,
    validate: bool = False,
    topic: str = TOPIC,
) -> int:
    """Lit studentAssessment.csv et envoie les événements sur Kafka.

    Args:
        data_dir: Chemin vers le dossier contenant les données nettoyées.
        bootstrap_servers: Adresse du broker Kafka.
        delay: Délai en secondes entre chaque message.
        limit: Nombre maximal de messages à envoyer (None = tous).
        chunk_size: Nombre de lignes lues par chunk pandas.
        dry_run: Si True, construit les messages sans les envoyer.
        validate: Si True (ou en dry-run), valide chaque message contre le schéma.
        topic: Nom du topic Kafka cible (par défaut ``evaluations``).

    Returns:
        Nombre total de messages envoyés avec succès.
    """
    csv_path = Path(data_dir) / CSV_ASSESSMENTS
    if not csv_path.exists():
        logger.error("Fichier introuvable : %s", csv_path)
        sys.exit(1)

    # Chargement du lookup pour l'enrichissement
    lookup = load_assessments_lookup(data_dir)

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
                    message = build_eval_message(row.to_dict(), lookup)
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
                    logger.debug(
                        "[DRY-RUN] Message EVAL : %s", message["metadata"]["event_id"]
                    )
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

                if total_sent % 5_000 == 0 and total_sent > 0:
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
        "Terminé — %d événements d'évaluation envoyés en %.1fs (%d erreurs, %d invalides).",
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
        description="Producteur Kafka pour les évaluations étudiantes (OULAD).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Répertoire contenant studentAssessment.csv et assessments.csv.",
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
    """Point d'entrée principal du producteur d'évaluations."""
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    config = load_env_config()
    bootstrap_servers = config["bootstrap_servers"]

    logger.info("=== Démarrage du producteur ÉVALUATIONS ===")
    logger.info("Broker Kafka     : %s", bootstrap_servers)
    logger.info("Topic cible      : %s", config["topic_evaluations"])
    logger.info("Dossier données  : %s", args.data_dir)
    logger.info("Délai            : %.3fs", args.delay)
    logger.info("Limite           : %s", args.limit or "illimitée")
    logger.info("Chunk size       : %d", args.chunk_size)
    logger.info("Validation       : %s", args.validate or args.dry_run)
    logger.info("Mode dry-run     : %s", args.dry_run)

    produce_eval_events(
        data_dir=args.data_dir,
        bootstrap_servers=bootstrap_servers,
        delay=args.delay,
        limit=args.limit,
        chunk_size=args.chunk_size,
        dry_run=args.dry_run,
        validate=args.validate,
        topic=config["topic_evaluations"],
    )


if __name__ == "__main__":
    main()
