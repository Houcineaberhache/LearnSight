"""
run_all_producers.py — Orchestrateur des producteurs Kafka LearnSight.

Lance les producteurs VLE et Évaluations en parallèle via multiprocessing.
Affiche le statut de chaque processus en temps réel et attend leur complétion.

Usage::

    python -m ingestion.producers.run_all_producers --delay 0.01 --limit 1000
    python -m ingestion.producers.run_all_producers --dry-run
"""

import argparse
import logging
import multiprocessing
import sys
import time
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Configuration du logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("run_all_producers")


# ---------------------------------------------------------------------------
# Fonctions cibles pour multiprocessing
# ---------------------------------------------------------------------------

def run_vle(
    data_dir: str,
    delay: float,
    limit: Optional[int],
    chunk_size: int,
    dry_run: bool,
    validate: bool = False,
) -> None:
    """Fonction cible pour le processus VLE."""
    # Import ici pour éviter les problèmes de pickling sur Windows
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ingestion.producers.vle_producer import produce_vle_events
    from ingestion.utils.kafka_utils import load_env_config

    config = load_env_config()
    produce_vle_events(
        data_dir=data_dir,
        bootstrap_servers=config["bootstrap_servers"],
        delay=delay,
        limit=limit,
        chunk_size=chunk_size,
        dry_run=dry_run,
        validate=validate,
        topic=config["topic_clics_vle"],
    )


def run_evaluations(
    data_dir: str,
    delay: float,
    limit: Optional[int],
    chunk_size: int,
    dry_run: bool,
    validate: bool = False,
) -> None:
    """Fonction cible pour le processus Évaluations."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from ingestion.producers.evaluations_producer import produce_eval_events
    from ingestion.utils.kafka_utils import load_env_config

    config = load_env_config()
    produce_eval_events(
        data_dir=data_dir,
        bootstrap_servers=config["bootstrap_servers"],
        delay=delay,
        limit=limit,
        chunk_size=chunk_size,
        dry_run=dry_run,
        validate=validate,
        topic=config["topic_evaluations"],
    )


# ---------------------------------------------------------------------------
# Orchestrateur
# ---------------------------------------------------------------------------

def run_all(
    data_dir: str,
    delay: float,
    limit: Optional[int],
    chunk_size: int,
    dry_run: bool,
    validate: bool = False,
) -> None:
    """Lance les producteurs en parallèle et supervise leur exécution.

    Args:
        data_dir: Chemin vers le dossier contenant les données nettoyées.
        delay: Délai entre chaque message.
        limit: Limite d'événements par producteur.
        chunk_size: Taille des chunks pandas.
        dry_run: Mode sans envoi Kafka réel.
        validate: Valider chaque message contre son schéma JSON.
    """
    producers = [
        {
            "name": "VLE Producer",
            "target": run_vle,
        },
        {
            "name": "Evaluations Producer",
            "target": run_evaluations,
        },
    ]

    args = (data_dir, delay, limit, chunk_size, dry_run, validate)
    processes = []

    logger.info("=== Lancement des producteurs en parallèle ===")
    for prod in producers:
        p = multiprocessing.Process(
            target=prod["target"],
            args=args,
            name=prod["name"],
        )
        p.start()
        logger.info("[DÉMARRÉ] %s (PID: %d)", prod["name"], p.pid)
        processes.append((prod["name"], p))

    # Surveillance des processus
    start_time = time.time()
    while any(p.is_alive() for _, p in processes):
        time.sleep(5)
        for name, p in processes:
            status = "EN COURS" if p.is_alive() else f"TERMINÉ (code={p.exitcode})"
            elapsed = time.time() - start_time
            logger.info("[%s] %s — %.0fs écoulées", name, status, elapsed)

    # Rapport final
    logger.info("=== Rapport final ===")
    all_ok = True
    for name, p in processes:
        code = p.exitcode
        if code == 0:
            logger.info("✓ %s : succès (code=%d)", name, code)
        else:
            logger.error("✗ %s : ÉCHEC (code=%d)", name, code)
            all_ok = False

    if not all_ok:
        sys.exit(1)
    logger.info("Tous les producteurs ont terminé avec succès.")


# ---------------------------------------------------------------------------
# Point d'entrée CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse les arguments de la ligne de commande."""
    parser = argparse.ArgumentParser(
        description="Orchestrateur des producteurs Kafka LearnSight.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--data-dir",
        default="data/processed",
        help="Répertoire contenant les fichiers CSV nettoyés.",
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
        help="Nombre maximal de messages par producteur (défaut : illimité).",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=10_000,
        help="Taille des chunks pandas.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mode test sans envoi réel vers Kafka (valide les messages).",
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
    """Point d'entrée principal de l'orchestrateur."""
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)

    logger.info("=== LearnSight — Orchestrateur Kafka ===")
    logger.info("Dossier données : %s", args.data_dir)
    logger.info("Délai           : %.3fs", args.delay)
    logger.info("Limite          : %s", args.limit or "illimitée")
    logger.info("Validation      : %s", args.validate or args.dry_run)
    logger.info("Dry-run         : %s", args.dry_run)

    run_all(
        data_dir=args.data_dir,
        delay=args.delay,
        limit=args.limit,
        chunk_size=args.chunk_size,
        dry_run=args.dry_run,
        validate=args.validate,
    )


if __name__ == "__main__":
    # Nécessaire pour multiprocessing sur Windows
    multiprocessing.freeze_support()
    main()
