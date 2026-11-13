"""
kafka_utils.py — Utilitaires partagés pour les producteurs Kafka LearnSight.

Ce module fournit des fonctions communes : génération de métadonnées, création
du producteur Kafka, envoi avec retry exponentiel et chargement de la configuration
depuis les variables d'environnement.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from kafka import KafkaProducer  # type: ignore  # noqa: F401
except ImportError:
    KafkaProducer = None  # type: ignore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Chargement de la configuration
# ---------------------------------------------------------------------------

def load_env_config() -> Dict[str, str]:
    """Charge la configuration depuis les variables d'environnement.

    Tente d'abord de charger un fichier .env si python-dotenv est installé.

    Returns:
        Dictionnaire contenant les variables de configuration pertinentes.
    """
    try:
        from dotenv import load_dotenv
        # Cherche .env à partir du répertoire courant ou de la racine du projet
        for candidate in [".env", "../../.env", "../../../.env"]:
            if os.path.isfile(candidate):
                load_dotenv(candidate)
                logger.debug("Fichier .env chargé depuis : %s", candidate)
                break
    except ImportError:
        logger.debug("python-dotenv non installé — variables d'env brutes utilisées.")

    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "topic_clics_vle": os.getenv("KAFKA_TOPIC_CLICS_VLE", "clics_vle"),
        "topic_evaluations": os.getenv("KAFKA_TOPIC_EVALUATIONS", "evaluations"),
    }


# ---------------------------------------------------------------------------
# Génération des métadonnées
# ---------------------------------------------------------------------------

def build_metadata(event_type: str) -> Dict[str, str]:
    """Génère un dictionnaire de métadonnées standard pour un événement Kafka.

    Args:
        event_type: Type de l'événement (ex: "vle_click", "student_assessment").

    Returns:
        Dictionnaire de métadonnées conforme aux schémas JSON du projet.
    """
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "source_system": "oulad_simulation_producer",
        "schema_version": "1.0",
    }


# ---------------------------------------------------------------------------
# Création du producteur
# ---------------------------------------------------------------------------

def create_producer(bootstrap_servers: str, retries: int = 3):
    """Crée et retourne un KafkaProducer configuré.

    Le producteur utilise la sérialisation JSON et la compression gzip.

    Args:
        bootstrap_servers: Adresse(s) du broker Kafka (ex: "localhost:9092").
        retries: Nombre de tentatives de reconnexion en cas d'échec initial.

    Returns:
        Instance configurée de KafkaProducer.

    Raises:
        RuntimeError: Si la connexion au broker échoue après tous les retries.
    """
    from kafka import KafkaProducer as _KafkaProducer  # type: ignore
    _Producer = KafkaProducer if KafkaProducer is not None else _KafkaProducer
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            producer = _Producer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k if isinstance(k, bytes) else str(k).encode("utf-8"),
                compression_type="gzip",
                acks="all",
                retries=5,
                max_block_ms=10_000,
            )
            logger.info(
                "Connexion au broker Kafka établie (tentative %d/%d) : %s",
                attempt,
                retries,
                bootstrap_servers,
            )
            return producer
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = 2 ** attempt
            logger.warning(
                "Échec connexion Kafka (tentative %d/%d) : %s — nouvelle tentative dans %ds",
                attempt,
                retries,
                exc,
                wait,
            )
            time.sleep(wait)

    raise RuntimeError(
        f"Impossible de se connecter au broker Kafka après {retries} tentatives : {last_exc}"
    )


# ---------------------------------------------------------------------------
# Envoi avec retry exponentiel
# ---------------------------------------------------------------------------

def send_with_retry(
    producer: Any,
    topic: str,
    key: bytes,
    value: Dict[str, Any],
    retries: int = 3,
) -> bool:
    """Envoie un message Kafka avec retry en cas d'échec.

    Utilise un backoff exponentiel entre les tentatives.

    Args:
        producer: Instance KafkaProducer.
        topic: Nom du topic Kafka cible.
        key: Clé du message (bytes).
        value: Valeur du message (dict sérialisable en JSON).
        retries: Nombre de tentatives maximales.

    Returns:
        True si l'envoi a réussi, False sinon.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            future = producer.send(topic, key=key, value=value)
            future.get(timeout=10)  # Bloquant pour confirmer l'envoi
            return True
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = 2 ** attempt
            logger.warning(
                "[%s] Échec envoi message (tentative %d/%d) : %s — retry dans %ds",
                topic,
                attempt,
                retries,
                exc,
                wait,
            )
            time.sleep(wait)

    logger.error("[%s] Abandon après %d tentatives : %s", topic, retries, last_exc)
    return False
