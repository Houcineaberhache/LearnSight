"""
schema_validator.py — Validation des événements Kafka contre les contrats JSON.

Chaque topic Kafka de LearnSight possède un schéma JSON Draft-07 dans
``ingestion/schemas/``. Ce module centralise le chargement (avec cache) et la
validation des messages, afin que les producteurs, les consommateurs de
vérification et les tests utilisent exactement le même contrat.

Usage::

    from ingestion.utils.schema_validator import validate_message

    validate_message("clics_vle", message)  # lève MessageValidationError
"""

import json
import logging
import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import jsonschema

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Emplacement des schémas et mapping topic -> fichier de schéma
# ---------------------------------------------------------------------------
DEFAULT_SCHEMA_DIR = Path(__file__).resolve().parents[1] / "schemas"

#: Association topic Kafka -> fichier de contrat JSON Schema.
TOPIC_SCHEMAS: Dict[str, str] = {
    "clics_vle": "clics_vle_schema.json",
    "evaluations": "evaluations_schema.json",
}


class MessageValidationError(ValueError):
    """Erreur levée lorsqu'un message ne respecte pas le contrat du topic."""


def get_schema_dir() -> Path:
    """Retourne le dossier des schémas (surchargeable via l'environnement).

    La variable ``LEARNSIGHT_SCHEMA_DIR`` permet d'utiliser un autre dossier de
    contrats, par exemple dans un conteneur ou un environnement d'intégration.
    """
    override = os.getenv("LEARNSIGHT_SCHEMA_DIR")
    return Path(override) if override else DEFAULT_SCHEMA_DIR


def resolve_schema_path(topic: str) -> Path:
    """Retourne le chemin du schéma associé à un topic.

    Args:
        topic: Nom du topic Kafka (``clics_vle`` ou ``evaluations``).

    Raises:
        KeyError: Si le topic n'a pas de contrat déclaré.
        FileNotFoundError: Si le fichier de schéma est absent.
    """
    if topic not in TOPIC_SCHEMAS:
        raise KeyError(
            f"Aucun schéma déclaré pour le topic '{topic}'. "
            f"Topics connus : {', '.join(sorted(TOPIC_SCHEMAS))}"
        )

    schema_path = get_schema_dir() / TOPIC_SCHEMAS[topic]
    if not schema_path.exists():
        raise FileNotFoundError(f"Schéma introuvable pour '{topic}' : {schema_path}")
    return schema_path


@lru_cache(maxsize=None)
def load_schema(topic: str) -> Dict[str, Any]:
    """Charge et met en cache le schéma JSON d'un topic.

    Args:
        topic: Nom du topic Kafka.

    Returns:
        Le schéma JSON désérialisé.
    """
    schema_path = resolve_schema_path(topic)
    with open(schema_path, "r", encoding="utf-8") as handle:
        return json.load(handle)


@lru_cache(maxsize=None)
def get_validator(topic: str) -> jsonschema.Draft7Validator:
    """Retourne un validateur Draft-07 (avec vérification des formats) en cache.

    Le ``FormatChecker`` active la validation des formats ``uuid`` et
    ``date-time`` utilisés par les métadonnées d'événements.
    """
    return jsonschema.Draft7Validator(
        load_schema(topic),
        format_checker=jsonschema.FormatChecker(),
    )


def validate_message(topic: str, message: Dict[str, Any]) -> None:
    """Valide un message contre le contrat du topic.

    Args:
        topic: Nom du topic Kafka cible.
        message: Message complet (``metadata`` + ``payload``).

    Raises:
        MessageValidationError: Si le message est invalide, avec le chemin du
            premier champ en erreur pour faciliter le diagnostic.
    """
    errors = sorted(
        get_validator(topic).iter_errors(message),
        key=lambda error: tuple(str(part) for part in error.path),
    )
    if not errors:
        return

    first = errors[0]
    path = ".".join(str(part) for part in first.absolute_path) or "<racine>"
    raise MessageValidationError(
        f"Message invalide pour le topic '{topic}' "
        f"({len(errors)} erreur(s), première sur '{path}') : {first.message}"
    )


def is_valid(topic: str, message: Dict[str, Any]) -> bool:
    """Version non bloquante de :func:`validate_message`.

    Returns:
        ``True`` si le message respecte le contrat, ``False`` sinon.
    """
    try:
        validate_message(topic, message)
        return True
    except MessageValidationError as exc:
        logger.debug("Validation échouée : %s", exc)
        return False


def validate_batch(topic: str, messages: list) -> Dict[str, Any]:
    """Valide une liste de messages et retourne un rapport agrégé.

    Utile pour les tests d'ingestion et les scripts de vérification.

    Args:
        topic: Nom du topic Kafka cible.
        messages: Liste de messages complets.

    Returns:
        Dictionnaire ``{"total": int, "valid": int, "invalid": int, "errors": [...]}``.
    """
    report = {"total": len(messages), "valid": 0, "invalid": 0, "errors": []}
    for index, message in enumerate(messages):
        try:
            validate_message(topic, message)
            report["valid"] += 1
        except MessageValidationError as exc:
            report["invalid"] += 1
            report["errors"].append({"index": index, "error": str(exc)})
    return report
