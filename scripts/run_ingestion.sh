#!/usr/bin/env bash
# =============================================================================
# LearnSight — Lancement du pipeline d'ingestion (Fatima)
# =============================================================================
# Variables d'environnement :
#   DATA_DIR      Répertoire des CSV nettoyés       (défaut: data/processed)
#   DELAY         Délai entre deux messages          (défaut: 0.01)
#   LIMIT         Limite par producteur, 0 = tout    (défaut: 0)
#   SKIP_TOPICS   1 pour ne pas créer les topics     (défaut: 0)
#   DRY_RUN       1 pour valider sans envoyer        (défaut: 0)
#   VALIDATE      1 pour valider chaque message      (défaut: 0)
#
# Exemples :
#   ./scripts/run_ingestion.sh
#   LIMIT=1000 DELAY=0.01 ./scripts/run_ingestion.sh
#   DRY_RUN=1 LIMIT=50 ./scripts/run_ingestion.sh
# =============================================================================
set -euo pipefail

DATA_DIR="${DATA_DIR:-data/processed}"
DELAY="${DELAY:-0.01}"
LIMIT="${LIMIT:-0}"
SKIP_TOPICS="${SKIP_TOPICS:-0}"
DRY_RUN="${DRY_RUN:-0}"
VALIDATE="${VALIDATE:-0}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PYTHON="python3"
if [[ -x "venv/bin/python" ]]; then
    PYTHON="venv/bin/python"
elif [[ -x ".venv/bin/python" ]]; then
    PYTHON=".venv/bin/python"
fi

echo "Python : $PYTHON"

if [[ "$SKIP_TOPICS" != "1" ]]; then
    echo "Création / vérification des topics Kafka..."
    "$PYTHON" -m ingestion.producers.kafka_admin --create --list
fi

ARGS=(
    -m ingestion.producers.run_all_producers
    --data-dir "$DATA_DIR"
    --delay "$DELAY"
)

if [[ "$LIMIT" != "0" ]]; then
    ARGS+=(--limit "$LIMIT")
fi
if [[ "$DRY_RUN" == "1" ]]; then
    ARGS+=(--dry-run)
fi
if [[ "$VALIDATE" == "1" ]]; then
    ARGS+=(--validate)
fi

echo "Lancement des producteurs..."
exec "$PYTHON" "${ARGS[@]}"
