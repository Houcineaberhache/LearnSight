"""
DAG principal LearnSight : pipeline hebdomadaire (Étudiante 4).

Chaque étape appelle un script Spark écrit par un autre étudiant.
Tant que le script n'existe pas, l'étape affiche un message [PLACEHOLDER]
et réussit, ce qui permet de tester le DAG sans attendre les autres.
Dès que le vrai script arrive dans le dépôt, l'étape le lance
automatiquement avec spark-submit : aucun changement de code nécessaire.

Contrat proposé pour chaque script :
  - il accepte l'argument  --date YYYY-MM-DD  (date d'exécution du DAG)
  - code de retour 0 = succès, autre valeur = échec

Mode strict : si la variable d'environnement LEARNSIGHT_STRICT=1,
un script manquant fait échouer l'étape (à activer pour l'intégration finale).
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

# Adresse du master Spark vue depuis le conteneur Airflow
SPARK_MASTER = os.getenv("SPARK_MASTER_URL_FOR_AIRFLOW", "spark://spark-master:7077")
STRICT = os.getenv("LEARNSIGHT_STRICT", "0")

# Dossiers montés dans le conteneur Airflow (voir docker-compose.yml)
LAKEHOUSE_DIR = "/opt/airflow/lakehouse"  # ./lakehouse  (Étudiant 2)
SPARK_DIR = "/opt/airflow/jobs"           # ./spark      (Étudiant 3)


def notify_failure(context):
    """Appelée par Airflow quand une étape échoue définitivement (après les retries)."""
    ti = context["task_instance"]
    log.error(
        "ECHEC pipeline LearnSight | dag=%s | task=%s | date=%s | logs=%s",
        ti.dag_id,
        ti.task_id,
        context.get("ds"),
        ti.log_url,
    )


def spark_job(task_id: str, script_path: str, owner: str) -> BashOperator:
    """Crée une étape qui lance un script Spark, ou un placeholder s'il n'existe pas encore."""
    command = f"""
set -e
SCRIPT="{script_path}"
if [ -f "$SCRIPT" ]; then
  echo "Lancement de $SCRIPT pour la date {{{{ ds }}}}"
  spark-submit --master {SPARK_MASTER} "$SCRIPT" --date {{{{ ds }}}}
else
  echo "[PLACEHOLDER] $SCRIPT n'existe pas encore ({owner})"
  if [ "{STRICT}" = "1" ]; then
    echo "Mode strict actif : échec"
    exit 1
  fi
fi
"""
    return BashOperator(task_id=task_id, bash_command=command)


default_args = {
    "owner": "learnsight",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="learnsight_weekly",
    description="Pipeline hebdomadaire : Silver -> Gold -> features -> prédictions -> Gold",
    start_date=datetime(2026, 11, 1),
    schedule="@weekly",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["learnsight", "weekly"],
) as dag:

    start = EmptyOperator(task_id="start")

    check_data = BashOperator(
        task_id="check_data",
        bash_command=(
            'echo "[PLACEHOLDER] Vérifier que les données Bronze existent dans MinIO '
            '(à brancher avec l\'Étudiant 2)"'
        ),
    )

    # --- Étudiant 2 : Lakehouse ---
    bronze_to_silver = spark_job(
        "bronze_to_silver", f"{LAKEHOUSE_DIR}/silver/bronze_to_silver.py", "Étudiant 2"
    )
    validate_silver = spark_job(
        "validate_silver", f"{LAKEHOUSE_DIR}/silver/validate_silver.py", "Étudiant 2"
    )
    silver_to_gold = spark_job(
        "silver_to_gold", f"{LAKEHOUSE_DIR}/gold/silver_to_gold.py", "Étudiant 2"
    )

    # --- Étudiant 3 : Spark & ML ---
    compute_features = spark_job(
        "compute_features", f"{SPARK_DIR}/features/compute_features.py", "Étudiant 3"
    )
    run_predictions = spark_job(
        "run_predictions", f"{SPARK_DIR}/ml/run_predictions.py", "Étudiant 3"
    )
    publish_gold = spark_job(
        "publish_gold", f"{SPARK_DIR}/ml/publish_predictions.py", "Étudiant 3"
    )

    # --- Étudiant 2 : contrôles finaux sur Gold ---
    quality_checks = spark_job(
        "quality_checks", f"{LAKEHOUSE_DIR}/gold/quality_checks.py", "Étudiant 2"
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> check_data
        >> bronze_to_silver
        >> validate_silver
        >> silver_to_gold
        >> compute_features
        >> run_predictions
        >> publish_gold
        >> quality_checks
        >> end
    )
