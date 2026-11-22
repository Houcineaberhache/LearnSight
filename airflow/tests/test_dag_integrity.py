"""
Tests d'intégrité des DAGs LearnSight (Étudiante 4).

Vérifie que :
  - tous les DAGs se chargent sans erreur d'import ;
  - le DAG hebdomadaire contient les bonnes étapes, dans le bon ordre ;
  - les retries sont configurés.

Lancement dans le conteneur Airflow :
  docker compose cp airflow/tests airflow-scheduler:/tmp/tests
  docker compose exec -e DAGS_DIR=/opt/airflow/dags airflow-scheduler \
      python /tmp/tests/test_dag_integrity.py
"""
import os
from pathlib import Path

from airflow.models import DagBag

DAGS_DIR = Path(os.getenv("DAGS_DIR", Path(__file__).resolve().parents[1] / "dags"))

EXPECTED_ORDER = [
    "start",
    "check_data",
    "bronze_to_silver",
    "validate_silver",
    "silver_to_gold",
    "compute_features",
    "run_predictions",
    "publish_gold",
    "quality_checks",
    "end",
]


def get_dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors():
    dagbag = get_dagbag()
    assert dagbag.import_errors == {}, f"Erreurs d'import : {dagbag.import_errors}"


def test_weekly_dag_structure():
    dag = get_dagbag().get_dag("learnsight_weekly")
    assert dag is not None, "Le DAG learnsight_weekly est introuvable"
    assert {t.task_id for t in dag.tasks} == set(EXPECTED_ORDER)
    for previous, following in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:]):
        assert following in dag.get_task(previous).downstream_task_ids, (
            f"{previous} doit être suivi de {following}"
        )


def test_weekly_dag_retries():
    dag = get_dagbag().get_dag("learnsight_weekly")
    assert dag.default_args["retries"] >= 1
    assert dag.catchup is False


if __name__ == "__main__":
    for test in (test_no_import_errors, test_weekly_dag_structure, test_weekly_dag_retries):
        test()
        print(f"OK  {test.__name__}")
    print("Tous les tests passent.")
