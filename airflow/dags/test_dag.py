from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="learnsight_smoke_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    start = BashOperator(task_id="start", bash_command="echo 'start'")
    end = BashOperator(task_id="end", bash_command="echo 'Airflow khdam'")
    start >> end