from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(dag_id="lesson_1_task_2_v2",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         tags=["lesson_1", "user_id"]) as dag:

    task = BashOperator(
        task_id="make_dir",
        bash_command="mkdir -p /opt/airflow/data/user_id"
    )

