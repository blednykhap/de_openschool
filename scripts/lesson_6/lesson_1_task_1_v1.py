from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_kwargs(**kwargs):
    print("kwargs: ")
    for k, v in kwargs.items():
        print(f"key: {k}, value: {v}")


with DAG(dag_id="lesson_1_task_1_v1",
         start_date=datetime(2024, 2, 15),
         schedule="@once",
         tags=["lesson_1", "user_id"]) as dag:

    task = PythonOperator(
        task_id="print_kwargs",
        python_callable=print_kwargs,
        provide_context=True
    )

task
