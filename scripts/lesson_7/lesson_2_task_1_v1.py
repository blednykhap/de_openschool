from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

table_regions_path = Variable.get("table_regions_path")
regions_list = Variable.get("regions_list").split(' ')


def aggregate_data(_region, **kwargs):
    # data from 01.02.2024
    import pandas
    from pathlib import Path

    ds = kwargs['ds']
    path_2_get = Path(f"{table_regions_path}/{_region}/{ds}.csv")
    print(f'path_2_get={path_2_get}')
    path_2_save = f"/opt/airflow/data/user_id/regions_random/{_region}/{ds}.csv"
    print(f'path_2_save={path_2_save}')

    if path_2_get.is_file():
        initial_df = pandas.read_csv(path_2_get)
        aggregated_df = initial_df.groupby('user_id').count().reset_index()
        aggregated_df.to_csv(path_2_save)


with DAG(dag_id="lesson_2_task_1_v1",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         tags=["lesson_2", "user_id"]) as dag:

    dummy_start_op = DummyOperator(task_id="start")

    for region in regions_list:

        make_dir_op = BashOperator(
            task_id=f"make_dir_for_{region}",
            bash_command=f"mkdir -p /opt/airflow/data/user_id/regions_random/{region}"
        )

        aggregate_data_op = PythonOperator(
            task_id=f"aggregate_data_by_{region}",
            python_callable=aggregate_data,
            op_kwargs={'_region': region},
            provide_context=True,
            do_xcom_push=True
        )

    dummy_start_op >> make_dir_op >> aggregate_data_op
