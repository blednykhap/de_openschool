from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from pathlib import Path
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pathlib import Path
from airflow.models import Variable
from airflow.decorators import dag, task

with DAG(dag_id="lesson_3_task_1_v1",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         catchup=True,
         dagrun_timeout=timedelta(minutes=60),
         tags=["lesson_3", "user_id"]) as dag:

    jars_path = 'jars/postgresql-42.3.1.jar'
    url = f"postgresql://postgres:5432/postgres"

    @task
    def get_regions_to_aggregate(**kwargs):
        regions_to_aggretate = []
        p = Path('data/regions_random')

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path("/opt/airflow/").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(filepath)
            print(filepath.is_file())
            if filepath.is_file():
                print("appended")
                regions_to_aggretate.append(r)

        return [str(p).split("/")[2] for p in regions_to_aggretate]


    table_regions_path_var = Variable.get("table_regions_path")

    regions_list = get_regions_to_aggregate(table_regions_path=table_regions_path_var)

    from airflow.hooks.base_hook import BaseHook

    connection = BaseHook.get_connection("postgres_data_user_id")
    conn_password = connection.password
    conn_login = connection.login
    conn_schema = connection.schema
    tablename = "spark_data_agg"

    @task
    def get_data_for_spark(table_regions_path:str, regions:list, **kwargs):
        return_list = []

        for r in regions:
            return_list.append([f'/opt/airflow/{table_regions_path}/{r}/{kwargs["ds"]}.csv',
                                conn_schema,
                                tablename,
                                conn_login,
                                conn_password,
                                r,
                                kwargs['ds']])
        return return_list



    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_data_user_id",
        sql=f"""create table IF NOT EXISTS {tablename} (
        date date, region varchar(10), user_id INT, count INT);
        """
    )

    csv_files = get_data_for_spark(table_regions_path_var, regions_list)
    create_table >> csv_files

    spark_agg_to_pg_task = SparkSubmitOperator.partial(
        task_id='spark_agg_to_pg_task',
        conn_id='spark_local',
        application=f'/opt/airflow/dags/students/user_id/spark_scripts/csv2postgres.py',
        name='csv2postgres',
        jars=jars_path,
        execution_timeout=timedelta(minutes=20)
    ).expand(application_args=csv_files)
