import random
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def generate_daily_data(**kwargs):
    time.sleep(3)
    filename = f"/opt/airflow/data/user_id/calls/{kwargs['ds']}.csv"
    rows_count = 100000
    regions = ['MSK', 'SPB', 'KHB', 'POV', 'SIB', 'SOU']
    regions_weighs = [50, 30, 10, 4, 3, 3]

    with open(filename, "w") as file:
        header = "user_id,call_region, call_time\n"
        file.write(header)

        for i in range(rows_count):
            file.write(
                f"{random.randint(1, 100)},{random.choices(regions, weights=regions_weighs)[0]}, {random.randrange(1000)}\n")


        kwargs["ti"].xcom_push(key="key", value="value")

        return filename


with DAG(dag_id="lesson_1_task_3_v1",
         start_date=datetime(2024, 2, 15),
         schedule="@once",
         max_active_runs=2,
       #  cоncurrency=2,
         tags=["lesson_1", "user_id"]) as dag:

    make_dir = BashOperator(
        task_id="make_dir",
        bash_command="mkdir -p /opt/airflow/data/user_id/calls"
    )

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=generate_daily_data,
        provide_context=True,
        do_xcom_push=True
    )

    get_file_size = BashOperator(
        task_id="get_file_size",
        bash_command="wc -c /opt/airflow/{{ ti.xcom_pull(task_ids='generate_data', key='return_value') }} | awk '{print $1}'",
        do_xcom_push=True
    )


    make_dir >> generate_data >> get_file_size