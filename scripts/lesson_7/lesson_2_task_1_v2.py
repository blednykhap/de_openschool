from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

table_regions_path = Variable.get("table_regions_path")
regions_list = Variable.get("region_list").split(' ')


with DAG(dag_id="lesson_2_task_1_v2",
         description="Aggregating user_id per date",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         tags=['lesson_2', 'user_id']
         ) as dag:

    @task
    def get_regions_to_aggregate(**kwargs):
        from pathlib import Path

        regions_to_aggretate = []
        p = Path(table_regions_path)

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path(table_regions_path).joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(filepath)
            print(filepath.is_file())
            if filepath.is_file():
                regions_to_aggretate.append(r)
                print("Appended")

        return [str(p).split("/")[-1] for p in regions_to_aggretate]


    @task
    def aggregate_daily_data(region, **kwargs):
        import csv
        from pathlib import Path

        print(f"region = {region}")
        filename = f"{table_regions_path}/{region}/{kwargs['ds']}.csv"


        print(f"filename = {filename}")
        user_count = {}
        with open(filename, newline='\n') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')

            for row in reader:
                if row[0] == 'user_id':
                    continue
                user_id = row[0]
                if user_id in user_count:
                    user_count[user_id] = user_count[user_id] + 1
                else:
                    user_count[user_id] = 1

        filename_agg = f"{ENDPOINT_DIR}/{region}/{kwargs['ds']}.csv"
        Path(filename_agg).parent.mkdir(parents=True, exist_ok=True)
        with open(filename_agg, "w") as file:
            file.write("user_id, count\n")
            for user_id, val_count in user_count.items():
                file.write(f"{user_id}, {val_count}\n")

        print(f"Write file {filename_agg}")
        return filename_agg


    regions_list = get_regions_to_aggregate()

    write_values = aggregate_daily_data.expand(region=regions_list)


