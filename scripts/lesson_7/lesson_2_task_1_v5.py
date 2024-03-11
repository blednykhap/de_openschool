from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import csv
from pathlib import Path


with DAG(dag_id="lesson_2_task_1_v5",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         tags=['user_id', 'lesson_2']) as dag:
    @task
    def get_regions_to_aggregate(**kwargs):
        regions_to_aggretate = []
        p = Path('/opt/airflow/data/regions_random/')

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path(f"{'/opt/airflow/data/regions_random/'}").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(f"filepath={filepath}")
            print(f"is_file={filepath.is_file()}")
            if filepath.is_file():
                regions_to_aggretate.append(r)
                print("Appended_data")

        regions_list = [str(p).split("/")[-1] for p in regions_to_aggretate]
        print(f'regions_list: {regions_list}')

        return regions_list


    @task
    def aggregate_daily_data(region, **kwargs):
        print(f"region = {region}")
        filename = f"{'/opt/airflow/data/regions_random/'}/{region}/{kwargs['ds']}.csv"
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

        filename_agg = f"{'/opt/airflow/data/user_id/regions_random/'}/{region}/{kwargs['ds']}.csv"
        Path(filename_agg).parent.mkdir(parents=True, exist_ok=True)
        with open(filename_agg, "w") as file:
            file.write("user_id, count\n")
            for user_id, val_count in user_count.items():
                file.write(f"{user_id}, {val_count}\n")

        print(f"Write file {filename_agg}")
        return filename_agg


    regions_list = get_regions_to_aggregate()
    write_values = aggregate_daily_data.expand(region=regions_list)
