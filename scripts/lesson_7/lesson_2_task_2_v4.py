from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime
from pathlib import Path

connection = 'postgres_data_blednyh'
source_dir = '/opt/airflow/data/user_id/regions_random/'

with DAG(dag_id="lesson_2_task_2_v5",
         start_date=datetime(2024, 1, 1),
         schedule="@daily",
         max_active_runs=2,
         concurrency=2,
         tags=['lesson_2', 'user_id']
         ) as dag:
    @task
    def get_regions_for_write(**kwargs):
        regions_to_write = []
        p = Path(source_dir)

        regions = [f for f in p.iterdir() if f.is_dir()]
        print(f"regions={regions}")
        for r in regions:
            filepath = Path(f"{source_dir}").joinpath(r).joinpath(f"{kwargs['ds']}.csv")
            print(f"filepath={filepath}")
            print(f"is_file={filepath.is_file()}")
            if filepath.is_file():
                regions_to_write.append(r)
                print("Appended")

        regions_list = [str(p).split("/")[-1] for p in regions_to_write]
        print(f'regions_list: {regions_list}')

        return regions_list


    @task
    def create_table(**kwargs):
        hook = PostgresHook(postgres_conn_id=connection)
        try:
            conn = hook.get_conn()
            cur = conn.cursor()
        except Exception as e:
            print("Connection failed")
            print(e.message, e.args)

        create_table_sql = """
           CREATE TABLE IF NOT EXISTS users_aggregations (
           date DATE,
           region VARCHAR,
           user_id INT,
           count INT
           );
           """
        cur.execute(create_table_sql)
        conn.commit()
        cur.close()
        print("Table created")


    @task
    def write_data_to_postgres(region, **kwargs):
        print(f"region = {region}")
        filename = f"{source_dir}/{region}/{kwargs['ds']}.csv"
        print(f"filename = {filename}")

        hook = PostgresHook(postgres_conn_id=connection)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(f"DELETE FROM users_aggregations WHERE date='{kwargs['ds']}' AND region='{region}'")

        copy_sql = f"""
            CREATE TEMPORARY TABLE buffer (
                user_id INT,
                count INT
            );

            COPY buffer FROM STDIN WITH CSV HEADER DELIMITER as ',';

            INSERT INTO users_aggregations (date, region, user_id, count)
            SELECT '{kwargs['ds']}', '{region}', user_id, count FROM buffer;

            DROP TABLE buffer;
            """

        with open(filename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            cur.close()
            print("Data writed")


    regions_list = get_regions_for_write()
    create_table()
    write_data_to_postgres.expand(region=regions_list)
