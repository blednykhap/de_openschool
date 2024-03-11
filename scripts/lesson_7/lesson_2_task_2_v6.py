from datetime import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from pathlib import Path

connection = 'postgres_data_user_id'
source_dir = '/opt/airflow/data/user_id/regions_random/'

with DAG(dag_id="lesson_2_task_2_v6",
         start_date=datetime(2024, 2, 1),
         schedule="@daily",
         max_active_runs=2,
         tags=["lesson_2", "user_id"]) as dag:

    create_table_op = PostgresOperator(
        task_id="load_data",
        sql="""
            CREATE TABLE IF NOT EXISTS call_data(            
            date DATE NOT NULL,
            user_id INTEGER NOT NULL,            
            call_region VARCHAR NOT NULL ,
            counts INTEGER NOT NULL)
        """,
        postgres_conn_id="postgres_data_user_id"
    )

    @task
    def get_region_list(**kwargs) -> list:
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

        regions_list = [str(p).split("/")[-1] for p in regions_to_write]
        print(f'regions_list: {regions_list}')

        return regions_list

    @task
    def write_data_to_db(region, **kwargs):

        filename = f"{source_dir}/{region}/{kwargs['ds']}.csv"
        hook = PostgresHook(postgres_conn_id=connection)
        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute(f"DELETE FROM call_data WHERE date='{kwargs['ds']}' AND call_region='{region}'")

        copy_sql = f"""
            CREATE TEMPORARY TABLE buffer (
                user_id INT,
                counts INT
            );

            COPY buffer FROM STDIN WITH CSV HEADER DELIMITER as ',';

            INSERT INTO call_data (date, call_region, user_id, counts)
            SELECT '{kwargs['ds']}', '{region}', user_id, counts FROM buffer;

            DROP TABLE buffer;
            """

        with open(filename, 'r') as f:
            cur.copy_expert(sql=copy_sql, file=f)
            conn.commit()
            cur.close()
            print("Data writed")

    region_list = get_region_list()
    write_data_to_db.expand(region=region_list)

    create_table_op >> get_region_list()
