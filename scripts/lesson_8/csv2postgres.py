import sys
import fire
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pathlib import Path


def csv2postgres(source_csv_file, database, target_table, username, password, region, cur_date) -> None:
    spark = SparkSession.builder \
        .appName('csv2postgres') \
        .getOrCreate()

    print('!!! *** !!!')

    source = spark.read.option("InferSchema", True) \
        .csv(source_csv_file, sep=',', header=True)

    source.printSchema()
    source.show(5)

    result = source \
        .groupBy("user_id") \
        .agg(f.count("*").alias("count")) \
        .withColumn('date', f.to_date(f.lit(cur_date))) \
        .withColumn('region', f.lit(region))

    result.printSchema()
    result.show(5)

    result.select(f.avg("call_time")) \
        .write \
        .format("jdbc") \
        .option("url", f"jdbc:postgres-data:5433/{database}") \
        .option("dbtable", target_table) \
        .option("user", username) \
        .option("password", password) \
        .save()

if __name__ == '__main__':
    fire.Fire(csv2postgres)