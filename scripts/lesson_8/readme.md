Написать даг, который будет считать с помощью spark считать агрегаты по регионам
и загружать в postgre через jdbc. 

Даг должен быть идемпотентным, то есть перезапуск определенной даты должен полностью
обновлять данные за соответствующи интервал.

Пример загрузки данных в postgre:

    df.write
        .format("jdbc")
        .option("driver","org.postgresql.Driver")
        .option("url", f"jdbc:postgresql://postgres-data:5432/{database}")
        .option("dbtable", target_table)
        .option("user", username)
        .option("password", password)
        .mode('append')
        .save()

Мы какие разрезы считаем в агрегатах, какие колонки должны писаться в базу?

так же, date, region, user_id, count



    base_config = {
        "task_id":"TEST",
        "conn_id":"test-conn",
        "application": "/home/test/test.py"
        "executor-memory":"10G",
        "driver-memory":"10G",
        "executor-cores":2,
        "principal":"test-host@test",
        "keytab":"/home/test-host.keytab",
        "env_vars":{"SPARK_MAJOR_VERSION":2}
        }
    
    spark_config = {
        "spark.master": "yarn",
        "spark.submit.deployMode": "client",
        "spark.yarn.queue":"test",
        "spark.dynamicAllocation.minExecutors":5,
        "spark.dynamicAllocation.maxExecutors":10, 
        "spark.yarn.driver.memoryOverhead":5120,
        "spark.driver.maxResultSize":"2G",
        "spark.yarn.executor.memoryOverhead":5120,
        "spark.kryoserializer.buffer.max":"1000m",
        "spark.executor.extraJavaOptions":"-XX:+UseG1GC",
        "spark.network.timeout":"15000s",
        "spark.executor.heartbeatInterval":"1500s",
        "spark.task.maxDirectResultSize":"8G",
        "spark.ui.view.acls":"*"
    }
    
    SparkSubmitOperator(**base_config,conf=spark_config)