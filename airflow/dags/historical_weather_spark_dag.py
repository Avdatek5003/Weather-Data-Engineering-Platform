
from datetime import datetime, timezone

from airflow.sdk import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import Param


with DAG(
    dag_id="historical_weather_spark_pipeline",
    description="Historical weather ETL: PostgreSQL -> Spark -> HDFS",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "test_fail_after_silver": Param(
            False,
            type="boolean",
            description="Inject a failure after Silver for testing",
        ),
    },
    tags=["weather", "spark", "historical", "hdfs"],
) as dag:

    run_historical_etl = SparkSubmitOperator(
        task_id="run_historical_weather_etl",
        jars="/opt/spark/jars/postgresql-42.7.13.jar",
        conn_id="spark_default",
        application="/opt/spark/work-dir/jobs/weather_pipeline/main.py",
        name="historical-weather-etl",
        deploy_mode="client",
        spark_binary="spark-submit",
        application_args=[
            "--mode", "full",
            "--start-date", "{{ dag_run.conf.get('start_date', '1940-01-01') }}",
            "--end-date", "{{ dag_run.conf.get('end_date', '1940-01-31') }}",
            "--fail-after-silver",
            "{{ '1' if params.test_fail_after_silver else '0' }}",
        ],
        conf={
            "spark.driver.host": "scheduler",
            "spark.driver.bindAddress": "0.0.0.0",

            # PostgreSQL JDBC driver classpath
            "spark.driver.extraClassPath": "/opt/spark/jars/postgresql-42.7.13.jar",
            "spark.executor.extraClassPath": "/opt/spark/jars/postgresql-42.7.13.jar",
        },
        env_vars={
            "PYTHONPATH": "/opt/spark/work-dir/jobs",
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python3.10",
        },
        executor_cores=2,
        executor_memory="1g",
        driver_memory="1g",
        verbose=False,
        retries=0,
    )
