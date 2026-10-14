from uuid import uuid4

from pyspark.sql import SparkSession

from weather_pipeline.config import PipelineConfig
from weather_pipeline.extract import read_historical_weather
from weather_pipeline.transform import add_landing_metadata
from weather_pipeline.load import write_parquet


def main():
    config = PipelineConfig.from_env()
    run_id = str(uuid4())

    spark = (
        SparkSession.builder
        .appName("WeatherPipeline-LandingSmokeTest")
        .master(config.spark_master)
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    try:
        historical_df = read_historical_weather(
            spark,
            config,
        )

        # Only a small sample for this checkpoint.
        sample_df = historical_df.limit(100)

        landing_df = add_landing_metadata(
            sample_df,
            run_id=run_id,
        )

        test_path = (
            "hdfs://namenode:8020"
            f"/weather/lab/final_sprint/landing/{run_id}"
        )

        write_parquet(
            df=landing_df,
            path=test_path,
            mode="errorifexists",
        )

        verified_df = spark.read.parquet(test_path)

        count = verified_df.count()

        print("RUN ID:", run_id)
        print("LANDING PATH:", test_path)
        print("LANDING ROWS:", count)

        verified_df.select(
            "city_name",
            "date",
            "source",
            "ingestion_ts",
            "pipeline_run_id",
        ).show(5, truncate=False)

        assert count == 100

        print("LANDING SMOKE TEST: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()