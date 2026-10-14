from uuid import uuid4

from pyspark.sql import SparkSession

from weather_pipeline.config import PipelineConfig
from weather_pipeline.transform import (
    build_monthly_gold,
    build_yearly_gold,
)
from weather_pipeline.load import write_parquet


def main():
    config = PipelineConfig.from_env()

    spark = (
        SparkSession.builder
        .appName("WeatherPipeline-GoldSmokeTest")
        .master(config.spark_master)
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    try:
        silver_df = spark.read.parquet(config.silver_path)

        monthly_df = build_monthly_gold(silver_df)
        yearly_df = build_yearly_gold(silver_df)

        run_id = str(uuid4())
        base_path = (
            "hdfs://namenode:8020/weather/lab/"
            f"final_sprint/gold/{run_id}"
        )

        monthly_path = f"{base_path}/monthly"
        yearly_path = f"{base_path}/yearly"

        write_parquet(monthly_df, monthly_path)
        write_parquet(yearly_df, yearly_path)

        monthly_check = spark.read.parquet(monthly_path)
        yearly_check = spark.read.parquet(yearly_path)

        monthly_rows = monthly_check.count()
        yearly_rows = yearly_check.count()

        print("MONTHLY GOLD ROWS:", monthly_rows)
        print("YEARLY GOLD ROWS:", yearly_rows)

        assert monthly_rows > 0
        assert yearly_rows > 0

        monthly_check.orderBy(
            "city_name", "year", "month"
        ).show(10, truncate=False)

        print("GOLD SMOKE TEST: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()