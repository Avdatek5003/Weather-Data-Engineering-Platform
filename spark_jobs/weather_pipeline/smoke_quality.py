from uuid import uuid4

from pyspark.sql import SparkSession

from weather_pipeline.config import PipelineConfig
from weather_pipeline.extract import read_cities
from weather_pipeline.transform import (
    normalize_historical_weather,
    attach_city_dimension,
)
from weather_pipeline.quality import (
    apply_quality_rules,
    split_valid_and_quarantine,
)
from weather_pipeline.load import write_parquet


def main():
    config = PipelineConfig.from_env()

    spark = (
        SparkSession.builder
        .appName("WeatherPipeline-QualitySmokeTest")
        .master(config.spark_master)
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    try:
        landing_path = (
            "hdfs://namenode:8020/weather/lab/final_sprint/"
            "landing/7e31536a-1e76-4324-b8cb-48b4f0d6ca51"
        )

        landing_df = spark.read.parquet(landing_path)
        cities_df = read_cities(spark, config)

        normalized_df = normalize_historical_weather(landing_df)

        joined_df = attach_city_dimension(
            normalized_df,
            cities_df,
        )

        quality_df = apply_quality_rules(joined_df)

        valid_df, quarantine_df = split_valid_and_quarantine(
            quality_df
        )

        run_id = str(uuid4())

        silver_path = (
            f"hdfs://namenode:8020/weather/lab/"
            f"final_sprint/silver/{run_id}"
        )

        quarantine_path = (
            f"hdfs://namenode:8020/weather/lab/"
            f"final_sprint/quarantine/{run_id}"
        )

        write_parquet(valid_df, silver_path)
        write_parquet(quarantine_df, quarantine_path)

        landing_rows = landing_df.count()
        silver_rows = spark.read.parquet(silver_path).count()
        quarantine_rows = spark.read.parquet(quarantine_path).count()

        print("LANDING:", landing_rows)
        print("SILVER:", silver_rows)
        print("QUARANTINE:", quarantine_rows)

        assert landing_rows == silver_rows + quarantine_rows

        print("QUALITY SMOKE TEST: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()