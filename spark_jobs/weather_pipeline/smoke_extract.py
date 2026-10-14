from pyspark.sql import SparkSession

from weather_pipeline.config import PipelineConfig
from weather_pipeline.extract import (
    read_historical_weather,
    read_cities,
)


def main():
    config = PipelineConfig.from_env()

    spark = (
        SparkSession.builder
        .appName("WeatherPipeline-ExtractSmokeTest")
        .master(config.spark_master)
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    try:
        historical_df = read_historical_weather(spark, config)
        cities_df = read_cities(spark, config)

        print("=== HISTORICAL SCHEMA ===")
        historical_df.printSchema()

        print("=== CITIES SCHEMA ===")
        cities_df.printSchema()

        print("=== HISTORICAL SAMPLE ===")
        historical_df.select("city_name", "date", "tavg").show(5)

        print("=== CITIES SAMPLE ===")
        cities_df.select("city_id", "city_name").show(5)

        print("EXTRACT SMOKE TEST: PASSED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()