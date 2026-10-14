from pyspark.sql import DataFrame, SparkSession

from weather_pipeline.config import PipelineConfig


def read_jdbc_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str,
) -> DataFrame:
    

    allowed_tables = {"historical_weather", "cities"}

    if table_name not in allowed_tables:
        raise ValueError(
            f"Unsupported JDBC table: {table_name}"
        )

    return (
        spark.read
        .format("jdbc")
        .option("url", config.jdbc_url)
        .option("dbtable", table_name)
        .option("user", config.jdbc_user)
        .option("password", config.jdbc_password)
        .option("driver", config.jdbc_driver)
        .load()
    )


def read_historical_weather(
    spark: SparkSession,
    config: PipelineConfig,
) -> DataFrame:
    return read_jdbc_table(
        spark=spark,
        config=config,
        table_name="historical_weather",
    )


def read_cities(
    spark: SparkSession,
    config: PipelineConfig,
) -> DataFrame:
    return read_jdbc_table(
        spark=spark,
        config=config,
        table_name="cities",
    )