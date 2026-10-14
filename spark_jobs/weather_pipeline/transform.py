from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_landing_metadata(
    df: DataFrame,
    run_id: str,
) -> DataFrame:
    """
    Add ingestion metadata to historical weather records.
    """

    return (
        df
        .withColumn(
            "source",
            F.lit("postgresql.historical_weather"),
        )
        .withColumn(
            "ingestion_ts",
            F.current_timestamp(),
        )
        .withColumn(
            "pipeline_run_id",
            F.lit(run_id),
        )
    )


# spark_jobs/weather_pipeline/transform.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


NUMERIC_COLUMNS = [
    "tavg", "tmin", "tmax", "prcp", "snow",
    "wdir", "wspd", "wpgt", "pres", "tsun",
]


def normalize_historical_weather(df: DataFrame) -> DataFrame:
    """Normalize dates, city names and weather column types."""

    result = (
        df
        .withColumn("observation_date", F.to_date("date"))
        .withColumn("city_name", F.trim(F.col("city_name")))
    )

    for column in NUMERIC_COLUMNS:
        result = result.withColumn(
            column,
            F.col(column).cast("double"),
        )

    return result


def attach_city_dimension(
    weather_df: DataFrame,
    cities_df: DataFrame,
) -> DataFrame:
    """Attach city_id using a broadcast dimension join."""

    city_dimension = (
        cities_df
        .select("city_id", "city_name")
        .withColumn("city_name", F.trim(F.col("city_name")))
    )

    return weather_df.join(
        F.broadcast(city_dimension),
        on="city_name",
        how="left",
    )

def build_monthly_gold(silver_df: DataFrame) -> DataFrame:
    """Aggregate valid daily observations by city and month."""

    return (
        silver_df
        .withColumn("year", F.year("observation_date"))
        .withColumn("month", F.month("observation_date"))
        .groupBy("city_id", "city_name", "year", "month")
        .agg(
            F.count("*").alias("observation_count"),
            F.avg("tavg").alias("avg_temperature"),
            F.avg("tmin").alias("avg_min_temperature"),
            F.avg("tmax").alias("avg_max_temperature"),
            F.sum("prcp").alias("total_precipitation"),
            F.sum("snow").alias("total_snow"),
        )
    )


def build_yearly_gold(silver_df: DataFrame) -> DataFrame:
    """Aggregate valid daily observations by city and year."""

    return (
        silver_df
        .withColumn("year", F.year("observation_date"))
        .groupBy("city_id", "city_name", "year")
        .agg(
            F.count("*").alias("observation_count"),
            F.avg("tavg").alias("avg_temperature"),
            F.avg("tmin").alias("avg_min_temperature"),
            F.avg("tmax").alias("avg_max_temperature"),
            F.sum("prcp").alias("total_precipitation"),
            F.sum("snow").alias("total_snow"),
        )
    )