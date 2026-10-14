from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def apply_quality_rules(df: DataFrame) -> DataFrame:
    """Attach all applicable data quality violation reasons."""

    rules = [
        ("UNMATCHED_CITY", F.col("city_id").isNull()),

        ("NULL_DATE", F.col("observation_date").isNull()),

        (
            "FUTURE_DATE",
            F.col("observation_date") > F.current_date()
        ),

        ("NEGATIVE_PRECIPITATION", F.col("prcp") < 0),

        ("NEGATIVE_SNOW", F.col("snow") < 0),

        (
            "INVALID_TEMP_RANGE",
            F.col("tmin") > F.col("tmax")
        ),

        (
            "INVALID_TAVG",
            F.col("tmin").isNotNull()
            & F.col("tavg").isNotNull()
            & F.col("tmax").isNotNull()
            & (
                (F.col("tavg") < F.col("tmin"))
                | (F.col("tavg") > F.col("tmax"))
            )
        ),
    ]

    reason_expressions = [
        F.when(condition, F.lit(name))
        for name, condition in rules
    ]

    return df.withColumn(
        "quality_reasons",
        F.filter(
            F.array(*reason_expressions),
            lambda reason: reason.isNotNull()
        )
    )


def split_valid_and_quarantine(
    df: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Split records into valid Silver and Quarantine datasets."""

    valid_df = (
        df.filter(F.size("quality_reasons") == 0)
        .drop("quality_reasons")
    )

    quarantine_df = df.filter(
        F.size("quality_reasons") > 0
    )

    return valid_df, quarantine_df