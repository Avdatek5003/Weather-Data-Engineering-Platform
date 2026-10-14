from pyspark.sql import DataFrame


def write_parquet(
    df: DataFrame,
    path: str,
    mode: str = "errorifexists",
) -> None:
    """
    Write a DataFrame to HDFS as Parquet.

    The caller is responsible for choosing a safe destination
    and an appropriate write mode.
    """

    allowed_modes = {
        "errorifexists",
        "overwrite",
        "append",
        "ignore",
    }

    if mode not in allowed_modes:
        raise ValueError(f"Unsupported write mode: {mode}")

    df.write.mode(mode).parquet(path)