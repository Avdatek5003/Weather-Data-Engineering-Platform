import argparse
import logging
from datetime import date
from time import perf_counter
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from weather_pipeline.config import PipelineConfig
from weather_pipeline.extract import (
    read_historical_weather,
    read_cities,
)
from weather_pipeline.transform import (
    add_landing_metadata,
    normalize_historical_weather,
    attach_city_dimension,
    build_monthly_gold,
    build_yearly_gold,
)
from weather_pipeline.quality import (
    apply_quality_rules,
    split_valid_and_quarantine,
)
from weather_pipeline.load import write_parquet


logger = logging.getLogger("weather_pipeline")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Historical Weather Spark ETL"
    )

    parser.add_argument(
        "--mode",
        choices=["sample", "full"],
        default="sample",
        help="Sample or full historical processing",
    )

    parser.add_argument("--start-date")
    parser.add_argument("--end-date")

    args = parser.parse_args()

    if bool(args.start_date) != bool(args.end_date):
        parser.error(
            "--start-date and --end-date must be provided together"
        )

    if args.start_date:
        try:
            start = date.fromisoformat(args.start_date)
            end = date.fromisoformat(args.end_date)
        except ValueError:
            parser.error("Dates must be valid YYYY-MM-DD values")

        if start > end:
            parser.error("Start date cannot be after end date")

    return args


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    args = parse_args()
    config = PipelineConfig.from_env()
    run_id = str(uuid4())
    start_time = perf_counter()

    spark = (
        SparkSession.builder
        .appName(f"{config.spark_app_name}-{run_id[:8]}")
        .master(config.spark_master)
        .config("spark.driver.host", "spark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.executor.cores", "2")
        .config("spark.executor.memory", "1g")
        .getOrCreate()
    )

    # All writes remain isolated until production promotion.
    base_path = (
        "hdfs://namenode:8020/weather/lab/"
        f"final_sprint/runs/{run_id}"
    )

    paths = {
        "landing": f"{base_path}/landing",
        "silver": f"{base_path}/silver",
        "quarantine": f"{base_path}/quarantine",
        "monthly": f"{base_path}/gold/monthly",
        "yearly": f"{base_path}/gold/yearly",
    }

    try:
        logger.info("RUN_ID=%s MODE=%s", run_id, args.mode)

        # EXTRACT
        historical_df = read_historical_weather(spark, config)
        cities_df = read_cities(spark, config)

        if args.start_date:
            historical_df = historical_df.filter(
                F.to_date("date").between(
                    args.start_date,
                    args.end_date,
                )
            )

        if args.mode == "sample":
            historical_df = historical_df.limit(1000)

        # LANDING
        landing_df = add_landing_metadata(
            historical_df,
            run_id,
        )

        write_parquet(
            landing_df,
            paths["landing"],
            mode="errorifexists",
        )

        landing_read = spark.read.parquet(paths["landing"])
        landing_count = landing_read.count()

        if landing_count == 0:
            raise ValueError("No source records matched the request")

        logger.info("LANDING_ROWS=%d", landing_count)

        # TRANSFORM
        normalized_df = normalize_historical_weather(
            landing_read
        )

        joined_df = attach_city_dimension(
            normalized_df,
            cities_df,
        )

        checked_df = apply_quality_rules(joined_df)

        silver_df, quarantine_df = (
            split_valid_and_quarantine(checked_df)
        )

        # SILVER + QUARANTINE
        write_parquet(
            silver_df,
            paths["silver"],
            mode="errorifexists",
        )

        write_parquet(
            quarantine_df,
            paths["quarantine"],
            mode="errorifexists",
        )

        silver_read = spark.read.parquet(paths["silver"])

        silver_count = silver_read.count()
        quarantine_count = spark.read.parquet(
            paths["quarantine"]
        ).count()

        if landing_count != silver_count + quarantine_count:
            raise ValueError(
                "Reconciliation failed: "
                f"{landing_count} != "
                f"{silver_count} + {quarantine_count}"
            )

        if silver_count == 0:
            raise ValueError(
                "No valid Silver records remain for Gold"
            )

        logger.info(
            "SILVER_ROWS=%d QUARANTINE_ROWS=%d",
            silver_count,
            quarantine_count,
        )

        # GOLD
        monthly_df = build_monthly_gold(silver_read)
        yearly_df = build_yearly_gold(silver_read)

        write_parquet(
            monthly_df,
            paths["monthly"],
            mode="errorifexists",
        )

        write_parquet(
            yearly_df,
            paths["yearly"],
            mode="errorifexists",
        )

        monthly_count = spark.read.parquet(
            paths["monthly"]
        ).count()

        yearly_count = spark.read.parquet(
            paths["yearly"]
        ).count()

        assert monthly_count > 0
        assert yearly_count > 0

        elapsed = perf_counter() - start_time

        print("\n========== PIPELINE RESULT ==========")
        print("RUN_ID:", run_id)
        print("MODE:", args.mode)
        print("LANDING:", landing_count)
        print("SILVER:", silver_count)
        print("QUARANTINE:", quarantine_count)
        print("MONTHLY GOLD:", monthly_count)
        print("YEARLY GOLD:", yearly_count)
        print("OUTPUT BASE:", base_path)
        print("ELAPSED SECONDS:", round(elapsed, 2))
        print("=====================================")
        print("END-TO-END PIPELINE: PASSED")

    except Exception:
        logger.exception("PIPELINE_FAILED RUN_ID=%s", run_id)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()