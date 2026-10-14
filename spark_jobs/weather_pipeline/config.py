# spark_jobs/weather_pipeline/config.py

import os
from dataclasses import dataclass
from datetime import date
from typing import Optional


@dataclass(frozen=True)
class PipelineConfig:
    # Spark
    spark_master: str
    spark_app_name: str

    # PostgreSQL
    jdbc_url: str
    jdbc_user: str
    jdbc_password: str
    jdbc_driver: str

    # HDFS
    landing_path: str
    silver_path: str
    quarantine_path: str
    gold_monthly_path: str
    gold_yearly_path: str

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        password = os.getenv("WEATHER_DB_PASSWORD")

        if not password:
            raise ValueError(
                "WEATHER_DB_PASSWORD environment variable is required."
            )

        return cls(
            spark_master=os.getenv(
                "SPARK_MASTER_URL",
                "spark://spark-master:7077",
            ),
            spark_app_name=os.getenv(
                "SPARK_APP_NAME",
                "WeatherHistoricalBatch",
            ),

            jdbc_url=os.getenv(
                "WEATHER_JDBC_URL",
                "jdbc:postgresql://host.docker.internal:5432/Weather_Data_Pipeline",
            ),
            jdbc_user=os.getenv(
                "WEATHER_DB_USER",
                "postgres",
            ),
            jdbc_password=password,
            jdbc_driver="org.postgresql.Driver",

            landing_path=os.getenv(
                "HDFS_LANDING_PATH",
                "hdfs://namenode:8020/weather/landing/historical",
            ),
            silver_path=os.getenv(
                "HDFS_SILVER_PATH",
                "hdfs://namenode:8020/weather/silver/weather_clean",
            ),
            quarantine_path=os.getenv(
                "HDFS_QUARANTINE_PATH",
                "hdfs://namenode:8020/weather/quarantine/historical_weather",
            ),
            gold_monthly_path=os.getenv(
                "HDFS_GOLD_MONTHLY_PATH",
                "hdfs://namenode:8020/weather/gold/monthly_climate",
            ),
            gold_yearly_path=os.getenv(
                "HDFS_GOLD_YEARLY_PATH",
                "hdfs://namenode:8020/weather/gold/yearly_climate",
            ),
        )