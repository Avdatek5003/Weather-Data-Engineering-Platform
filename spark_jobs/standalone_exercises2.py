#%%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day2-JDBC-Landing")
    .master("spark://spark-master:7077")
    .config("spark.driver.host", "spark")
    .config("spark.driver.bindAddress", "0.0.0.0")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "1g")
    .getOrCreate()
)
# %%
jdbc_url = (
    "jdbc:postgresql://host.docker.internal:5432/"
    "Weather_Data_Pipeline"
)
# %%
jdbc_properties = {
    "user": "postgres",
    "password": "Avdatek5003",
    "driver": "org.postgresql.Driver"
}
# %%
historical_df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="public.historical_weather",
        properties=jdbc_properties
    )
)
# %%
historical_df.show(20,truncate=False)
# %%
landing_path = "hdfs://namenode:8020/weather/landing/historical"
# %%
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime, timezone

run_id = datetime.now(timezone.utc).strftime(
    "historical_%Y%m%dT%H%M%SZ"
)
# %%
landing_df = (
    historical_df
    .withColumn("source", lit("postgresql_historical_weather"))
    .withColumn("ingestion_ts", current_timestamp())
    .withColumn("pipeline_run_id", lit(run_id))
)
# %%
landing_df.show(20,truncate=False)
# %%
(
    landing_df
    .write
    .mode("overwrite")
    .parquet(landing_path)
)
# %%
print("Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)
print("Spark UI URL:", spark.sparkContext.uiWebUrl)
# %%
from pyspark.sql import functions as F
from time import perf_counter

silver_path = "hdfs://namenode:8020/weather/silver/weather_clean"

silver_df = spark.read.parquet(silver_path)

print("Master:", spark.sparkContext.master)
print("Silver partitions:", silver_df.rdd.getNumPartitions())
print("Silver rows:", silver_df.count())
# %%
experiment_results = []

for n in [2, 8, 16]:

    test_df = silver_df.repartition(n)

    start = perf_counter()

    result = (
        test_df
        .agg(
            F.count("*").alias("row_count"),
            F.avg("tavg").alias("avg_temperature"),
            F.sum("prcp").alias("total_precipitation")
        )
        .collect()[0]
    )

    elapsed = perf_counter() - start

    experiment_results.append({
        "partitions": n,
        "rows": result["row_count"],
        "avg_temperature": result["avg_temperature"],
        "total_precipitation": result["total_precipitation"],
        "duration": round(elapsed, 3)
    })

    print(f"\n=== REPARTITION({n}) ===")
    print("Rows:", result["row_count"])
    print("Average temperature:", result["avg_temperature"])
    print("Total precipitation:", result["total_precipitation"])
    print("Duration:", round(elapsed, 3), "seconds")
# %%
silver_8 = silver_df.repartition(8)

print(
    "After repartition:",
    silver_8.rdd.getNumPartitions()
)
# %%
silver_coalesce_2 = silver_8.coalesce(2)

silver_repartition_2 = silver_8.repartition(2)

print(
    "Coalesce partitions:",
    silver_coalesce_2.rdd.getNumPartitions()
)

print(
    "Repartition partitions:",
    silver_repartition_2.rdd.getNumPartitions()
)
# %%
print("=== COALESCE PLAN ===")
silver_coalesce_2.explain("formatted")

print("=== REPARTITION PLAN ===")
silver_repartition_2.explain("formatted")
# %%
coalesce_path = (
    "hdfs://namenode:8020/weather/lab/day2/coalesce_2"
)

repartition_path = (
    "hdfs://namenode:8020/weather/lab/day2/repartition_2"
)
# %%
(
    silver_coalesce_2
    .write
    .mode("overwrite")
    .parquet(coalesce_path)
)

(
    silver_repartition_2
    .write
    .mode("overwrite")
    .parquet(repartition_path)
)
# %%
def count_parquet_files(hdfs_path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()

    path = jvm.org.apache.hadoop.fs.Path(hdfs_path)
    fs = path.getFileSystem(conf)

    files = fs.listStatus(path)

    return sum(
        1
        for f in files
        if f.isFile()
        and f.getPath().getName().endswith(".parquet")
    )


print(
    "Coalesce Parquet files:",
    count_parquet_files(coalesce_path)
)

print(
    "Repartition Parquet files:",
    count_parquet_files(repartition_path)
)
# %%
city_distribution = (
    silver_df
    .groupBy("city_name")
    .count()
    .orderBy(F.desc("count"))
)

city_distribution.show(40, truncate=False)
# %%
city_stats = city_distribution.agg(
    F.count("*").alias("city_count"),
    F.min("count").alias("min_records"),
    F.max("count").alias("max_records"),
    F.avg("count").alias("avg_records")
)

city_stats.show(truncate=False)
# %%
base8_path = "hdfs://namenode:8020/weather/lab/day2/base_8"

(
    silver_df
    .repartition(8)
    .write
    .mode("overwrite")
    .parquet(base8_path)
)
# %%
base8_df = spark.read.parquet(base8_path)

print("Base input partitions:", base8_df.rdd.getNumPartitions())

coalesce_df = base8_df.coalesce(2)
repartition_df = base8_df.repartition(2)

print("=== COALESCE ===")
coalesce_df.explain("formatted")

print("=== REPARTITION ===")
repartition_df.explain("formatted")
# %%
coalesce_path = "hdfs://namenode:8020/weather/lab/day2/coalesce_2"
repartition_path = "hdfs://namenode:8020/weather/lab/day2/repartition_2"

coalesce_df.write.mode("overwrite").parquet(coalesce_path)

repartition_df.write.mode("overwrite").parquet(repartition_path)
# %%
from pyspark.sql import functions as F

normal_df = silver_df.repartition(8)

normal_distribution = (
    normal_df
    .withColumn("partition_id", F.spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

normal_distribution.show(20)
# %%
city_df = silver_df.repartition(8, "city_name")

city_distribution = (
    city_df
    .withColumn("partition_id", F.spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

city_distribution.show(20)
# %%
city_partition_mapping = (
    city_df
    .select(
        "city_name",
        F.spark_partition_id().alias("partition_id")
    )
    .groupBy("partition_id", "city_name")
    .count()
    .orderBy("partition_id", F.desc("count"))
)

city_partition_mapping.show(40, truncate=False)
# %%
city_partition_summary = (
    city_partition_mapping
    .groupBy("partition_id")
    .agg(
        F.count("*").alias("city_count"),
        F.sum("count").alias("total_records")
    )
    .orderBy("partition_id")
)

city_partition_summary.show()
# %%
