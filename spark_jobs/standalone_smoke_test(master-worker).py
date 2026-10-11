# %%
from pyspark.sql import SparkSession
# %%
spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day2-Standalone-Smoke")
    .master("spark://spark-master:7077")

    # Worker/Executor'ın Driver'a geri ulaşabilmesi için
    .config("spark.driver.host", "spark")
    .config("spark.driver.bindAddress", "0.0.0.0")

    # Kaynakları bilinçli sınırla
    .config("spark.executor.cores", "2")
    .config("spark.executor.memory", "1g")

    .getOrCreate()
)
# %%
print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)
print("Default parallelism:", spark.sparkContext.defaultParallelism)
# %%
from pyspark.sql.functions import col, avg, count
# %%
df = (
    spark.range(0, 1_000_000)
    .withColumn("group_id", col("id") % 10)
)
# %%
result = (
    df.groupBy("group_id")
      .agg(
          count("*").alias("row_count"),
          avg("id").alias("avg_id")
      )
      .orderBy("group_id")
)
# %%
result.show()
# %%
print("Input partitions:", df.rdd.getNumPartitions())
print("Result partitions:", result.rdd.getNumPartitions())
# %%
print("Master:", spark.sparkContext.master)
print("Application ID:", spark.sparkContext.applicationId)
print("Default parallelism:", spark.sparkContext.defaultParallelism)
print("Executor cores:", spark.conf.get("spark.executor.cores"))
# %%
from pyspark.sql.functions import col, sum as spark_sum
# %%
base_df = (
    spark.range(0, 1_000_000, numPartitions=2)
    .withColumn("group_id", col("id") % 10)
)
# %%
print("Base partitions:", base_df.rdd.getNumPartitions())
# %%
df_8 = base_df.repartition(8)
# %%
print("After repartition:", df_8.rdd.getNumPartitions())
# %%
result_8 = (
    df_8.groupBy("group_id")
    .agg(spark_sum("id").alias("total_id"))
    .orderBy("group_id")
)

# %%
result_8.show()
# %%
from time import perf_counter
from pyspark.sql.functions import sum as spark_sum
# %%
experiments = []

for n in [2, 8, 16]:
    test_df = base_df.repartition(n)

    start = perf_counter()

    total = (
        test_df
        .agg(spark_sum("id").alias("total_id"))
        .collect()[0]["total_id"]
    )

    elapsed = perf_counter() - start

    experiments.append({
        "partitions": n,
        "total_id": total,
        "duration_seconds": round(elapsed, 3)
    })

    print(
        f"Partitions={n} | "
        f"Total={total} | "
        f"Duration={elapsed:.3f}s"
    )
# %%
spark.stop()
# %%
