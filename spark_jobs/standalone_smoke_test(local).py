# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day1-Local-Smoke")
    .master("local[*]")
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
stop.session()
# %%
