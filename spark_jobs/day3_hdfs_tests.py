# %%
from pyspark.sql import SparkSession

# %%
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day3-Ankara")
    .master("local[*]")
    .getOrCreate()
)
# %%
# %%
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "hdfs://namenode:8020/weather/bronze/historical/ankara_weather.csv"
    )
)

# %%
df.printSchema()
# %%
df.show(10,truncate=False)

# %%
print("Columns:", df.columns)
# %%
# %%
print(df.rdd.getNumPartitions())
# %%
# %%
hot_days = df.filter(df.tmax > 30)
# %%
hot_days.show(10)

# %%
from pyspark.sql.functions import avg, max, min, sum

city_summary = (
    df.groupBy("city_name")
      .agg(
          avg("tavg").alias("avg_temperature"),
          max("tmax").alias("max_temperature"),
          min("tmin").alias("min_temperature"),
          sum("prcp").alias("total_precipitation")
      )
)
# %%
city_summary.show(10,truncate=False)
# %%
# %%
city_summary.explain()
# %%
# %%
(
    city_summary.write
    .mode("overwrite")
    .parquet(
        "hdfs://namenode:8020/weather/silver/ankara_summary"
    )
)
# %%
