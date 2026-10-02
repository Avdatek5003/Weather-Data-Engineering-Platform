# %%
from pyspark.sql import SparkSession
# %%
spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day4")
    .master("local[*]")
    .getOrCreate()
)
# %%
bronze_path = "hdfs://namenode:8020/weather/bronze/historical/ankara_weather.csv"
# %%
df = (
    spark.read
    .option("header",True)
    .option("inferschema",True)
    .csv(bronze_path)
)
# %%
df.printSchema()
# %%
df.show(10,truncate=False)
# %%
print(df.columns)
# %%
print(df.rdd.getNumPartitions())
# %%
from pyspark.sql.functions import col, count, when, round, sum

# %%
df.select(
    count(when(col("tavg").isNull(), 1)).alias("tavg_null"),
    count(when(col("tmin").isNull(), 1)).alias("tmin_null"),
    count(when(col("tmax").isNull(), 1)).alias("tmax_null"),
    count(when(col("prcp").isNull(), 1)).alias("prcp_null"),
    count(when(col("wspd").isNull(), 1)).alias("wspd_null")
).show()
# %%
df.select("tavg").distinct().show(20, truncate=False)
# %%
amsterdam_path = "hdfs://namenode:8020/weather/bronze/historical/amsterdam_weather.csv"
# %%
df_amsterdam = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(amsterdam_path)
)

df_amsterdam.printSchema()
# %%
df_clean = df.withColumn(
    "temperature_range",
    round(col("tmax") - col("tmin"), 2)
)
# %%
df_clean.show(5)
# %%
df_clean=df_clean.withColumn(
    "weather_category",
    when(col("tmax")>=30 ,"Hot")
    .when(col("tmax")<5  ,"Cold")
    .otherwise("Normal")
)

# %%
df_clean.show(10,truncate=False)
# %%
null_counts = df_clean.select(
    *[
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df_clean.columns
    ]
)

null_counts.show()

# %%
silver_df = df_clean.select(
    "date",
    "city_name",
    "tmin",
    "tmax",
    "prcp",
    "snow",
    "wspd",
    "temperature_range",
    "weather_category"
)
# %%
silver_df.printSchema()
# %%
silver_df.show(10, truncate=False)
# %%
silver_path = "hdfs://namenode:8020/weather/silver/ankara_cleaned"
# %%
silver_df.write \
    .mode("overwrite") \
    .parquet(silver_path)
# %%
