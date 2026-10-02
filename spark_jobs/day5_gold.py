# %%
from pyspark.sql import SparkSession
# %%
spark = (
    SparkSession.builder
    .appName("WeatherPlatform-Day5-Gold")
    .master("local[*]")
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
historical_df.printSchema()

# %%
historical_df.show(5,truncate=False)

# %%
historical_df.count()
# %%
historical_df.rdd.getNumPartitions()
# %%
cities_df = (
    spark.read
    .jdbc(
        url=jdbc_url,
        table="public.cities",
        properties=jdbc_properties
    )
)
# %%
cities_df.printSchema()
# %%
cities_df.show()
# %%
cities_df.count()
# %%
from pyspark.sql.functions import col
# %%
joined_df = (
    historical_df.alias("weather")
    .join(
        cities_df.alias("cities"),
        col("weather.city_name") == col("cities.city_name"),
        "left"
    )
)
# %%
joined_df.show(5,truncate=False)
# %%
joined_df.printSchema()
# %%
joined_df.select(
    "weather.city_name",
    "cities.city_id",
    "cities.country",
    "cities.latitude",
    "cities.longitude"
).show(20, truncate=False)
# %%
joined_df.count()
# %%
historical_df.count()
# %%
joined_df.explain()
# %%
from pyspark.sql.functions import broadcast
# %%
broadcast_join_df=(
    historical_df.alias("weather")
        .join(broadcast(cities_df).alias("cities"),
        col("weather.city_name") == col("cities.city_name"),
        "left"
    )
)
# %%
broadcast_join_df.explain()
# %%
gold_base_df = (
    broadcast_join_df
    .select(
        col("cities.city_id").alias("city_id"),
        col("weather.date").alias("date"),
        col("weather.tavg").alias("tavg"),
        col("weather.tmin").alias("tmin"),
        col("weather.tmax").alias("tmax"),
        col("weather.prcp").alias("prcp"),
        col("weather.snow").alias("snow")
    )
)
# %%
gold_base_df.printSchema()
# %%
gold_base_df.show(10,truncate=False)
# %%
print("Historical:", historical_df.count())
print("Gold base:", gold_base_df.count())
# %%
from pyspark.sql.functions import (
    year,
    month,
    avg,
    min,
    max,
    sum,
    count,
    when,
    col
)
# %%
monthly_climate = (
    gold_base_df
    .withColumn("year", year("date"))
    .withColumn("month", month("date"))
    .groupBy("city_id", "year", "month")
    .agg(
        avg("tavg").alias("avg_tavg"),
        min("tmin").alias("min_tmin"),
        max("tmax").alias("max_tmax"),
        sum("prcp").alias("total_prcp"),
        sum(
            when(col("snow") > 0, 1)
            .otherwise(0)
        ).alias("snow_days"),
        count("*").alias("record_count")
    )
)
# %%
monthly_climate.show(20,truncate=False)
# %%
monthly_climate.explain()
# %%
monthly_climate.printSchema()
# %%
print("Monthly row count:", monthly_climate.count())
# %%
monthlymonthly_climate.orderBy("city_id","year","month")
# %%
monthly_path = (
    "hdfs://namenode:8020/weather/gold/monthly_climate"
)

monthly_climate.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(monthly_path)
# %%
yearly_climate = (
    gold_base_df
    .withColumn("year", year("date"))
    .groupBy("city_id", "year")
    .agg(
        avg("tavg").alias("avg_tavg"),
        min("tmin").alias("min_tmin"),
        max("tmax").alias("max_tmax"),
        sum("prcp").alias("total_prcp"),
        sum(
            when(col("snow") > 0, 1)
            .otherwise(0)
        ).alias("snow_days"),
        count("*").alias("record_count")
    )
)
# %%
yearly_climate.explain()
# %%
yearly_climate.printSchema()
# %%
yearly_climate.show(20,truncate=False)
# %%
yearly_climate.count()
# %%
yearly_climate.orderBy("city_id","year").show(20,truncate=False)
# %%
yearly_path = (
    "hdfs://namenode:8020/weather/gold/yearly_climate"
)

yearly_climate.write \
    .mode("overwrite") \
    .partitionBy("year") \
    .parquet(yearly_path)
# %%
# Eşleşmeyen kayıt kontrolü
print(
    "Eşleşmeyen şehir kayıtları:",
    gold_base_df.filter(col("city_id").isNull()).count()
)
# %%
# PostgreSql ile veri karşılaştırma ve kontrol
monthly_climate.filter(
    (
        (col("city_id") == 11) &
        (col("year") == 1995) &
        (col("month") == 1)
    ) |
    (
        (col("city_id") == 12) &
        (col("year") == 1973) &
        (col("month") == 6)
    ) |
    (
        (col("city_id") == 13) &
        (col("year") == 1963) &
        (col("month") == 3)
    )
).orderBy("city_id").show(truncate=False)
# %%
# Veri okuma ve kontrol 
monthly_check = spark.read.parquet(
    "hdfs://namenode:8020/weather/gold/monthly_climate"
)

yearly_check = spark.read.parquet(
    "hdfs://namenode:8020/weather/gold/yearly_climate"
)
# %%
print("Monthly Gold:", monthly_check.count())
print("Yearly Gold:", yearly_check.count())
# %%
monthly_check.printSchema()
yearly_check.printSchema()
# %%
