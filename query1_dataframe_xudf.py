from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, avg

spark = SparkSession.builder.appName("Query1_NoUDF_Parquet").getOrCreate()

df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/yellow_tripdata_2015")

filtered = df.filter((col("pickup_latitude") != 0) & (col("pickup_longitude") != 0))

result = (
    filtered
    .withColumn("hour", hour("tpep_pickup_datetime"))
    .groupBy("hour")
    .agg(
        avg("pickup_latitude").alias("avg_latitude"),
        avg("pickup_longitude").alias("avg_longitude")
    )
    .orderBy("hour")
)

result.show(24, truncate=False)

