from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col
import time

#Spark
spark = SparkSession.builder.appName("Query6 - Revenue by Borough").getOrCreate()

# Μέτρηση χρόνου εκτέλεσης
start_time = time.time()

#Διαβάζουμε τα δεδομένα ταξί του 2024 από Parquet
trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/yellow_tripdata_2024/")

#Διαβάζουμε το lookup αρχείο των ζωνών (CSV)
zones_df = spark.read.option("header", "true").csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

# Κάνουμε join για να συνδέσουμε το PULocationID με το Borough
joined_df = trips_df.join(
    zones_df.withColumnRenamed("LocationID", "PULocationID"),
    on="PULocationID"
)

# Group by Borough και άθροιση όλων των οικονομικών πεδίων
revenue_df = joined_df.groupBy("Borough").agg(
    _sum("fare_amount").alias("Fare"),
    _sum("tip_amount").alias("Tips"),
    _sum("tolls_amount").alias("Tolls"),
    _sum("extra").alias("Extras"),
    _sum("mta_tax").alias("MTA_Tax"),
    _sum("congestion_surcharge").alias("Congestion"),
    _sum("airport_fee").alias("Airport_Fee"),
    _sum("total_amount").alias("Total_Revenue")
)

# Ταξινόμηση κατά φθίνουσα σειρά εσόδων
sorted_df = revenue_df.orderBy(col("Total_Revenue").desc())

# Προβολή αποτελεσμάτων
sorted_df.show(truncate=False)

# Χρόνος εκτέλεσης
print(f"Execution time: {time.time() - start_time:.2f} seconds")

#Πλάνο εκτέλεσης (για Catalyst Optimizer)
print("\n	 Πλάνο εκτέλεσης	")
sorted_df.explain(True)

spark.stop()

