from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("Query5 - Most Frequent Zone Pairs") \
    .getOrCreate()

# Ξεκινάμε χρονόμετρο για μέτρηση χρόνου εκτέλεσης
start_time = time.time()

#Διαβάζουμε το dataset του 2024 από σωστό Parquet φάκελο
trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/yellow_tripdata_2024/")

#Διαβάζουμε το αρχείο με τις πληροφορίες των ζωνών (CSV)
zones_df = spark.read.option("header", "true").csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

#Κάνουμε join για να αντιστοιχίσουμε τα PULocationID σε PickupZone
trips_with_pickup = trips_df.join(
    zones_df.withColumnRenamed("LocationID", "PULocationID").withColumnRenamed("Zone", "PickupZone"),
    on="PULocationID"
)

#Κάνουμε join για να αντιστοιχίσουμε τα DOLocationID σε DropoffZone
trips_with_zones = trips_with_pickup.join(
    zones_df.withColumnRenamed("LocationID", "DOLocationID").withColumnRenamed("Zone", "DropoffZone"),
    on="DOLocationID"
)

# Φιλτράρουμε τις εγγραφές όπου PickupZone == DropoffZone
filtered = trips_with_zones.filter(col("PickupZone") != col("DropoffZone"))

#Ομαδοποιούμε με βάση PickupZone & DropoffZone και μετράμε διαδρομές
result = filtered.groupBy("PickupZone", "DropoffZone") \
                 .agg(count("*").alias("TotalTrips")) \
                 .orderBy(col("TotalTrips").desc())

#Εμφανίζουμε τα πρώτα 20 αποτελέσματα
result.show(20, truncate=False)

# Υπολογισμός και εμφάνιση χρόνου εκτέλεσης
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")

# Εμφανίζουμε το πλάνο εκτέλεσης για να δούμε τη στρατηγική join
print("\n	 Πλάνο Εκτέλεσης	")
result.explain(True)

spark.stop()

ort και την ανάλυση join στρατηγικής)
print("\n	 Πλάνο Εκτέλεσης	")
result.explain(True)

spark.stop()

