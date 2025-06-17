from pyspark.sql import SparkSession

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("Query3 - Σύνολο ταξιδιών ανά δήμο") \
    .getOrCreate()

# Διαβάζουμε τα δεδομένα
trips = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/yellow_tripdata_2024")
zones = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/taxi_zone_lookup")

# Δημιουργία προσωρινών views
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

# Εκτέλεση ερωτήματος
result = spark.sql("""
SELECT
    z1.Borough AS Borough,
    COUNT(*) AS TotalTrips
FROM trips t
JOIN zones z1 ON t.PULocationID = z1.LocationID
JOIN zones z2 ON t.DOLocationID = z2.LocationID
WHERE z1.Borough = z2.Borough
GROUP BY z1.Borough
ORDER BY TotalTrips DESC
""")

#  Προσθήκη explain
result.explain(True)

# Εμφάνιση των αποτελεσμάτων σε μορφή πίνακα
print("Borough\tTotalTrips")
for row in result.collect():
    print(f"{row['Borough']}\t{row['TotalTrips']}")

# Αποθήκευση στο HDFS
result.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/results/query3")

# Τερματισμός
spark.stop()

