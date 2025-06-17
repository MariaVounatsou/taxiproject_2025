from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, year

# Δημιουργία SparkSession
spark = SparkSession.builder \
    .appName("Query4 - Νυχτερινές διαδρομές ανά Vendor") \
    .getOrCreate()

# Διαβάζουμε το αρχείο 2024 από Parquet
trips = spark.read.parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/data/parquet/yellow_tripdata_2024")

# Φιλτράρουμε μόνο εγγραφές του 2024
trips_2024 = trips.filter(year(col("tpep_pickup_datetime")) == 2024)

# Προσθέτουμε στήλη 'hour' και φιλτράρουμε τις νυχτερινές ώρες
night_trips = trips_2024.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
    .filter((col("hour") >= 23) | (col("hour") <= 6))

# Ομαδοποίηση ανά VendorID και μέτρηση διαδρομών
result = night_trips.groupBy("VendorID").count().withColumnRenamed("count", "NightTrips")

# Εμφάνιση αποτελεσμάτων
result.show(truncate=False)

# Αποθήκευση στο HDFS
result.write.mode("overwrite").parquet("hdfs://hdfs-namenode:9000/user/mvounatsou/results/query4")

# Τερματισμός
spark.stop()


