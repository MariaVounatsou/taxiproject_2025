from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, avg, col
from pyspark.sql.types import IntegerType
from datetime import datetime

# Δημιουργία SparkSession
spark = SparkSession.builder.appName("Query1WithUDF").getOrCreate()

# UDF για εξαγωγή ώρας
@udf(IntegerType())
def extract_hour(dt_str):
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S').hour
    except:
        return None

# Φόρτωση CSV ως DataFrame
df = spark.read.csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv", header=True)

# Φιλτράρισμα και μετατροπές τύπων
filtered = df.filter((col("pickup_latitude") != "0") & (col("pickup_longitude") != "0"))

typed = (
    filtered
    .withColumn("hour", extract_hour("tpep_pickup_datetime").cast("int"))
    .withColumn("pickup_latitude", col("pickup_latitude").cast("double"))
    .withColumn("pickup_longitude", col("pickup_longitude").cast("double"))
)

# Ομαδοποίηση και υπολογισμός μέσων όρων
result = typed.groupBy("hour").agg(
    avg("pickup_latitude").alias("avg_latitude"),
    avg("pickup_longitude").alias("avg_longitude")
).orderBy("hour")

# Εμφάνιση αποτελεσμάτων
result.show(24, truncate=False)

