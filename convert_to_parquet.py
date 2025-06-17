from pyspark.sql import SparkSession

# Δημιουργία SparkSession με HDFS namenode hostname
spark = SparkSession.builder \
    .appName("Convert CSV to Parquet") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-namenode.default.svc.cluster.local:9000") \
    .getOrCreate()

# paths με το σωστό πλήρες όνομα
input_paths = {
    "yellow_tripdata_2015": "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2015.csv",
    "yellow_tripdata_2024": "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/yellow_tripdata_2024.csv",
    "taxi_zone_lookup": "hdfs://hdfs-namenode.default.svc.cluster.local:9000/data/taxi_zone_lookup.csv"
}

output_base = "hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/mvounatsou/data/parquet/"

# Ανάγνωση και αποθήκευση κάθε αρχείου
for name, path in input_paths.items():
    print(f"Μετατροπή αρχείου {name}...")
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.write.mode("overwrite").parquet(f"{output_base}{name}")
    print(f" Ολοκληρώθηκε: {name}")

# Τερματισμός Spark
spark.stop()

