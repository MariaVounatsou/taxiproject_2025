from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp, udf, row_number
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
import math
import time

# Συνάρτηση Haversine υπολογίζει γεωδαισιακή απόσταση μεταξύ δύο σημείων (σε km)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # ακτίνα της Γης σε km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2.0) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# Δημιουργία UDF για Haversine ώστε να μπορεί να εφαρμοστεί σε DataFrame
haversine_udf = udf(haversine, DoubleType())

# Δημιουργία SparkSession
spark = SparkSession.builder.appName("Query2").getOrCreate()


# Μέτρηση χρόνου εκτέλεσης
start = time.time()

# Ανάγνωση των δεδομένων από HDFS (CSV 2015)
df = spark.read.option("header", "true").csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

# Μετατροπή στηλών σε αριθμητικούς τύπους
df = df.withColumn("pickup_latitude", col("pickup_latitude").cast("float")) \
       .withColumn("pickup_longitude", col("pickup_longitude").cast("float")) \
       .withColumn("dropoff_latitude", col("dropoff_latitude").cast("float")) \
       .withColumn("dropoff_longitude", col("dropoff_longitude").cast("float")) \
       .withColumn("VendorID", col("VendorID").cast("int"))  # πάροχος ταξί

# Μετατροπή datetime σε timestamp τύπο
df = df.withColumn("pickup_time", to_timestamp("tpep_pickup_datetime")) \
       .withColumn("dropoff_time", to_timestamp("tpep_dropoff_datetime"))

# Υπολογισμός διάρκειας διαδρομής σε λεπτά
df = df.withColumn("duration_min",
    (unix_timestamp("dropoff_time") - unix_timestamp("pickup_time")) / 60
)

# Εφαρμογή της συνάρτησης Haversine για υπολογισμό απόστασης
df = df.withColumn("haversine_distance", haversine_udf(
    col("pickup_latitude"), col("pickup_longitude"),
    col("dropoff_latitude"), col("dropoff_longitude")
))

# Φιλτράρισμα άκυρων εγγραφών:
# Απόσταση να είναι ορισμένη
# Διάρκεια > 0
df = df.filter((col("haversine_distance").isNotNull()) & (col("duration_min") > 0))

# Ορισμός παραθύρου ταξινόμησης:
# Ανά VendorID
# Με σειρά φθίνουσα ως προς απόσταση
windowSpec = Window.partitionBy("VendorID").orderBy(col("haversine_distance").desc())

# Βαθμολόγηση (rank) για να κρατήσουμε μόνο τη μέγιστη απόσταση για κάθε VendorID
df_max = df.withColumn("rank", row_number().over(windowSpec)) \
           .filter(col("rank") == 1)

# Εμφάνιση τελικού αποτελέσματος
# VendorID: πάροχος ταξί
# haversine_distance: μέγιστη διανυθείσα απόσταση (σε km)
# duration_min: διάρκεια διαδρομής που αντιστοιχεί στην max απόσταση
df_max.select("VendorID", "haversine_distance", "duration_min").show()

# Εκτύπωση συνολικού χρόνου εκτέλεσης
end = time.time()
print(f"Execution time: {end - start:.2f} seconds")
