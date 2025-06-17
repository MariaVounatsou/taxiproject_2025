from pyspark import SparkContext
from datetime import datetime

sc = SparkContext.getOrCreate()

# Διαβάζουμε το CSV από το HDFS (προσαρμοσε το path αν χρειάζεται)
lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

header = lines.first()
data = lines.filter(lambda row: row != header)

def parse_line(line):
    try:
        parts = line.split(",")
        # Παίρνουμε pickup_datetime (στήλη 1), latitude (στήλη 6), longitude (στήλη 5)
        pickup_time = datetime.strptime(parts[1], '%Y-%m-%d %H:%M:%S')
        hour = pickup_time.hour
        lat = float(parts[6])
        lon = float(parts[5])
        # Φιλτράρουμε συντεταγμένες με 0.0
        if lat != 0.0 and lon != 0.0:
            return (hour, (lat, lon, 1))  # (ώρα, (sum_lat, sum_lon, count))
        else:
            return None
    except:
        return None

parsed = data.map(parse_line).filter(lambda x: x is not None)

# ReduceByKey: άθροιση lat, lon, count ανά ώρα
aggregated = parsed.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))

# Υπολογισμός μέσου όρου για κάθε ώρα
averages = aggregated.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))

# Ταξινόμηση κατά ώρα
result = averages.sortByKey()

for hour, (avg_lat, avg_lon) in result.collect():
    print(f"Hour: {hour:02d}  AvgLatitude: {avg_lat:.6f}  AvgLongitude: {avg_lon:.6f}")

