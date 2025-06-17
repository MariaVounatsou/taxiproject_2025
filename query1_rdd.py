from pyspark import SparkContext
from datetime import datetime

sc = SparkContext.getOrCreate()

def parse_line(line):
    try:
        parts = line.split(",")
        pickup_time = datetime.strptime(parts[1], '%Y-%m-%d %H:%M:%S')
        hour = pickup_time.hour
        lat = float(parts[6])
        lon = float(parts[5])
        if lat != 0.0 and lon != 0.0:
            return (hour, (lat, lon, 1))
    except:
        return None

lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")
header = lines.first()
data = lines.filter(lambda x: x != header)
parsed = data.map(parse_line).filter(lambda x: x is not None)

# Sum lat, lon, count
aggregated = parsed.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
averages = aggregated.mapValues(lambda x: (x[0]/x[2], x[1]/x[2]))
result = averages.sortByKey()

result.collect()

