from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum as _sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

spark = SparkSession.builder.appName("StreamingTraffic").getOrCreate()

kafka_bootstrap = "localhost:9092"
topic = "traffic-readings"

raw = spark.readStream.format("kafka")     .option("kafka.bootstrap.servers", kafka_bootstrap)     .option("subscribe", topic)     .option("startingOffsets", "latest")     .load()

schema = StructType([
    StructField("id", StringType()),
    StructField("timestamp", StringType()),
    StructField("city", StringType()),
    StructField("road_type", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed", DoubleType()),
    StructField("weather", StringType())
])

parsed = raw.select(from_json(col("value").cast("string"), schema).alias("data")).selectExpr(
    "data.id", "to_timestamp(data.timestamp) as ts", "data.city", "data.road_type", "data.vehicle_count", "data.avg_speed"
)

windowed = parsed.withWatermark("ts", "2 minutes").groupBy(
    window(col("ts"), "1 minute"), col("city")
).agg(
    _sum("vehicle_count").alias("vehicles"),
    avg("avg_speed").alias("avg_speed")
)

alerts = windowed.filter("vehicles > 200")     .selectExpr("window.start as window_start", "city", "vehicles", "avg_speed")

query_console = windowed.writeStream.outputMode("append").format("console").option("truncate", False).start()
query_alerts = alerts.writeStream.outputMode("append").format("console").option("truncate", False).start()

query_parquet = windowed.writeStream     .outputMode("append")     .format("parquet")     .option("path", "data/stream_output/")     .option("checkpointLocation", "data/checkpoints/streaming_app")     .start()

spark.streams.awaitAnyTermination()
