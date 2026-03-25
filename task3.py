from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("RideSharingAnalytics_Task3").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")).select("data.*")

parsed_df = parsed_df \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .withWatermark("event_time", "1 minute")

windowed_df = parsed_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare")
)

result_df = windowed_df \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .drop("window")

def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.toPandas().to_csv(f"outputs/task_3/batch_{batch_id}.csv", index=False, header=True)

query = result_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
