from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics_Task2").getOrCreate()
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

agg_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare"),
    avg("distance_km").alias("avg_distance")
)

def write_batch(batch_df, batch_id):
    # Save the batch DataFrame as a CSV file with the batch ID in the filename
    if not batch_df.isEmpty():
        batch_df.toPandas().to_csv(f"outputs/task_2/batch_{batch_id}.csv", index=False, header=True)

query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
