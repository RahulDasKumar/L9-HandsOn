from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()
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

def write_batch(batch_df, batch_id):
    if not batch_df.isEmpty():
        batch_df.toPandas().to_csv(f"outputs/task_1/batch_{batch_id}.csv", index=False, header=True)

query = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .start()

query.awaitTermination()
