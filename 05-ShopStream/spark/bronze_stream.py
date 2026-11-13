from pyspark.sql import functions as F
from utils.config import BRONZE_PATH, CHECKPOINT_BASE, KAFKA_BOOTSTRAP
from utils.spark_session import get_spark_session

# ----------------------------------------------------------
# BRONZE STREAM
# Reads raw Debezium events from Kafka and lands them
# as-is into a Delta table. No parsing, no transformation.
# ----------------------------------------------------------

CHECKPOINT = f"{CHECKPOINT_BASE}/bronze_orders"

spark = get_spark_session("bronze_orders")
spark.sparkContext.setLogLevel("WARN")


# Read data from kafka 
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", "shop.public.orders")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "true")
    .load()
)

# --- Cast bytes to strings, keep all Kafka metadata ---
bronze = raw.select(
    F.col("key").cast("string").alias("key"),
    F.col("value").cast("string").alias("value"),  # null for tombstones
    F.col("topic"),
    F.col("partition"),
    F.col("offset"),
    F.col("timestamp").alias("kafka_ts"),
    F.current_timestamp().alias("ingested_at")
)

# write to delta , append only
query = (
    bronze.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start(BRONZE_PATH)
)

print("Bronze stream started. Waiting for data...")
query.awaitTermination()