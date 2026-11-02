from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------
# BRONZE STREAM
# Reads raw Debezium events from Kafka and lands them
# as-is into a Delta table. No parsing, no transformation.
# ----------------------------------------------------------


BRONZE_PATH = "/opt/spark-data/lake/bronze/orders"
CHECKPOINT = "/opt/spark-data/checkpoints/bronze_orders"

spark = (
    SparkSession.builder
    .appName("bronze_orders")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


# Read data from kafka 
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
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