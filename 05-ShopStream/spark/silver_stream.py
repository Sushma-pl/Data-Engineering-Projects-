from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from delta.tables import DeltaTable

# ----------------------------------------------------------
# SILVER STREAM
# Reads Bronze Delta, parses Debezium envelope, applies
# MERGE into Silver. One row per order = current state.
# ----------------------------------------------------------

BRONZE_PATH = "/opt/spark-data/lake/bronze/orders"
SILVER_PATH = "/opt/spark-data/lake/silver/orders"
CHECKPOINT  = "/opt/spark-data/checkpoints/silver_orders"


spark = (SparkSession.builder
    .appName("silver_orders")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------
# Schema — describes the Debezium JSON envelope
# Must match exactly what's in the Bronze value column
# 

# Shape of one order row inside before/after
ORDER_SCHEMA = T.StructType([
    T.StructField("id",          T.IntegerType()),
    T.StructField("customer_id", T.IntegerType()),
    T.StructField("amount",      T.StringType()),    # arrives as string
    T.StructField("status",      T.StringType()),
    T.StructField("created_at",  T.LongType()),      # microseconds
    T.StructField("updated_at",  T.LongType()),      # microseconds
])

# Shape of the source metadata block
SOURCE_SCHEMA = T.StructType([
    T.StructField("lsn", T.LongType()),
])


# Full Debezium payload envelope
PAYLOAD_SCHEMA = T.StructType([
    T.StructField("before",  ORDER_SCHEMA),
    T.StructField("after",   ORDER_SCHEMA),
    T.StructField("op",      T.StringType()),
    T.StructField("source",  SOURCE_SCHEMA),
])

# Outer wrapper — because schemas.enable was on
ENVELOPE_SCHEMA = T.StructType([
    T.StructField("payload", PAYLOAD_SCHEMA),
])

# ----------------------------------------------------------
# parse_changes()
# Takes a raw Bronze DataFrame, returns a clean DataFrame
# with one row per CDC event, proper types, ready for MERGE
# ----------------------------------------------------------

def parse_changes(df):
    # Step 1 — parse the JSON string into a struct
    parsed = df.withColumn("envelope", F.from_json(F.col("value"), ENVELOPE_SCHEMA))

    # Step 2 — extract payload from the envelope
    parsed = parsed.withColumn("payload", F.col("envelope.payload"))

    # Step 3 — filter out tombstone messages (value is null)
    parsed = parsed.filter(F.col("value").isNotNull())

    # Step 4 — extract all fields we need
    result = parsed.select(
        F.col("payload.op").alias("op"),
        F.col("payload.source.lsn").alias("lsn"),

        # For deletes, after is null — use before.id to know which row to delete
        F.coalesce(
            F.col("payload.after.id"),
            F.col("payload.before.id")
        ).alias("id"),

        F.col("payload.after.customer_id").alias("customer_id"),

        # Cast amount from string to decimal
        F.col("payload.after.amount").cast("decimal(10,2)").alias("amount"),

        F.col("payload.after.status").alias("status"),

        # Convert microseconds to readable timestamps
        F.expr("timestamp_micros(payload.after.created_at)").alias("created_at"),
        F.expr("timestamp_micros(payload.after.updated_at)").alias("updated_at"),
    )

    return result

# ----------------------------------------------------------
# upsert_orders()
# Called by foreachBatch for every micro-batch.
# Applies CDC events to the Silver Delta table.
# ----------------------------------------------------------

def upsert_orders(batch_df, batch_id):
    print(f"\n--- Batch {batch_id} ---")

    # Step 1 — parse raw bronze rows into clean change events
    changes = parse_changes(batch_df)

    # Step 2 — keep only the latest event per order ID in this batch
    # If order 5 was updated 3 times in one batch, we only want the last one
    # Otherwise MERGE throws "multiple source rows matched one target row"
    w = Window.partitionBy("id").orderBy(
        F.col("lsn").desc()
    )

    latest = (
        changes.withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    print(f"Changes in this batch: {latest.count()}")

    # Step 3 — MERGE into Silver
    target = DeltaTable.forPath(spark, SILVER_PATH)

    (
        target.alias("t")
        .merge(latest.alias("s"), "t.id = s.id")

        # DELETE — when op is d, remove the row from silver
        .whenMatchedDelete(condition="s.op='d'")

         # UPDATE — when op is not d AND incoming lsn is newer
        # lsn guard makes this idempotent — replaying same batch is safe
        .whenMatchedUpdate(
            condition="s.op != 'd' AND s.lsn > t.lsn",
            set={
                "customer_id": "s.customer_id",
                "amount":      "s.amount",
                "status":      "s.status",
                "updated_at":  "s.updated_at",
                "lsn":         "s.lsn"
            }
        )

        # INSERT — new row that doesn't exist in silver yet
        .whenNotMatchedInsert(
            condition="s.op != 'd'",
            values={
                "id":          "s.id",
                "customer_id": "s.customer_id",
                "amount":      "s.amount",
                "status":      "s.status",
                "created_at":  "s.created_at",
                "updated_at":  "s.updated_at",
                "lsn":         "s.lsn"
            })
        .execute())

    print(f"Batch {batch_id} complete.")


# ----------------------------------------------------------
# Create Silver table if it doesn't exist yet
# ----------------------------------------------------------
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{SILVER_PATH}` (
        id          INT,
        customer_id INT,
        amount      DECIMAL(10,2),
        status      STRING,
        created_at  TIMESTAMP,
        updated_at  TIMESTAMP,
        lsn         BIGINT
    ) USING DELTA
""")

# ----------------------------------------------------------
# Start the stream — reads from Bronze Delta, not Kafka
# ----------------------------------------------------------
query = (spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
    .writeStream
    .foreachBatch(upsert_orders)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start())

print("Silver stream started.")
query.awaitTermination()


# ----------------------------------------------------------
# Create Silver table if it doesn't exist yet
# Must exist before the stream starts so MERGE has a target
# ----------------------------------------------------------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS delta.`{SILVER_PATH}` (
        id          INT,
        customer_id INT,
        amount      DECIMAL(10,2),
        status      STRING,
        created_at  TIMESTAMP,
        updated_at  TIMESTAMP,
        lsn         BIGINT
    ) USING DELTA
""")

# ----------------------------------------------------------
# Start the stream — reads from Bronze Delta, not Kafka
# This means Silver can be rebuilt any time by wiping its
# checkpoint and table and replaying from Bronze
# ----------------------------------------------------------

query = (spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
    .writeStream
    .foreachBatch(upsert_orders)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="10 seconds")
    .start())

print("Silver stream started.")
query.awaitTermination()
