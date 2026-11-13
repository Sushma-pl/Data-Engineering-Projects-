from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window


# ----------------------------------------------------------
# Debezium message schemas and parsing logic.
# Kept here so silver_stream.py and tests both import
# from one place — no duplication.
# ----------------------------------------------------------


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
