from pyspark.sql import functions as F
from pyspark.sql import Window
from delta.tables import DeltaTable
from utils.schemas import parse_changes
from utils.config import BRONZE_PATH, SILVER_PATH, CHECKPOINT_BASE
from utils.spark_session import get_spark_session

# ----------------------------------------------------------
# SILVER STREAM
# Reads Bronze Delta, parses Debezium envelope, applies
# MERGE into Silver. One row per order = current state.
# ----------------------------------------------------------

CHECKPOINT = f"{CHECKPOINT_BASE}/silver_orders"

spark = get_spark_session("silver_orders")
spark.sparkContext.setLogLevel("WARN")


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
