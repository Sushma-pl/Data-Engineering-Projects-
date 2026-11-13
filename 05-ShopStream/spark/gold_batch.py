from pyspark.sql import functions as F
from utils.config import SILVER_PATH, GOLD_PATH
from utils.spark_session import get_spark_session

# ----------------------------------------------------------
# GOLD BATCH JOB
# Reads Silver Delta, computes daily revenue by status.
# Overwrites Gold on every run — always a fresh aggregate.
# Run this on a schedule (hourly, daily).
# ----------------------------------------------------------

spark = get_spark_session("gold_daily_revenue")
spark.sparkContext.setLogLevel("ERROR")

# --- Read current state of Silver ---
silver = (
    spark.read.format("delta").load(SILVER_PATH)
)

# --- Aggregate: daily revenue by status ---
gold =  (
    silver.withColumn("day", F.to_date(F.col("updated_at")))
    .groupBy("day", "status")
    .agg(
        F.count("*").alias("order_count"),
        F.round(F.sum("amount"), 2).alias("total_revenue")
    )
    .orderBy("day", "status")
)

# --- Overwrite Gold on every run ---
(
    gold.write.format("delta").mode("overwrite")
    .save(GOLD_PATH)
)


print("Gold written successfully.")

# --- Show the result ---
spark.read.format("delta").load(GOLD_PATH).show(truncate=False)