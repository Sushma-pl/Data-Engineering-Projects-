import sys
sys.path.insert(0, "/opt/spark-apps")

from pyspark.sql import functions as F
from utils.config import SILVER_PATH
from utils.spark_session import get_spark_session

spark = get_spark_session("check_silver")
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(SILVER_PATH)

print(f"\n=== Silver ({SILVER_PATH}) ===")

print("\n--- Row Count ---")
print(df.count())

print("\n--- Schema ---")
df.printSchema()

print("\n--- Sample Rows ---")
df.show(5, truncate=False)

print("\n--- Status Distribution ---")
df.groupBy("status").count().orderBy("count", ascending=False).show()

print("\n--- Total Revenue ---")
df.agg(F.round(F.sum("amount"), 2).alias("total_revenue")).show()
