import sys
sys.path.insert(0, "/opt/spark-apps")

from utils.config import BRONZE_PATH
from utils.spark_session import get_spark_session

spark = get_spark_session("check_bronze")
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load(BRONZE_PATH)

print(f"\n=== Bronze ({BRONZE_PATH}) ===")
print("Row count:", df.count())
df.printSchema()
df.show(3, truncate=80)
