from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("check_silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/opt/spark-data/lake/silver/orders")

print("=== Silver Row Count ===")
print(df.count())

print("\n=== Schema ===")
df.printSchema()

print("\n=== Sample Rows ===")
df.show(5, truncate=False)

print("\n=== Status Distribution ===")
df.groupBy("status").count().orderBy("count", ascending=False).show()
