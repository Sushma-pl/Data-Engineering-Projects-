from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .appName("check")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate())

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("/opt/spark-data/lake/bronze/orders")
print("Row count:", df.count())
df.printSchema()
df.show(3, truncate=80)
