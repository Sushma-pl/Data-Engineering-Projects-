from pyspark.sql import SparkSession
from utils.config import STORAGE_MODE,MINIO_ACCESS_KEY,MINIO_ENDPOINT,MINIO_SECRET_KEY

def get_spark_session(app_name: str)->SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
    )

    if STORAGE_MODE == "minio":
        builder = (builder
            .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem"))

    return builder.getOrCreate()