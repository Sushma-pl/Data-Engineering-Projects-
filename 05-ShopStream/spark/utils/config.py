import os


# Storage mode — "local" or "minio"
# Set via environment variable so you can switch without touching code
STORAGE_MODE = os.getenv("STORAGE_MODE", "local")

# --- Paths ---
if STORAGE_MODE == "minio":
    BRONZE_PATH     = "s3a://lake/bronze/orders"
    SILVER_PATH     = "s3a://lake/silver/orders"
    GOLD_PATH       = "s3a://lake/gold/daily_revenue"
    CHECKPOINT_BASE = "s3a://lake/checkpoints"
else:
    BRONZE_PATH     = "/opt/spark-data/lake/bronze/orders"
    SILVER_PATH     = "/opt/spark-data/lake/silver/orders"
    GOLD_PATH       = "/opt/spark-data/lake/gold/daily_revenue"
    CHECKPOINT_BASE = "/opt/spark-data/checkpoints"


# --- MinIO / S3 credentials ---
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "minioadmin")

# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")