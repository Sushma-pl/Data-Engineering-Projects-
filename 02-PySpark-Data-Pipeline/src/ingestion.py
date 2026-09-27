"""Bronze layer ingestion module.

Reads raw CSV files from a Unity Catalog volume and writes them as
Delta tables in the bronze schema using explicitly defined schemas
for type-safe ingestion.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from schemas import (
    cutomer_schema,
    geolocation_schema,
    order_items_schema,
    order_payment_schema,
    order_reviews_schema,
    order_schema,
    products_schema,
    sellers_schema,
    product_category_name_translation_schema,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

VOLUME_PATH = "/Volumes/olist_lakehouse/raw/olist_files"
TARGET_SCHEMA = "olist_lakehouse.bronze"

# Maps each source CSV file to its (bronze_table_name, StructType schema).
FILE_TABLE_MAP = {
    "olist_customers_dataset.csv": ("customers", cutomer_schema),
    "olist_geolocation_dataset.csv": ("geolocation", geolocation_schema),
    "olist_order_items_dataset.csv": ("order_items", order_items_schema),
    "olist_order_payments_dataset.csv": ("order_payments", order_payment_schema),
    "olist_order_reviews_dataset.csv": ("order_reviews", order_reviews_schema),
    "olist_orders_dataset.csv": ("orders", order_schema),
    "olist_products_dataset.csv": ("products", products_schema),
    "olist_sellers_dataset.csv": ("sellers", sellers_schema),
    "product_category_name_translation.csv": (
        "product_category_name_translation",
        product_category_name_translation_schema,
    ),
}


def get_spark() -> SparkSession:
    """Return the active SparkSession, creating one if needed."""
    return SparkSession.builder.getOrCreate()


def ingest_to_bronze(
    spark: SparkSession,
    file_path: str,
    schema: StructType,
    table_name: str,
    target_schema: str = TARGET_SCHEMA,
) -> int:
    """Ingest a single CSV file into a bronze Delta table.

    Args:
        spark: Active SparkSession.
        file_path: Full path to the source CSV file.
        schema: Explicit StructType schema for the CSV.
        table_name: Target table name (without schema prefix).
        target_schema: Fully qualified target schema (catalog.schema).

    Returns:
        Number of rows written.

    Raises:
        Exception: If reading or writing fails.
    """
    full_table = f"{target_schema}.{table_name}"
    logger.info("Ingesting %s -> %s", file_path, full_table)

    df = spark.read.csv(
        file_path,
        schema=schema,
        header=True,
        sep=",",
        multiLine=True,
    )

    row_count = df.count()

    (
        df.write.format("delta")
        .mode("overwrite")
        .saveAsTable(full_table)
    )

    logger.info("Completed %s: %d rows", full_table, row_count)
    return row_count


def run_bronze_ingestion(
    spark: Optional[SparkSession] = None,
    volume_path: str = VOLUME_PATH,
    target_schema: str = TARGET_SCHEMA,
) -> dict:
    """Run bronze ingestion for all configured source files.

    Args:
        spark: Active SparkSession. If None, one is created.
        volume_path: Base volume path for source files.
        target_schema: Target catalog.schema for bronze tables.

    Returns:
        Dict mapping table names to row counts.

    Raises:
        RuntimeError: If any file fails to ingest.
    """
    if spark is None:
        spark = get_spark()

    results = {}
    failed = []

    for file_name, (table_name, schema) in FILE_TABLE_MAP.items():
        file_path = f"{volume_path}/{file_name}"
        try:
            count = ingest_to_bronze(
                spark, file_path, schema, table_name, target_schema
            )
            results[table_name] = count
        except Exception as e:
            logger.error("Failed to ingest %s: %s", file_name, e)
            failed.append(file_name)

    if failed:
        raise RuntimeError(f"Ingestion failed for: {', '.join(failed)}")

    logger.info("All %d tables ingested successfully", len(results))
    return results


if __name__ == "__main__":
    run_bronze_ingestion()
