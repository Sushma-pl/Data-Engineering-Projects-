import pytest
import json
import sys
import os
import psycopg2

# works both locally and inside container
sys.path.insert(0, "/opt/spark-apps")

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from utils.schemas import parse_changes
from utils.config import SILVER_PATH

# ----------------------------------------------------------
# Shared SparkSession for all tests
# ----------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .appName("test_pipeline")
        .master("local[1]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate())


# ----------------------------------------------------------
# Helper — builds a Bronze-style DataFrame from raw dicts
# ----------------------------------------------------------

def make_bronze_df(spark, events):
    rows = [{"value": json.dumps(e) if e is not None else None} for e in events]
    schema = T.StructType([T.StructField("value", T.StringType(), True)])
    return spark.createDataFrame(rows, schema=schema)


# ----------------------------------------------------------
# Unit tests for parse_changes()
# ----------------------------------------------------------

def test_insert_event(spark):
    """op=c should produce one row with correct fields"""
    event = {"payload": {
        "before": None,
        "after": {"id": 1, "customer_id": 2, "amount": "1500.00",
                  "status": "created", "created_at": 1000000, "updated_at": 1000000},
        "op": "c",
        "source": {"lsn": 100}
    }}
    df = make_bronze_df(spark, [event])
    result = parse_changes(df)

    assert result.count() == 1
    row = result.first()
    assert row["op"] == "c"
    assert row["id"] == 1
    assert float(row["amount"]) == 1500.00
    assert row["status"] == "created"
    assert row["lsn"] == 100


def test_delete_event(spark):
    """op=d should use before.id since after is null"""
    event = {"payload": {
        "before": {"id": 5, "customer_id": 1, "amount": "500.00",
                   "status": "shipped", "created_at": 1000000, "updated_at": 1000000},
        "after": None,
        "op": "d",
        "source": {"lsn": 200}
    }}
    df = make_bronze_df(spark, [event])
    result = parse_changes(df)

    assert result.count() == 1
    row = result.first()
    assert row["op"] == "d"
    assert row["id"] == 5


def test_tombstone_filtered_out(spark):
    """null value (tombstone) should be filtered out"""
    df = make_bronze_df(spark, [None])
    result = parse_changes(df)
    assert result.count() == 0


def test_update_event(spark):
    """op=u should capture after values"""
    event = {"payload": {
        "before": {"id": 3, "customer_id": 1, "amount": "800.00",
                   "status": "created", "created_at": 1000000, "updated_at": 1000000},
        "after":  {"id": 3, "customer_id": 1, "amount": "800.00",
                   "status": "shipped", "created_at": 1000000, "updated_at": 2000000},
        "op": "u",
        "source": {"lsn": 300}
    }}
    df = make_bronze_df(spark, [event])
    result = parse_changes(df)

    assert result.count() == 1
    row = result.first()
    assert row["op"] == "u"
    assert row["status"] == "shipped"
    assert row["lsn"] == 300


def test_mixed_batch(spark):
    """batch with insert, update, delete, tombstone — tombstone filtered, rest kept"""
    events = [
        {"payload": {"before": None,
                     "after": {"id": 1, "customer_id": 1, "amount": "100.00",
                               "status": "created", "created_at": 1000000, "updated_at": 1000000},
                     "op": "c", "source": {"lsn": 10}}},
        {"payload": {"before": {"id": 1, "customer_id": 1, "amount": "100.00",
                                "status": "created", "created_at": 1000000, "updated_at": 1000000},
                     "after":  {"id": 1, "customer_id": 1, "amount": "100.00",
                                "status": "paid", "created_at": 1000000, "updated_at": 2000000},
                     "op": "u", "source": {"lsn": 20}}},
        {"payload": {"before": {"id": 1, "customer_id": 1, "amount": "100.00",
                                "status": "paid", "created_at": 1000000, "updated_at": 2000000},
                     "after": None,
                     "op": "d", "source": {"lsn": 30}}},
        None,   # tombstone
    ]
    df = make_bronze_df(spark, events)
    result = parse_changes(df)
    assert result.count() == 3


# ----------------------------------------------------------
# Reconciliation test
# Pauses generator externally, waits for Silver to catch up,
# then asserts Silver count == Postgres count.
#
# HOW TO RUN:
#   1. Stop the generator (Ctrl+C)
#   2. Wait ~30 seconds for Silver to process remaining events
#   3. Run: pytest tests/test_pipeline.py::test_silver_matches_postgres -v
# ----------------------------------------------------------

@pytest.fixture(scope="session")
def pg_cursor():
    """Direct connection to source Postgres"""
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="shop",
        user="postgres",
        password="postgres"
    )
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()


def test_silver_matches_postgres(spark, pg_cursor):
    """Silver row count and total amount must match Postgres orders table"""
    # --- Postgres counts ---
    pg_cursor.execute("SELECT count(*), coalesce(sum(amount), 0) FROM orders")
    pg_count, pg_sum = pg_cursor.fetchone()

    # --- Silver counts ---
    from pyspark.sql import functions as F
    silver = spark.read.format("delta").load(SILVER_PATH)
    silver_row = silver.agg(F.count("*").alias("cnt"), F.sum("amount").alias("total")).first()
    silver_count = silver_row["cnt"]
    silver_sum   = float(silver_row["total"] or 0)

    print(f"\nPostgres: count={pg_count}, sum={pg_sum}")
    print(f"Silver:   count={silver_count}, sum={silver_sum}")

    assert pg_count == silver_count, (
        f"Row count mismatch: Postgres={pg_count}, Silver={silver_count}"
    )
    assert abs(float(pg_sum) - silver_sum) < 0.01, (
        f"Sum mismatch: Postgres={pg_sum}, Silver={silver_sum}"
    )