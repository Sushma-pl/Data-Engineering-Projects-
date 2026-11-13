import pytest
import json
import sys
import os


# correct — works both locally and inside container
sys.path.insert(0, "/opt/spark-apps")


from pyspark.sql import SparkSession
from pyspark.sql import types as T
from utils.schemas import parse_changes


# ----------------------------------------------------------
# Shared SparkSession for all tests
# ----------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    return (SparkSession.builder
        .appName("test_pipeline")
        .master("local[1]")
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
    assert row["id"] == 5      # came from before.id, not after.id

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
    assert row["status"] == "shipped"   # after value
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

    assert result.count() == 3   # tombstone filtered out


