import duckdb
import logging
import datetime
from pathlib import Path
from src.config import SILVER_BUCKET_PATH , GOLD_BUCKET_PATH

logging.basicConfig(
    filename="logs/load.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_into_duckdb(db_file: str = None) -> None:
    """
    Load transformed Parquet data into DuckDB.
    """
    logging.info("Starting Load step...")

    # Use current date to locate silver file
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    silver_file = Path(SILVER_BUCKET_PATH) / f"olist_orders_clean_{current_date}.parquet"

    # Default DuckDB file inside gold folder
    if db_file is None:
        db_file = Path(GOLD_BUCKET_PATH) / "warehouse.duckdb"

    # Connect to DuckDB
    con = duckdb.connect(database=str(db_file))
    

    # Load parquet into DuckDB table
    con.execute(f"""
        CREATE OR REPLACE TABLE orders AS
        SELECT * FROM '{silver_file}'
    """)

    logging.info(f"Loaded data from {silver_file} into DuckDB table 'orders'")
    print(f"Load complete. Data available in DuckDB at {db_file}")

    # Example query
    result = con.execute("""
        SELECT order_status, COUNT(*) AS total_orders, AVG(delivery_time_days) AS avg_delivery
        FROM orders
        GROUP BY order_status
        ORDER BY total_orders DESC
    """).fetchdf()

    print(result)
    con.close()
