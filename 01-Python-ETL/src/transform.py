import pandas as pd
import datetime
import logging
from pathlib import Path
from src.config import SILVER_BUCKET_PATH

# Configure logging
logging.basicConfig(
    filename="logs/transform.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def transform_orders_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the validated orders dataset:
    - Normalize column names
    - Convert timestamps
    - Add derived features
    - Save to silver layer
    """

    logging.info("Starting transformation step...")
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    # 1. Normalize column names (snake_case)
    df = df.rename(columns={
        "order_approved_at": "order_approved_timestamp"
    })

    # 2. Ensure datetime conversion
    datetime_cols = [
        "order_purchase_timestamp",
        "order_approved_timestamp",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    # 3. Derived features
    # Delivery time in days
    df["delivery_time_days"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    # Approval lag in minutes
    df["approval_lag_minutes"] = (
        df["order_approved_timestamp"] - df["order_purchase_timestamp"]
    ).dt.total_seconds() / 60

    # Late delivery flag
    df["is_late"] = df["order_delivered_customer_date"] > df["order_estimated_delivery_date"]

    # ETL run timestamp
    df["etl_run_timestamp"] = datetime.datetime.now()

    # 4. Normalize categorical values
    df["order_status"] = df["order_status"].str.lower()

    # 5. Save to silver layer
    silver_file = Path(SILVER_BUCKET_PATH) / f"olist_orders_clean_{current_date}.parquet"
    df.to_parquet(silver_file, index=False)
    logging.info(f"Transformed dataset saved to {silver_file}")

    print(f"Transform complete. Saved to {silver_file}")
    return df


