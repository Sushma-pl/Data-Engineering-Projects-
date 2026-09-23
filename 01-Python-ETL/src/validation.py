import pandas as pd
from pandas.api.types import is_string_dtype, is_datetime64_any_dtype
import logging

logging.basicConfig(
    filename="logs/validation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def validate_dataset(df: pd.DataFrame) -> bool:
    """
    Validate the orders dataset:
    - Schema check
    - Data type check
    - Missing values
    - Duplicate detection
    - Business rules
    """

    errors = []

    # 1. Expected schema (match actual dataset)
    expected_schema = {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "order_purchase_timestamp": "datetime64[ns]",
        "order_approved_at": "datetime64[ns]",
        "order_delivered_carrier_date": "datetime64[ns]",
        "order_delivered_customer_date": "datetime64[ns]",
        "order_estimated_delivery_date": "datetime64[ns]"
    }

    # 2. Schema validation
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            errors.append(f"Missing column: {col}")
        else:
            if expected_type == "string" and not is_string_dtype(df[col]):
                errors.append(f"{col} should be string")
            if expected_type == "datetime64[ns]" and not is_datetime64_any_dtype(df[col]):
                # Try conversion if not already datetime
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                except Exception:
                    errors.append(f"{col} could not be converted to datetime")

    # 3. Missing values in critical fields
    critical_cols = ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]
    for col in critical_cols:
        if df[col].isnull().any():
            errors.append(f"Missing values detected in {col}")

    # 4. Duplicate detection
    if df["order_id"].duplicated().any():
        errors.append("Duplicate order_id values found")

    # 5. Business rule validation
    valid_statuses = [
        "created", "approved", "invoiced", "processing",
        "shipped", "delivered", "canceled", "unavailable"
    ]
    invalid_status = df[~df["order_status"].isin(valid_statuses)]
    if not invalid_status.empty:
        errors.append(f"Invalid order_status values: {invalid_status['order_status'].unique()}")

    # Final result
    if errors:
        print("Validation failed with the following issues:")
        for e in errors:
            print(" -", e)
            logging.error(e)
        return False
    else:
        print("Validation passed. Dataset is clean.")
        logging.info("Validation passed. Dataset is clean.")
        return True
