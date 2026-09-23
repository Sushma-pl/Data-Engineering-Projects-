from src.ingestion import ingest_orders_dataset
from src.validation import validate_dataset

# ingest order dataset 
orders_df= ingest_orders_dataset()

# validate dataset 
is_valid = validate_dataset(orders_df)

if is_valid:
    from src.transform import transform_orders_dataset
    transformed_df = transform_orders_dataset(orders_df)

    # Load into DuckDB
    from src.load import load_into_duckdb
    load_into_duckdb()
