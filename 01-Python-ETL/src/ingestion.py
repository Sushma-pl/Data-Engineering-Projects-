import pandas as pd 
from pathlib import Path
import datetime
import logging
from src.config import BRONZE_BUCKET_PATH

logging.basicConfig(
    filename="logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",

)

# ingest olist_orders_dataset.csv -> file we receive everyday but this is corrupt
def ingest_orders_dataset() -> pd.DataFrame:
    '''
    Ingest the orders dataset from the given file path.

    '''
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    folder = Path(BRONZE_BUCKET_PATH)
    pattern = f"olist_orders_dataset_{current_date}.csv"

    for file_name in folder.glob(pattern=pattern):
        logging.info(f"Processing file: {file_name.name}")
        try:
            df = pd.read_csv(file_name)
            logging.info(f"Successfully read file: {file_name.name} with rows {len(df)}")
            print(f"Successfully read file: {file_name.name} with rows: {len(df)}")
            return df
        except Exception as e:
            logging.error(f"Error reading file {file_name.name}: {e}")
            print(f"Error reading file: {file_name.name}: {e}")

    logging.warning(f"No file found matching the pattern: {pattern}")
    print(f"No file found matching the pattern: {pattern}")
    return pd.DataFrame()  


