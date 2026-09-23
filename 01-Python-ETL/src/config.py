from pathlib import Path
from dotenv import load_dotenv
import os 
load_dotenv()

BRONZE_BUCKET_PATH = os.getenv("BRONZE_BUCKET_PATH")
SILVER_BUCKET_PATH = os.getenv("SILVER_BUCKET_PATH")
GOLD_BUCKET_PATH = os.getenv("GOLD_BUCKET_PATH")
DB_FILE_PATH = os.getenv("DB_FILE_PATH")
