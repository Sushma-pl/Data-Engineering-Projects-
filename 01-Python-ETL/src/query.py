import duckdb
from src.config import DB_FILE_PATH

con = duckdb.connect(database=DB_FILE_PATH)
result_df = con.execute("""SELECT COUNT(*) AS total_orders
                FROM orders""").fetch_df()

print(result_df)

