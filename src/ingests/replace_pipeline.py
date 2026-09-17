import dlt
from dlt.sources.sql_database import sql_database
import urllib.parse
import os
from dotenv import load_dotenv


#  load environment variables from .env
load_dotenv()

DB_USER = os.getenv("DB_USER", "sa")
DB_PASS = urllib.parse.quote_plus(os.getenv("SA_PASSWORD", "Radeon@2204"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "AdventureWorks")

SQL_ALCHEMY_URL = f"mssql+pymssql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


REPLACE_TABLE = [
    "territories", 
    "product_categories",
    "product_subcategories",
    "products",
    "calendar",
    "returns"
]

def run_replace_sync():
    print("==========================================================")
    print(" Running Full Replace Sync (source schema -> Snowflake)")
    print("==========================================================")

    # Define SQL source querying the source schema
    source = sql_database(
        credentials=SQL_ALCHEMY_URL,
        schema="dbo",
        table_names=REPLACE_TABLE,
        detect_precision_hints=True
    )

    pipeline = dlt.pipeline(
        pipeline_name="sqlserver_replace_to_snowflake",
        destination="snowflake",
        dataset_name="BRONZE"
    )

    
    load_info = pipeline.run(source, write_disposition="replace")

    print("\n--- REPLACE Load Summary ---")
    print(load_info) 
    
if __name__ == "__main__":
    run_replace_sync()   

































