#!/usr/bin/env python3
"""
cdc_pipeline.py - Incrementally captures SQL Server CDC logs and streams them to Snowflake.
"""

import dlt
from dlt.sources.sql_database import sql_database
import urllib.parse
import os

DB_USER = os.getenv("DB_USER", "sa")
DB_PASS = urllib.parse.quote_plus(os.getenv("SA_PASSWORD", "Radeon@2204"))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "AdventureWorks")

SQL_ALCHEMY_URL = f"mssql+pymssql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# List of CDC change tables
CDC_TABLES = [
    "dbo_calendar_CT",
    "dbo_customers_CT",
    "dbo_product_categories_CT",
    "dbo_product_subcategories_CT",
    "dbo_products_CT",
    "dbo_returns_CT",
    "dbo_sales_CT",
    "dbo_territories_CT"
]


def run_cdc_sync():
    print("==========================================================")
    print(" Running Incremental CDC Sync (cdc schema -> Snowflake)")
    print("==========================================================")

    # Define SQL source querying the cdc schema
    # Uses incremental loading on __$start_lsn
    source = sql_database(
        credentials=SQL_ALCHEMY_URL,
        schema="cdc",
        table_names=CDC_TABLES,
        detect_precision_hints=True
    )

    # Configure incremental cursor for each change table
    for table_name in CDC_TABLES:
        source.resources[table_name].apply_hints(
            incremental=dlt.sources.incremental(
                "__$start_lsn",
                initial_value=b"\x00" * 10  # Starts from zero or your checkpoint LSN
            )
        )

    pipeline = dlt.pipeline(
        pipeline_name="sqlserver_cdc_to_snowflake",
        destination="snowflake",
        dataset_name="BRONZE"
    )

    # In Bronze, CDC records are always appended
    load_info = pipeline.run(source, write_disposition="append")
    
    print("\n--- CDC Load Summary ---")
    print(load_info)


if __name__ == "__main__":
    run_cdc_sync()  