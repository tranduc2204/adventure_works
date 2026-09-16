#!/usr/bin/env python3
"""
initial_snapshot.py - Ingests a full baseline snapshot of dbo tables into Snowflake Bronze.
Captures the current Max LSN checkpoint for Phase 2.
"""

import dlt
from dlt.sources.sql_database import sql_database
from sqlalchemy import create_engine, text
import urllib.parse
import os
from dotenv import load_dotenv

load_dotenv()
# 1. SQL Server Connection
DB_USER = os.getenv("DB_USER", "sa")
DB_PASS = urllib.parse.quote_plus(os.getenv("SA_PASSWORD", ""))
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "1433")
DB_NAME = os.getenv("DB_NAME", "AdventureWorks")

SQL_ALCHEMY_URL = f"mssql+pymssql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

TABLES_TO_SYNC = [
    "calendar",
    "customers",
    "product_categories",
    "product_subcategories",
    "products",
    "returns",
    "sales",
    "territories"
]


def capture_checkpoint_lsn():
    """Captures the current highest Log Sequence Number (LSN) from SQL Server."""
    engine = create_engine(SQL_ALCHEMY_URL)
    with engine.connect() as conn:
        max_lsn = conn.execute(text("SELECT sys.fn_cdc_get_max_lsn()")).scalar()
        print(f"\n[CHECKPOINT] Current Max LSN: {max_lsn.hex() if max_lsn else 'None'}")
        return max_lsn


def run_snapshot():
    print("==========================================================")
    print(" Starting Initial Full Snapshot (dbo -> Snowflake BRONZE)")
    print("==========================================================")

    # Capture LSN checkpoint BEFORE taking snapshot
    checkpoint_lsn = capture_checkpoint_lsn()

    # Define dlt source using native sql_database connector
    source = sql_database(
        credentials=SQL_ALCHEMY_URL,
        schema="dbo",
        table_names=TABLES_TO_SYNC,
        detect_precision_hints=True
    )

    # Define dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name="sqlserver_initial_snapshot",
        destination="snowflake",
        dataset_name="BRONZE"
    )

    # Run pipeline with 'replace' disposition to establish baseline
    print("\nExtracting and loading baseline tables to Snowflake...")
    load_info = pipeline.run(source, write_disposition="replace")
    
    print("\n--- Load Summary ---")
    print(load_info)
    print("==========================================================")
    print(" Baseline Snapshot Completed Successfully!")
    print(f" IMPORTANT: Use LSN {checkpoint_lsn.hex()} as starting checkpoint for CDC!")
    print("==========================================================")


if __name__ == "__main__":
    run_snapshot()