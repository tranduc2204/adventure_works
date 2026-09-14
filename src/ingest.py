#!/usr/bin/env python3
"""
ingest.py - Ingest CSV files into SQL Server tables for AdventureWorks dataset.
"""

import os
import sys
import glob
import time
import argparse
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text, inspect

# Try loading .env from current directory or parent directory
try:
    from dotenv import load_dotenv
    for env_path in [
        ".env",
        "../.env",
        os.path.join(os.path.dirname(__file__), "..", ".env"),
        os.path.join(os.path.dirname(__file__), ".env"),
    ]:
        if os.path.isfile(env_path):
            load_dotenv(env_path)
            break
except ImportError:
    pass

# Date column candidates for proper SQL datetime types
DATE_COLUMNS = {"date", "birth_date", "return_date", "order_date", "stock_date"}


def find_default_data_dir() -> str:
    candidates = [
        os.environ.get("DATA_DIR"),
        "./data",
        "../data",
        os.path.join(os.path.dirname(__file__), "..", "data"),
        "/usr/src/app/data",
    ]
    for c in candidates:
        if c and os.path.isdir(c):
            return c
    return "./data"


def parse_args():
    parser = argparse.ArgumentParser(description="Ingest CSV datasets into SQL Server tables.")
    parser.add_argument(
        "--host",
        default=os.environ.get("DB_HOST", "localhost"),
        help="SQL Server host (default: localhost or $DB_HOST)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DB_PORT", "1433")),
        help="SQL Server port (default: 1433 or $DB_PORT)",
    )
    parser.add_argument(
        "--user",
        default=os.environ.get("DB_USER", "sa"),
        help="Database user (default: sa or $DB_USER)",
    )
    parser.add_argument(
        "--password",
        default=os.environ.get("DB_PASSWORD", os.environ.get("SA_PASSWORD", "YourStrong@Passw0rd")),
        help="Database password (default: YourStrong@Passw0rd or $SA_PASSWORD / $DB_PASSWORD)",
    )
    parser.add_argument(
        "--database",
        default=os.environ.get("DB_NAME", "AdventureWorks"),
        help="Target database name (default: AdventureWorks or $DB_NAME)",
    )
    parser.add_argument(
        "--data-dir",
        default=find_default_data_dir(),
        help=f"Directory containing CSV files (default: {find_default_data_dir()} or $DATA_DIR)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=1000,
        help="Batch chunk size for SQL insertion (default: 1000)",
    )
    return parser.parse_args()


def get_connection_url(host: str, port: int, user: str, password: str, database: str) -> str:
    encoded_password = urllib.parse.quote_plus(password)
    return f"mssql+pymssql://{user}:{encoded_password}@{host}:{port}/{database}"


def wait_for_sql_server(host: str, port: int, user: str, password: str, max_retries: int = 30, delay: int = 2):
    master_url = get_connection_url(host, port, user, password, "master")
    print(f"Connecting to SQL Server at {host}:{port}...")

    for attempt in range(1, max_retries + 1):
        try:
            engine = create_engine(master_url, connect_args={"timeout": 5})
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(f"SQL Server is ready! (Connected on attempt {attempt})")
            return
        except Exception as e:
            print(f"Attempt {attempt}/{max_retries}: SQL Server not ready yet ({e.__class__.__name__}). Retrying in {delay}s...")
            time.sleep(delay)

    print("ERROR: Timeout waiting for SQL Server to start.")
    sys.exit(1)


def ensure_database(host: str, port: int, user: str, password: str, database: str):
    master_url = get_connection_url(host, port, user, password, "master")
    engine_master = create_engine(master_url, isolation_level="AUTOCOMMIT")

    with engine_master.connect() as conn:
        result = conn.execute(
            text("SELECT database_id FROM sys.databases WHERE name = :name"),
            {"name": database},
        ).fetchone()

        if not result:
            print(f"Creating database [{database}]...")
            conn.execute(text(f"CREATE DATABASE [{database}]"))
            print(f"Database [{database}] created successfully.")
        else:
            print(f"Database [{database}] already exists.")


def ingest_data(host: str, port: int, user: str, password: str, database: str, data_dir: str, chunksize: int):
    target_url = get_connection_url(host, port, user, password, database)
    engine = create_engine(target_url)

    csv_files = sorted(glob.glob(os.path.join(data_dir, "*.csv")))
    if not csv_files:
        print(f"WARNING: No CSV files found in {data_dir}")
        return

    print(f"\nFound {len(csv_files)} CSV files to ingest into [{database}]:")
    summary = []

    for csv_file in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_file))[0].lower()
        print(f"\n--- Processing '{os.path.basename(csv_file)}' -> table [{table_name}] ---")

        # Read CSV file
        df = pd.read_csv(csv_file)
        initial_rows = len(df)

        # Standardize column names (strip whitespace and lowercase)
        df.columns = [col.strip().lower() for col in df.columns]

        # Convert date columns to datetime
        for col in df.columns:
            if col in DATE_COLUMNS or "date" in col:
                try:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                    print(f"  Converted column '{col}' to datetime.")
                except Exception as ex:
                    print(f"  Notice: Could not parse '{col}' as datetime: {ex}")

        # Ingest into SQL Server
        start_time = time.time()
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=chunksize,
        )
        elapsed = time.time() - start_time

        # Verify count in database
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM [{table_name}]")).scalar()

        print(f"  Inserted {count} rows in {elapsed:.2f}s ({count / max(elapsed, 0.001):.1f} rows/s)")
        summary.append({"Table": table_name, "Source CSV": os.path.basename(csv_file), "Rows": count})

    print("\n================== INGESTION SUMMARY ==================")
    print(f"{'Table':<25} {'Source CSV':<30} {'Rows':<10}")
    print("-" * 68)
    for item in summary:
        print(f"{item['Table']:<25} {item['Source CSV']:<30} {item['Rows']:<10}")
    print("=======================================================\n")


def main():
    args = parse_args()
    wait_for_sql_server(args.host, args.port, args.user, args.password)
    ensure_database(args.host, args.port, args.user, args.password, args.database)
    ingest_data(args.host, args.port, args.user, args.password, args.database, args.data_dir, args.chunksize)
    print("All tables successfully ingested into SQL Server!")


if __name__ == "__main__":
    main()

