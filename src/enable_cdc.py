#!/usr/bin/env python3
"""
enable_cdc.py - Enable SQL Server Change Data Capture (CDC) on AdventureWorks database and all tables.
"""

import os
import sys
import urllib.parse
from sqlalchemy import create_engine, text

# Try loading .env
try:
    from dotenv import load_dotenv
    for env_path in [".env", "../.env", os.path.join(os.path.dirname(__file__), "..", ".env")]:
        if os.path.isfile(env_path):
            load_dotenv(env_path)
            break
except ImportError:
    pass

DB_HOST = os.environ.get("DB_HOST", "localhost")
DB_PORT = int(os.environ.get("DB_PORT", "1433"))
DB_USER = os.environ.get("DB_USER", "sa")
DB_PASSWORD = os.environ.get("DB_PASSWORD", os.environ.get("SA_PASSWORD", "YourStrong@Passw0rd"))
DB_NAME = os.environ.get("DB_NAME", "AdventureWorks")

PRIMARY_KEYS = {
    "calendar": [("date", "DATETIME NOT NULL")],
    "customers": [("customer_key", "BIGINT NOT NULL")],
    "product_categories": [("product_category_key", "BIGINT NOT NULL")],
    "product_subcategories": [("product_subcategory_key", "BIGINT NOT NULL")],
    "products": [("product_key", "BIGINT NOT NULL")],
    "territories": [("sales_territory_key", "BIGINT NOT NULL")],
    "returns": [
        ("return_date", "DATETIME NOT NULL"),
        ("territory_key", "BIGINT NOT NULL"),
        ("product_key", "BIGINT NOT NULL"),
    ],
    "sales": [
        ("order_number", "VARCHAR(50) NOT NULL"),
        ("order_line_item", "BIGINT NOT NULL"),
    ],
}


def get_engine():
    pwd = urllib.parse.quote_plus(DB_PASSWORD)
    url = f"mssql+pymssql://{DB_USER}:{pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url, isolation_level="AUTOCOMMIT")


def main():
    print("================================================================")
    print(f" Enabling CDC for Database: {DB_NAME}")
    print(f" Host: {DB_HOST}:{DB_PORT}")
    print("================================================================")

    engine = get_engine()

    with engine.connect() as conn:
        # Step 1: Enable CDC on Database
        print("\n1. Checking database CDC status...")
        is_db_cdc = conn.execute(
            text("SELECT is_cdc_enabled FROM sys.databases WHERE name = :name"),
            {"name": DB_NAME},
        ).scalar()

        if not is_db_cdc:
            print("   Enabling CDC on database...")
            conn.execute(text("EXEC sys.sp_cdc_enable_db"))
            print("   -> Database CDC enabled successfully!")
        else:
            print("   -> Database CDC is already enabled.")

        # Step 2: Add Primary Keys
        print("\n2. Ensuring Primary Key constraints on all tables...")
        for table, cols in PRIMARY_KEYS.items():
            pk_name = f"PK_{table}"
            exists = conn.execute(
                text("SELECT 1 FROM sys.key_constraints WHERE name = :pk"),
                {"pk": pk_name},
            ).scalar()

            if not exists:
                try:
                    # Alter columns to NOT NULL
                    for col_name, col_type in cols:
                        conn.execute(text(f"ALTER TABLE [{table}] ALTER COLUMN [{col_name}] {col_type}"))
                    
                    # Add PK constraint
                    col_list = ", ".join([f"[{c[0]}]" for c in cols])
                    conn.execute(text(f"ALTER TABLE [{table}] ADD CONSTRAINT [{pk_name}] PRIMARY KEY ({col_list})"))
                    print(f"   -> Added {pk_name} on [{table}] ({col_list})")
                except Exception as e:
                    print(f"   -> Notice on [{table}] PK: {e}")
            else:
                print(f"   -> {pk_name} already exists.")

        # Step 3: Enable CDC on Tables
        print("\n3. Enabling CDC on tables...")
        for table in PRIMARY_KEYS.keys():
            is_tbl_cdc = conn.execute(
                text("SELECT is_tracked_by_cdc FROM sys.tables WHERE name = :name"),
                {"name": table},
            ).scalar()

            if not is_tbl_cdc:
                try:
                    conn.execute(text(f"""
                        EXEC sys.sp_cdc_enable_table
                            @source_schema = N'dbo',
                            @source_name   = N'{table}',
                            @role_name     = NULL,
                            @supports_net_changes = 1
                    """))
                    print(f"   -> Enabled CDC on table [{table}]")
                except Exception as e:
                    print(f"   -> Error enabling CDC on [{table}]: {e}")
            else:
                print(f"   -> CDC already enabled on table [{table}]")

        # Step 4: Summary Report
        print("\n======================= CDC STATUS REPORT =======================")
        summary = conn.execute(text("""
            SELECT 
                t.name AS [table_name],
                t.is_tracked_by_cdc,
                c.capture_instance
            FROM sys.tables t
            JOIN sys.schemas s ON t.schema_id = s.schema_id
            LEFT JOIN cdc.change_tables c ON c.source_object_id = t.object_id
            WHERE s.name = 'dbo'
            ORDER BY t.name
        """)).fetchall()

        print(f"{'Table':<25} {'CDC Enabled':<15} {'Capture Instance':<30}")
        print("-" * 70)
        for row in summary:
            status = "YES (Active)" if row[1] else "NO"
            inst = row[2] or "N/A"
            print(f"{row[0]:<25} {status:<15} {inst:<30}")
        print("=================================================================\n")


if __name__ == "__main__":
    main()
