# AdventureWorks End-to-End Modern Data Platform
> **Enterprise Data Platform:** Simulated OLTP on SQL Server &rarr; CDC Streaming & Batch Ingestion with DLT &rarr; Snowflake Data Cloud &rarr; Data Transformation & Modeling with dbt Core (Star Schema / Medallion Architecture).

---

## 1. End-to-End Architecture Overview

```mermaid
flowchart TB
    subgraph S1 ["1. OLTP SOURCE SYSTEM (Dockerized SQL Server 2022)"]
        direction TB
        CSV["8 Source CSV Files\n(data/ directory)"]
        Ingest["ingest.py\n(Batch Ingestion / SQLAlchemy)"]
        SQL_DB["AdventureWorks Database\n(dbo schema)"]
        Agent["SQL Server Agent\n(Capture Job)"]
        CDC_Log["CDC Change Table\n(cdc.dbo_sales_CT)"]

        CSV --> Ingest
        Ingest -->|Data Load| SQL_DB
        SQL_DB -->|Transaction Log| Agent
        Agent -->|Captures Insert / Update / Delete| CDC_Log
    end

    subgraph S2 ["2. DATA INGESTION LAYER (dlt - data load tool)"]
        direction TB
        DLT_Init["initial_snapshot.py\n(Baseline Full Sync: dbo -> BRONZE)"]
        DLT_Rep["replace_pipeline.py\n(Full Replace for Dimensions)"]
        DLT_CDC["cdc_pipeline.py\n(Incremental CDC Stream by LSN: Append)"]
    end

    SQL_DB --> DLT_Init
    SQL_DB --> DLT_Rep
    CDC_Log --> DLT_CDC

    subgraph S3 ["3. DATA WAREHOUSE (Snowflake Data Cloud)"]
        direction TB
        
        subgraph BronzeLayer ["BRONZE SCHEMA (Raw Tables & Append-only Logs)"]
            B_Dim["SALES, CUSTOMERS, PRODUCTS,\nCALENDAR, TERRITORIES, RETURNS,\nPRODUCT_CATEGORIES, PRODUCT_SUBCATEGORIES"]
            B_CDC["DBO_SALES_CT\n(Raw CDC event stream with LSN & Operation codes)"]
        end

        subgraph StagingLayer ["BRONZE_STAGING SCHEMA (dbt Staging & Snapshots)"]
            S_Src["12 Staging Views (src_*.sql)\nData cleaning & type casting"]
            S_CDC["src_cdc_sales.sql\nDeduplication via ROW_NUMBER() to get latest state"]
            S_Snap["snap_products (SCD Type 2)\nTracks historical price & cost changes"]
        end

        subgraph GoldLayer ["GOLD SCHEMA (Core Dimensional Modeling - Star Schema)"]
            D_Cust["dim_customers\n(Customer demographics, birth_date)"]
            D_Prod["dim_products\n(Flattened categories, normalized size & style)"]
            D_Cal["dim_calendar\n(Rich calendar hierarchy, Smart Date Key)"]
            D_Terr["dim_territories\n(Sales geographic dimensions, territory_key)"]
            
            F_Sales["fct_sales\n(Incremental Merge Fact: 1 PK = 1 Row)"]
            F_Ret["fct_returns\n(Product returns fact)"]
        end

        BronzeLayer --> StagingLayer
        StagingLayer --> GoldLayer
    end

    DLT_Init -->|write_disposition: replace| B_Dim
    DLT_Rep -->|write_disposition: replace| B_Dim
    DLT_CDC -->|write_disposition: append| B_CDC

    subgraph S4 ["4. ANALYTICS & BUSINESS INTELLIGENCE"]
        BI["Power BI / Tableau / Metabase\n• Revenue & Growth Trends (MoM, YoY)\n• Return Rate Analysis by Product & Region\n• Customer Segmentation by Demographics"]
    end

    GoldLayer --> BI
```

---

## 2. Dimensional Data Model (Kimball Star Schema)

> **Gold Layer Architecture:** Designed strictly according to Ralph Kimball's Dimensional Modeling principles. High-volume transactional Fact tables (`fct_sales`, `fct_returns`) sit at the center of the star, surrounded by Conformed Dimensions (`dim_calendar`, `dim_customers`, `dim_products`, `dim_territories`). Features **Kimball Pure SCD Type 2 Surrogate Keys (`product_scd_key`)** and **Smart Integer Date Keys (`date_key`)** for high-performance $O(1)$ equi-joins on Snowflake.

### 2.1. Star Schema Topology

```mermaid
flowchart TD
    subgraph Dimensions ["🌟 Conformed Dimension Tables (Gold Layer)"]
        DC["📅 <b>DIM_CALENDAR</b><br/>PK: date_key (YYYYMMDD)<br/>Hierarchies: Year, Quarter, Month, Day, Weekday"]
        DP["🚲 <b>DIM_PRODUCTS (SCD Type 2)</b><br/>PK: product_scd_key (dbt_scd_id)<br/>NK: product_key | Price, Cost, Category, Size"]
        DU["👤 <b>DIM_CUSTOMERS</b><br/>PK: customer_key<br/>Demographics: Name, Gender, Birth Date, Income"]
        DT["🌍 <b>DIM_TERRITORIES</b><br/>PK: territory_key<br/>Geography: Region, Country, Continent"]
    end

    subgraph Facts ["⚡ Core Fact Tables (Gold Layer)"]
        FS["🛒 <b>FCT_SALES</b> (Incremental Merge)<br/>PK: order_number, order_line_item<br/>Measures: order_quantity<br/>SCD2 SK: product_scd_key"]
        FR["📦 <b>FCT_RETURNS</b><br/>Measures: return_quantity<br/>SCD2 SK: product_scd_key"]
    end

    DC -->|"order_date_key / stock_date_key (1:N)"| FS
    DP -->|"product_scd_key (Pure SCD2 Equi-Join 1:N)"| FS
    DU -->|"customer_key (1:N)"| FS
    DT -->|"territory_key (1:N)"| FS

    DC -->|"return_date_key (1:N)"| FR
    DP -->|"product_scd_key (1:N)"| FR
    DT -->|"territory_key (1:N)"| FR
```

### 2.2. Entity-Relationship (ER) Diagram & Table Attributes

```mermaid
erDiagram
    DIM_CALENDAR ||--o{ FCT_SALES : "order_date_key = date_key"
    DIM_CALENDAR ||--o{ FCT_SALES : "stock_date_key = date_key"
    DIM_CUSTOMERS ||--o{ FCT_SALES : "customer_key"
    DIM_PRODUCTS ||--o{ FCT_SALES : "product_scd_key (SCD2)"
    DIM_TERRITORIES ||--o{ FCT_SALES : "territory_key"

    DIM_CALENDAR ||--o{ FCT_RETURNS : "return_date_key = date_key"
    DIM_PRODUCTS ||--o{ FCT_RETURNS : "product_scd_key (SCD2)"
    DIM_TERRITORIES ||--o{ FCT_RETURNS : "territory_key"

    DIM_CALENDAR {
        int date_key PK "Smart Date Key (YYYYMMDD)"
        date full_date "Standard Calendar Date"
        int year "Calendar Year"
        int quarter "Quarter (1 - 4)"
        int month "Month (1 - 12)"
        string month_name "Month Name (e.g. May)"
        string day_name "Day Name (e.g. Monday)"
        boolean is_weekend "Weekend Indicator"
    }

    DIM_CUSTOMERS {
        int customer_key PK "Surrogate / Business Key"
        string first_name "Customer First Name"
        string last_name "Customer Last Name"
        date birth_date "Date of Birth"
        string gender "Gender (M / F)"
        decimal annual_income "Annual Income"
    }

    DIM_PRODUCTS {
        string product_scd_key PK "dbt_scd_id (SCD Type 2 Surrogate Key)"
        int product_key "Business / Natural Key"
        string product_sku "Product SKU"
        string product_name "Product Name"
        string category_name "Category (e.g. Bikes, Accessories)"
        string subcategory_name "Subcategory (e.g. Mountain Bikes)"
        string product_size "Normalized Size Value"
        string product_size_type "Size Domain (Clothing / Frame / No Size)"
        string product_style "Style (Unisex / Women / Men)"
        decimal product_cost "Historical Unit Cost at Snapshot"
        decimal product_price "Historical Unit Price at Snapshot"
        int valid_from_date_key "SCD2 Valid From (YYYYMMDD / 19000101)"
        int valid_to_date_key "SCD2 Valid To (YYYYMMDD / NULL)"
    }

    DIM_TERRITORIES {
        int territory_key PK "Territory Unique Identifier"
        string region "Sales Region"
        string country "Country Name"
        string continent "Continent Name"
    }

    FCT_SALES {
        string order_number PK "Sales Order Number"
        int order_line_item PK "Order Line Item Number"
        int order_date_key FK "Order Date Key (links to DIM_CALENDAR)"
        int stock_date_key FK "Stocking Date Key (links to DIM_CALENDAR)"
        string product_scd_key FK "SCD2 Surrogate Key (links to DIM_PRODUCTS)"
        int product_key "Business Key (SCD1 / Current State Analysis)"
        int customer_key FK "Customer Key (links to DIM_CUSTOMERS)"
        int territory_key FK "Territory Key (links to DIM_TERRITORIES)"
        int order_quantity "Quantity Ordered"
        boolean is_deleted "CDC Tombstone Flag (Soft Delete)"
    }

    FCT_RETURNS {
        int return_date_key FK "Return Date Key (links to DIM_CALENDAR)"
        int territory_key FK "Territory Key (links to DIM_TERRITORIES)"
        string product_scd_key FK "SCD2 Surrogate Key (links to DIM_PRODUCTS)"
        int product_key "Business Key"
        int return_quantity "Quantity Returned"
    }
```

### 2.3. Key Modeling Innovations
1. **Kimball Pure SCD Type 2 via Surrogate Key:** Rather than storing only `product_key` and forcing downstream analytics to run expensive non-equi range joins (`BETWEEN valid_from AND valid_to`), `fct_sales` materializes the Point-in-Time version key (`product_scd_key = p.dbt_scd_id`). All BI queries execute lightning-fast $O(1)$ equi-joins with zero memory spilling.
2. **Dual-Key Strategy:** Both `product_scd_key` (Historical As-Was) and `product_key` (Current As-Is) are retained in Fact tables, granting maximum analytical flexibility.
3. **Smart Integer Date Keys:** Integer-encoded dates (`YYYYMMDD`) eliminate time-zone translation overhead and maximize Snowflake micro-partition pruning.

---

## 3. Project Directory Structure

```
adventure_works/
├── data/                            # 8 Source CSV files
│   ├── calendar.csv                 # Calendar dates (911 rows)
│   ├── customers.csv                # Customer profiles (18,148 rows)
│   ├── product_categories.csv       # High-level product categories (4 rows)
│   ├── product_subcategories.csv    # Detailed product subcategories (37 rows)
│   ├── products.csv                 # Product catalog & pricing (293 rows)
│   ├── returns.csv                  # Return transactions (1,809 rows)
│   ├── sales.csv                    # Historical sales orders (23,935 rows)
│   └── territories.csv              # Geographic sales territories (10 rows)
├── docker/                          # Docker infrastructure
│   ├── Dockerfile                   # Custom SQL Server 2022 image with Python runtime
│   └── entrypoint.sh                # Container startup and automated ingestion script
├── scripts/
│   └── run_sqlserver.sh             # 1-click script to build and launch SQL Server container
├── src/                             # Ingestion & CDC Python source code
│   ├── ingest.py                    # Ingests 8 CSVs into SQL Server database
│   ├── enable_cdc.py                # Enables SQL Server CDC on database & sales table
│   └── ingests/                     # dlt (data load tool) pipelines
│       ├── initial_snapshot.py      # Full baseline ingestion of 8 tables into Snowflake Bronze
│       ├── replace_pipeline.py      # Periodic full replace sync for dimension tables
│       ├── cdc_pipeline.py          # Incremental CDC stream sync by LSN into Bronze
│       ├── test_conn.py             # Snowflake connection health check
│       └── test_conn_sqlserver.py   # SQL Server connection health check
├── dbt_project/                     # dbt Core project (Transformation & Modeling)
│   ├── dbt_project.yml              # Project configuration & schema definitions
│   ├── profiles.yml                 # Snowflake connection config (RSA Key-Pair Auth)
│   ├── analyses/                    # Ad-hoc business analysis queries
│   │   ├── q01.sql                  # Q1: Total gross revenue (CFO)
│   │   ├── q02.sql                  # Q2: Category revenue ranking with SCD2 handling
│   │   ├── q04.sql                  # Q4: Total returned quantity & date range
│   │   ├── q05.sql                  # Q5: Catalog breakdown by product color & % share
│   │   └── q06.sql                  # Q6: Gross margin & profitability by subcategory
│   ├── macros/
│   │   └── generate_schema_name.sql # Custom schema name resolution macro
│   ├── models/
│   │   ├── sources.yml              # Declaration of Snowflake BRONZE source tables
│   │   ├── schema.yml               # 30 automated data tests & column documentation
│   │   ├── src/                     # [STAGING LAYER - BRONZE_STAGING SCHEMA]
│   │   │   ├── src_calendar.sql
│   │   │   ├── src_customers.sql
│   │   │   ├── src_product_categories.sql
│   │   │   ├── src_product_subcategories.sql
│   │   │   ├── src_products.sql
│   │   │   ├── src_returns.sql
│   │   │   ├── src_sales.sql        # Baseline staging for historical sales
│   │   │   ├── src_cdc_sales.sql    # CDC log deduplication by LSN
│   │   │   ├── src_territories.sql
│   │   │   ├── src__dlt_loads.sql
│   │   │   ├── src__dlt_pipeline_state.sql
│   │   │   └── src__dlt_version.sql
│   │   ├── dim/                     # [GOLD LAYER - DIMENSIONS]
│   │   │   ├── dim_calendar.sql     # Rich date dimension (Date Key, Year, Quarter, Month, Weekday)
│   │   │   ├── dim_customers.sql    # Cleaned customer profiles, birth_date
│   │   │   ├── dim_products.sql     # Flattened product catalog, normalized size & style
│   │   │   └── dim_territories.sql  # Geographic sales territory dimension
│   │   └── fct/                     # [GOLD LAYER - FACTS]
│   │       ├── fct_sales.sql        # Incremental Merge Fact (combining baseline + CDC events)
│   │       └── fct_returns.sql      # Product returns fact table
│   └── snapshots/
│       └── snap_products.sql        # SCD Type 2 snapshot tracking price & cost history
├── set_up_sql/
│   └── create_role_ingest.sql       # Snowflake setup script (Roles, Users, Privileges)
├── .dlt/                            # dlt framework configuration
│   ├── config.toml
│   └── secrets.toml                 # Encrypted credentials for Snowflake & SQL Server
├── docker-compose.yml
├── requirements.txt
├── rsa_key.p8                       # Snowflake private key (Key-Pair Authentication)
├── rsa_key.pub                      # Snowflake public key
└── .env                             # Local environment variables
```

---

## 4. Phase 1: OLTP System Simulation (SQL Server 2022 on Docker)

The project simulates an enterprise transactional database (OLTP / ERP) using **SQL Server 2022** running inside Docker:

### 4.1. Source Datasets & Table Mappings (`src/ingest.py`)

| Source CSV (`data/`) | Target SQL Server Table | Record Count | Date Columns (`DATETIME`) | Business Description |
| :--- | :--- | :---: | :--- | :--- |
| `calendar.csv` | `dbo.calendar` | 911 | `date` | Reference calendar dates |
| `customers.csv` | `dbo.customers` | 18,148 | `birth_date` | Customer demographic records |
| `product_categories.csv` | `dbo.product_categories` | 4 | - | Top-level product category classifications |
| `product_subcategories.csv` | `dbo.product_subcategories` | 37 | - | Granular product subcategories |
| `products.csv` | `dbo.products` | 293 | - | Complete product catalog and pricing |
| `returns.csv` | `dbo.returns` | 1,809 | `return_date` | Product return transactions |
| `sales.csv` | `dbo.sales` | 23,935 | `order_date`, `stock_date` | Historical order line-item transactions |
| `territories.csv` | `dbo.territories` | 10 | - | Geographic sales regions & countries |

### 4.2. Automated Ingestion Highlights:
1. `wait_for_sql_server`: Actively polls port `1433` until SQL Server is healthy and ready to accept queries.
2. `ensure_database`: Automatically executes `CREATE DATABASE [AdventureWorks]` if it does not already exist.
3. Standardizes column names to lowercase and strips leading/trailing whitespaces.
4. Auto-detects and casts date strings to native `DATETIME` types so SQL Server creates proper date columns instead of generic `VARCHAR`.
5. Ingests records in batches (`chunksize=1000`) for optimal memory and disk I/O performance.

---

## 5. Phase 2: Change Data Capture (CDC) Configuration

The platform leverages **SQL Server Change Data Capture (CDC)** on the high-frequency transactional table `sales`:

### 5.1. Enabling CDC (`src/enable_cdc.py`)
1. **Database-Level Enablement:** Executes `sys.sp_cdc_enable_db`.
2. **Primary Key Enforcement:** Ensures a composite primary key `PK_sales` on `(order_number, order_line_item)`.
3. **Table-Level Enablement:** Executes `sys.sp_cdc_enable_table` on `dbo.sales` with `@supports_net_changes = 1`.
4. **Change Table Generation:** SQL Server automatically creates the tracking table **`cdc.dbo_sales_CT`**.

### 5.2. CDC Operation Identifiers (`__$operation`):
* **`1` = DELETE**: Record was deleted in the OLTP database.
* **`2` = INSERT**: A new sales order was created.
* **`3` = UPDATE (Before)**: The old state of the record *immediately prior* to the update.
* **`4` = UPDATE (After)**: The updated state of the record *immediately after* the update.

> [!IMPORTANT]
> SQL Server CDC relies on the **SQL Server Agent** background capture job. In `docker-compose.yml`, `MSSQL_AGENT_ENABLED: "true"` is configured to ensure the capture job continuously writes transaction log changes into `cdc.dbo_sales_CT`.

---

## 6. Phase 3: Data Ingestion to Snowflake via DLT

The ingestion layer uses **`dlt` (data load tool)** with **RSA Key-Pair Authentication** to extract and load data into Snowflake:

### 6.1. Three Specialized Ingestion Pipelines:

#### 1. Baseline Snapshot Pipeline (`src/ingests/initial_snapshot.py`):
- Loads all 8 source tables from `dbo` to Snowflake schema `BRONZE` using `write_disposition="replace"`.
- Records the highest current Log Sequence Number (`capture_checkpoint_lsn()`) as the initial CDC checkpoint.

#### 2. Full Replace Pipeline (`src/ingests/replace_pipeline.py`):
- Dedicated to the 7 dimension and returns tables (`calendar`, `customers`, `products`, `product_categories`, `product_subcategories`, `returns`, `territories`).
- Runs on a periodic schedule with `write_disposition="replace"` to maintain synchronicity with source master data.

#### 3. Incremental CDC Streaming Pipeline (`src/ingests/cdc_pipeline.py`):
- Targets the change table `cdc.dbo_sales_CT`.
- Employs an **Incremental Cursor** on `__$start_lsn`:
  ```python
  source.resources["dbo_sales_CT"].apply_hints(
      incremental=dlt.sources.incremental(
          '"__$start_lsn"',
          initial_value=b"\x00" * 10
      )
  )
  ```
- Appends newly captured transaction events into **`ADVENTUREWORKS.BRONZE.DBO_SALES_CT`** using **`write_disposition="append"`** to preserve a complete audit trail.

---

## 7. Phase 4: Data Modeling & Transformation with dbt Core

The transformation layer adopts the **Medallion Architecture (Bronze &rarr; Staging &rarr; Gold)** and **Kimball Star Schema**:

### 7.1. Staging Layer (`BRONZE_STAGING` Schema):
Consists of 12 views (`src_*.sql`) providing preliminary cleaning, casting, and renaming.

* **CDC Deduplication Algorithm in [`src_cdc_sales.sql`](dbt_project/models/src/src_cdc_sales.sql):**
  Discards obsolete `UPDATE_BEFORE` records (`_operation = 3`) and isolates the latest event per order line using window functions:
  ```sql
  ROW_NUMBER() OVER (
      PARTITION BY order_number, order_line_item 
      ORDER BY _start_lsn DESC, _seqval DESC
  ) AS rn
  ```

### 7.2. SCD Type 2 Snapshot ([`snapshots/snap_products.sql`](dbt_project/snapshots/snap_products.sql)):
Applies dbt snapshot `check` strategy on pricing columns (`product_price`, `product_cost`) to capture price change history over time (`dbt_valid_from`, `dbt_valid_to`).

### 7.3. Gold Layer: Kimball Star Schema Details

*(See [Section 2: Dimensional Data Model](#2-dimensional-data-model-kimball-star-schema) for full topology and column definitions).*

```mermaid
erDiagram
    DIM_CALENDAR ||--o{ FCT_SALES : "order_date_key = date_key"
    DIM_CALENDAR ||--o{ FCT_SALES : "stock_date_key = date_key"
    DIM_CUSTOMERS ||--o{ FCT_SALES : "customer_key"
    DIM_PRODUCTS ||--o{ FCT_SALES : "product_scd_key (SCD2)"
    DIM_TERRITORIES ||--o{ FCT_SALES : "territory_key"

    DIM_CALENDAR ||--o{ FCT_RETURNS : "return_date_key = date_key"
    DIM_PRODUCTS ||--o{ FCT_RETURNS : "product_scd_key (SCD2)"
    DIM_TERRITORIES ||--o{ FCT_RETURNS : "territory_key"

    DIM_CALENDAR {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        string month_name
        string day_name
        boolean is_weekend
    }

    DIM_CUSTOMERS {
        int customer_key PK
        string first_name
        string last_name
        date birth_date
        string gender
        decimal annual_income
    }

    DIM_PRODUCTS {
        string product_scd_key PK
        int product_key
        string product_name
        string category_name
        string subcategory_name
        string product_size
        string product_size_type
        string product_style
        decimal product_price
        decimal product_cost
        int valid_from_date_key
        int valid_to_date_key
    }

    DIM_TERRITORIES {
        int territory_key PK
        string region
        string country
        string continent
    }

    FCT_SALES {
        string order_number PK
        int order_line_item PK
        int order_date_key FK
        int stock_date_key FK
        string product_scd_key FK
        int product_key
        int customer_key FK
        int territory_key FK
        int order_quantity
        boolean is_deleted
    }

    FCT_RETURNS {
        int return_date_key FK
        int territory_key FK
        string product_scd_key FK
        int product_key
        int return_quantity
    }
```

#### Gold Dimension & Fact Details:
1. **`dim_calendar`**: Rich time dimension table generated dynamically from `2000-01-01` through `CURRENT_DATE()` using Snowflake's `TABLE(GENERATOR())` in `src_calendar`. Produces integer **Smart Date Keys** (`YYYYMMDD`), allowing Fact tables to link to time attributes without expensive SQL joins.
2. **`dim_customers`**: Demographic profiles for 18,148 customers with normalized `birth_date` cast to `DATE`.
3. **`dim_products`**: Denormalized (flattened) catalog joining products, subcategories, and categories. Tracks pricing history via SCD Type 2 (`product_scd_key`). Standardizes mixed alphanumeric sizes (`Clothing` vs. `Frame (cm)`) and style codes (`Unisex`, `Women`, `Men`).
4. **`dim_territories`**: 10 global sales regions with unified `territory_key`.
5. **`fct_sales`**: Transactional sales fact table utilizing **Incremental Merge Strategy**. Captures point-in-time pricing via `product_scd_key`. Initial run ingests 23,935 baseline orders; subsequent runs execute a `MERGE INTO` statement on `(order_number, order_line_item)` using CDC events, preventing revenue duplication.
6. **`fct_returns`**: Product returns fact table tracking 1,809 return records to measure Return Rates.

---

## 8. Phase 5: Data Quality Testing & Governance

Configured in [`models/schema.yml`](dbt_project/models/schema.yml) with **30 automated data tests**:
- **Uniqueness & Not-Null:** Enforced across all primary keys in both Dimension and Fact models.
- **Referential Integrity (`relationships`):** Verifies that `fct_sales.customer_key` exists in `dim_customers`.
- **Accepted Values:** Validates categorical domain values, such as customer gender (`gender IN ['M', 'F']`) and CDC operation codes (`_operation IN [1, 2, 3, 4]`).

---

## 9. Operational Runbook (Step-by-Step CLI Execution)

### Step 1: Start SQL Server on Docker
```bash
# Grant execution permissions and run automated startup script
chmod +x scripts/run_sqlserver.sh
./scripts/run_sqlserver.sh
```

### Step 2: Enable Change Data Capture (CDC)
```bash
.venv/bin/python src/enable_cdc.py
```

### Step 3: Execute Ingestion to Snowflake via DLT
```bash
# 1. Ingest initial baseline snapshot (run once)
.venv/bin/python src/ingests/initial_snapshot.py

# 2. Ingest master dimension tables (full replace)
.venv/bin/python src/ingests/replace_pipeline.py

# 3. Stream CDC transaction logs (run periodically or upon new events)
.venv/bin/python src/ingests/cdc_pipeline.py
```

### Step 4: Run Transformations & Tests with dbt
```bash
cd dbt_project

# 1. Execute SCD Type 2 snapshot for products
dbt snapshot

# 2. Build the entire model pipeline (use --full-refresh on the first fct_sales run)
dbt run --select fct_sales --full-refresh
dbt run

# 3. Execute all 30 automated data quality tests
dbt test

# 4. Preview model outputs directly in terminal without opening Snowflake UI
dbt show --select dim_customers --limit 5
dbt show --select fct_sales --limit 5

# 5. Generate and serve interactive documentation & lineage graph
dbt docs generate
dbt docs serve
```

### Step 5: Automated Pipeline Orchestration with Apache Airflow
Instead of manually running CLI commands, you can run the entire platform automatically using the built-in vanilla Apache Airflow setup in `docker-compose.yml`:

```bash
# 1. Build images and start all services in the background
docker compose up -d --build

# 2. Access the Airflow Web UI
# URL: http://localhost:8080
# Credentials: admin / admin

# 3. Trigger or monitor production DAGs:
# - dag_adventureworks_cdc_sales: Near real-time sync (every 30m) for CDC logs -> fct_sales merge -> tests
# - dag_adventureworks_daily_dimensions: Daily batch sync (01:00 AM) for Master Dimensions -> SCD2 Snapshot -> tests
```


---

## 10. CDC Verification Walkthrough

To verify end-to-end CDC replication and incremental merging:

1. **Insert a new sales order into SQL Server:**
   ```sql
   INSERT INTO dbo.sales (order_date, stock_date, order_number, product_key, customer_key, territory_key, order_line_item, order_quantity)
   VALUES (GETDATE(), GETDATE(), 'ORD-CDC-DEMO-999', 310, 11000, 1, 1, 10);
   ```

2. **Run the DLT CDC pipeline:**
   ```bash
   .venv/bin/python src/ingests/cdc_pipeline.py
   ```
   *DLT extracts the change event (`_OPERATION = 2`) and loads 1 package into `ADVENTUREWORKS.BRONZE.DBO_SALES_CT`.*

3. **Execute dbt incremental merge:**
   ```bash
   cd dbt_project && dbt run --select fct_sales
   ```
   *dbt executes an atomic `MERGE INTO`: merging exactly 1 new record into `GOLD.FCT_SALES` without reloading the 23,935 historical records!*

---

## 11. Phase 6: Ad-Hoc Business Analytics (`dbt_project/analyses/`)

The platform contains production-ready analytical queries in `dbt_project/analyses/`, addressing key executive business questions across Finance, Merchandising, and Quality Assurance:

| Analysis File | Business Persona | Core Question & Objective | Key Technical / Modeling Approach |
| :--- | :--- | :--- | :--- |
| [`q01.sql`](dbt_project/analyses/q01.sql) | **CFO** | Total gross sales revenue across the entire platform. | Aggregates `fct_sales` joined with `dim_products` to compute total orders, total units sold, and gross revenue (`order_quantity * product_price`). |
| [`q02.sql`](dbt_project/analyses/q02.sql) | **Head of Merchandising** | Top-performing product categories by revenue (descending). | Resolves **SCD Type 2 Point-in-Time Join** via `dim_calendar` (`full_date >= dbt_valid_from AND (full_date < dbt_valid_to OR dbt_valid_to IS NULL)`), eliminating duplicate rows caused by pricing history. |
| [`q04.sql`](dbt_project/analyses/q04.sql) | **Head of Quality** | Total returned goods quantity and operational date boundary. | Joins `fct_returns` with `dim_calendar` to calculate total return records, earliest/latest return dates, and aggregate return volume. |
| [`q05.sql`](dbt_project/analyses/q05.sql) | **Head of Merchandising** | Product color distribution and catalogue market share (% of total). | Aggregates distinct products, average price points, and computes % of catalog share via cross-join CTE with `dim_products`. |
| [`q06.sql`](dbt_project/analyses/q06.sql) | **CFO** | Most profitable product subcategories by Gross Margin. | Computes gross profit margin: `SUM(order_quantity * product_price - order_quantity * product_cost)` grouped by `subcategory_name` and sorted descending. |

### How to Run & Preview Analyses

In dbt, files in `analyses/` are treated as ad-hoc analytical queries rather than materialized database models (`dbt run` does not materialize them into tables/views):

1. **Preview results directly in terminal:**
   ```bash
   cd dbt_project
   dbt show --select q01
   dbt show --select q02
   dbt show --select q04
   dbt show --select q05
   dbt show --select q06
   ```

2. **Compile to native SQL for Snowflake worksheets or BI reporting:**
   ```bash
   dbt compile --select q02
   # Compiled SQL is generated at: target/compiled/dbt_project/analyses/q02.sql
   ```

