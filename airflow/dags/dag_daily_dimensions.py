"""
dag_daily_dimensions.py
Airflow DAG orchestrating daily batch master data synchronization:
1. Ingest master dimensions (customers, products, territories, categories, returns) via dlt.
2. Run dbt snapshot (SCD Type 2) on snap_products to capture pricing history.
3. Run dbt transformation models for all dimensions and returns fact.
4. Run comprehensive data quality tests on dimensions and facts.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_adventureworks_daily_dimensions",
    default_args=default_args,
    description="Daily sync for Master Dimensions, Product Pricing SCD2, and Returns",
    schedule_interval="0 1 * * *",  # Runs daily at 01:00 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["adventureworks", "dimensions", "scd2", "daily"],
) as dag:

    # 1. Ingest dimension tables from SQL Server to Snowflake Bronze via dlt
    run_dlt_dimensions = BashOperator(
        task_id="run_dlt_replace_pipeline",
        bash_command="python /opt/airflow/src/ingests/replace_pipeline.py",
    )

    # 2. Run SCD Type 2 snapshot for products
    dbt_snapshot_products = BashOperator(
        task_id="dbt_snapshot_products",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt snapshot --select snap_products"
        ),
    )

    # 3. Build Staging, Dimensions, and Returns Fact tables
    dbt_run_dimensions = BashOperator(
        task_id="dbt_run_dimensions_and_returns",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select src_* dim_* fct_returns"
        ),
    )

    # 4. Run automated tests across dimensions and returns
    dbt_test_dimensions = BashOperator(
        task_id="dbt_test_dimensions_and_returns",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --select dim_* fct_returns"
        ),
    )

    # Execution Lineage
    run_dlt_dimensions >> dbt_snapshot_products >> dbt_run_dimensions >> dbt_test_dimensions

