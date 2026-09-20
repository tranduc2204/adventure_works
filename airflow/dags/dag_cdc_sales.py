"""
dag_cdc_sales.py
Airflow DAG orchestrating near real-time CDC synchronization:
1. Extract incremental CDC transaction logs from SQL Server to Snowflake Bronze via dlt.
2. Run dbt incremental merge on fct_sales.
3. Run data quality tests on fct_sales.
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
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dag_adventureworks_cdc_sales",
    default_args=default_args,
    description="Synchronize CDC sales transactions from SQL Server to Snowflake Gold Fact",
    schedule_interval="*/30 * * * *",  # Runs every 30 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["adventureworks", "cdc", "sales", "incremental"],
) as dag:

    # 1. Ingest incremental CDC records via dlt
    run_dlt_cdc = BashOperator(
        task_id="run_dlt_cdc_pipeline",
        bash_command="python /opt/airflow/src/ingests/cdc_pipeline.py",
    )

    # 2. Incrementally transform & merge into fct_sales via dbt
    dbt_run_fact_sales = BashOperator(
        task_id="dbt_run_fact_sales",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --select src_cdc_sales fct_sales"
        ),
    )

    # 3. Verify data quality & integrity on fct_sales
    dbt_test_fact_sales = BashOperator(
        task_id="dbt_test_fact_sales",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt test --select fct_sales"
        ),
    )

    # Execution Lineage
    run_dlt_cdc >> dbt_run_fact_sales >> dbt_test_fact_sales

