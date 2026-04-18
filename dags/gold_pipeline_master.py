from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Ensure the root project path is in sys.path for local imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from ingest_manager import GoldIngestor

default_args = {
    'owner': 'Gold Analytics Team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@goldproject.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_ingestion():
    """Wrapper function to trigger the GoldIngestor."""
    series_map = {
        'WB/commodity_prices/FGOLD-1W': 'gold_prices_api',
        'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
        'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
        'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api'
    }
    
    ingestor = GoldIngestor()
    try:
        for sid, table in series_map.items():
            ingestor.fetch_and_ingest(sid, table)
    finally:
        ingestor.close()

with DAG(
    'gold_pipeline_master',
    default_args=default_args,
    description='Master pipeline for gold market intelligence data ingestion and transformation.',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gold', 'finance', 'dbt'],
) as dag:

    # Task 1: Ingest Raw Data from DBnomics
    ingest_task = PythonOperator(
        task_id='ingest_dbnomics_to_bronze',
        python_callable=run_ingestion,
    )

    # Task 2: Run dbt Transformations (Silver & Gold Layers)
    # Using BashOperator for dbt to allow environment-based target switching
    dbt_run_task = BashOperator(
        task_id='dbt_run_gold_marts',
        bash_command='cd gold_dbt && dbt run --target {{ var.value.get("DBT_TARGET", "dev") }}',
    )

    # Task 3: Run dbt Tests to ensure data quality
    dbt_test_task = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command='cd gold_dbt && dbt test --target {{ var.value.get("DBT_TARGET", "dev") }}',
    )

    # Define DAG Lineage
    ingest_task >> dbt_run_task >> dbt_test_task
