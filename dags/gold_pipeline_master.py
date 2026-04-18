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
    from ingest_manager import GoldIngestor
    from config import SERIES_MAP, EXCEL_MAP, YFINANCE_MAP

    default_args = {
    ...
    def run_api_ingestion():
        """Ingests data from DBnomics APIs."""
        ingestor = GoldIngestor()
        try:
            for sid, table in SERIES_MAP.items():
                ingestor.fetch_and_ingest(sid, table)
        finally:
            ingestor.close()

    def run_excel_ingestion():
        """Ingests market fundamental data from Excel files."""
        ingestor = GoldIngestor()
        try:
            for table, (path, sheet) in EXCEL_MAP.items():
                ingestor.ingest_excel(path, table, sheet)
        finally:
            ingestor.close()

    def run_indicator_ingestion():
        """Ingests macro indicators from Yahoo Finance."""
        ingestor = GoldIngestor()
        try:
            for table, symbol in YFINANCE_MAP.items():
                ingestor.fetch_yfinance(symbol, table)
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

    # Task 1: Ingest API Data
    task_ingest_api = PythonOperator(
        task_id='ingest_api_data',
        python_callable=run_api_ingestion,
    )

    # Task 2: Ingest Excel Data
    task_ingest_excel = PythonOperator(
        task_id='ingest_excel_data',
        python_callable=run_excel_ingestion,
    )

    # Task 3: Ingest Market Indicators
    task_ingest_indicators = PythonOperator(
        task_id='ingest_market_indicators',
        python_callable=run_indicator_ingestion,
    )

    # Task 4: dbt Run (Silver & Gold Layers)
    task_dbt_run = BashOperator(
        task_id='dbt_run_gold_marts',
        bash_command='cd gold_dbt && dbt run --target {{ var.value.get("DBT_TARGET", "dev") }}',
    )

    # Task 5: dbt Test
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command='cd gold_dbt && dbt test --target {{ var.value.get("DBT_TARGET", "dev") }}',
    )

    # Define DAG Lineage (Parallel Ingestion -> Sequential Transformation)
    [task_ingest_api, task_ingest_excel, task_ingest_indicators] >> task_dbt_run >> task_dbt_test
