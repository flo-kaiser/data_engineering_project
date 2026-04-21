from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Pfad-Anpassung für Importe im Docker-Container
sys.path.insert(0, '/app')

def run_api_ingestion():
    """Ingests data from DBnomics APIs."""
    from ingest_manager import GoldIngestor
    from config import SERIES_MAP
    ingestor = GoldIngestor()
    try:
        for sid, table in SERIES_MAP.items():
            ingestor.fetch_and_ingest(sid, table)
    finally:
        ingestor.close()

def run_indicator_ingestion():
    """Ingests macro indicators from Yahoo Finance."""
    from ingest_manager import GoldIngestor
    from config import YFINANCE_MAP
    ingestor = GoldIngestor()
    try:
        for table, symbol in YFINANCE_MAP.items():
            ingestor.fetch_yfinance(symbol, table)
    finally:
        ingestor.close()

default_args = {
    'owner': 'Gold Analytics Team',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gold_pipeline_master',
    default_args=default_args,
    description='Master pipeline for gold market intelligence data ingestion and transformation.',
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'finance', 'dbt', 'api-driven'],
) as dag:

    # Task 1: Ingest API Data (Bronze Layer)
    task_ingest_api = PythonOperator(
        task_id='ingest_api_data',
        python_callable=run_api_ingestion,
    )

    # Task 2: Ingest Yahoo Finance Data (Bronze Layer)
    task_ingest_indicators = PythonOperator(
        task_id='ingest_market_indicators',
        python_callable=run_indicator_ingestion,
    )

    # Task 3: dbt Run (Silver & Gold Layers)
    # Nutzt uv run dbt für Umgebungstreue
    dbt_target = os.getenv('DBT_TARGET', 'dev')
    task_dbt_run = BashOperator(
        task_id='dbt_run_gold_marts',
        bash_command=f'cd /app && uv run dbt run --project-dir gold_dbt --profiles-dir gold_dbt --target {dbt_target}',
    )

    # Task 4: dbt Test (Data Quality)
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command=f'cd /app && uv run dbt test --project-dir gold_dbt --profiles-dir gold_dbt --target {dbt_target}',
    )

    # Lineage: Sequential Ingestion to prevent DuckDB locking issues
    task_ingest_api >> task_ingest_indicators >> task_dbt_run >> task_dbt_test
