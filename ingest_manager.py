import os
import pandas as pd
import dbnomics
import duckdb
import logging
import io
import time
from datetime import datetime
from typing import Dict, Optional, Callable
from google.cloud import storage, bigquery
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()

# --- Configuration & Setup ---
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GoldIngestor")

class GoldIngestor:
    """
    100% API-BASED INGESTOR.
    Supports Local (DuckDB) and Cloud (GCS/BigQuery).
    Now with built-in retry logic for production robustness.
    """

    def __init__(self):
        self.env = os.getenv('ENVIRONMENT', 'local').lower()
        self.db_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
        # Use a path that is predictable for both host and container
        self.local_data_dir = 'data/bronze' 
        
        if self.env == 'local':
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.con = duckdb.connect(self.db_path)
            self._setup_warehouse()
            os.makedirs(self.local_data_dir, exist_ok=True)
            logger.info(f"Initialized GoldIngestor in 100% API LOCAL mode")
        else:
            self.storage_client = storage.Client()
            self.bq_client = bigquery.Client()
            logger.info(f"Initialized GoldIngestor in PROD mode (BQ: {os.getenv('GCP_PROJECT_ID')})")

    def _setup_warehouse(self):
        """DuckDB only - BigQuery schemas are assumed to exist."""
        if self.env == 'local':
            self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            self.con.execute("""
                CREATE TABLE IF NOT EXISTS bronze.ingestion_metadata (
                    source_id VARCHAR,
                    target_table VARCHAR,
                    last_updated TIMESTAMP,
                    row_count INTEGER,
                    status VARCHAR,
                    error_message VARCHAR
                )
            """)

    def _fetch_with_retry(self, fetch_func: Callable, identifier: str, max_retries: int = 3, base_delay: int = 2):
        """
        Helper to execute a fetch function with exponential backoff.
        """
        for attempt in range(max_retries):
            try:
                return fetch_func()
            except Exception as e:
                wait_time = base_delay * (2 ** attempt)
                if attempt < max_retries - 1:
                    logger.warning(f"[RETRY] Attempt {attempt+1} failed for {identifier}. Waiting {wait_time}s... Error: {str(e)}")
                    time.sleep(wait_time)
                else:
                    logger.error(f"[FATAL] All {max_retries} attempts failed for {identifier}")
                    raise e

    def fetch_and_ingest(self, series_id: str, table_name: str):
        logger.info(f"[API] Fetching: {series_id} -> {table_name}")
        try:
            df = self._fetch_with_retry(lambda: dbnomics.fetch_series(series_id), series_id)
            
            if df is None or df.empty:
                raise ValueError(f"No data for {series_id}")

            df_cleaned = df[['period', 'value']].copy()
            df_cleaned['period'] = pd.to_datetime(df_cleaned['period']).dt.date
            df_cleaned['source_id'] = series_id
            df_cleaned['ingested_at'] = datetime.now()

            self._save_data(df_cleaned, table_name, series_id)
            logger.info(f"[SUCCESS] Ingested {len(df_cleaned)} rows")

        except Exception as e:
            logger.error(f"[ERROR] API failed: {str(e)}")
            self._update_metadata(series_id, table_name, 0, "FAILED", str(e))

    def fetch_yfinance(self, symbol: str, table_name: str):
        import yfinance as yf
        logger.info(f"[YFINANCE] Fetching: {symbol} -> {table_name}")
        try:
            ticker = yf.Ticker(symbol)
            df = self._fetch_with_retry(lambda: ticker.history(period="max"), symbol)
            
            if df is None or df.empty:
                 raise ValueError(f"No data for {symbol}")

            df.reset_index(inplace=True)
            df['Date'] = df['Date'].dt.date
            df['source_id'] = symbol
            df['ingested_at'] = datetime.now()
            self._save_data(df, table_name, symbol)
            logger.info(f"[SUCCESS] Ingested {len(df)} rows")
        except Exception as e:
            logger.error(f"[ERROR] YFinance failed: {str(e)}")
            self._update_metadata(symbol, table_name, 0, "FAILED", str(e))

    def _save_data(self, df: pd.DataFrame, table_name: str, source_id: str):
        if self.env == 'local':
            # Use absolute paths so dbt (running in a subfolder) can always find the files
            file_path = os.path.abspath(os.path.join(self.local_data_dir, f"{table_name}.parquet"))
            df.to_parquet(file_path, index=False)
            self.con.execute(f"DROP VIEW IF EXISTS bronze.{table_name}")
            self.con.execute(f"CREATE VIEW bronze.{table_name} AS SELECT * FROM read_parquet('{file_path}')")
            self._update_metadata(source_id, table_name, len(df), "SUCCESS")
        else:
            # Cloud: 1. GCS, 2. BigQuery
            gcs_uri = self._ingest_to_gcs(df, table_name)
            self._load_to_bigquery(table_name, gcs_uri)
            self._update_metadata(source_id, table_name, len(df), "SUCCESS")

    def _ingest_to_gcs(self, df: pd.DataFrame, table_name: str) -> str:
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"bronze/{table_name}.parquet")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
        return f"gs://{self.bucket_name}/bronze/{table_name}.parquet"

    def _load_to_bigquery(self, table_name: str, gcs_uri: str):
        dataset_id = os.getenv('GCP_DATASET', 'gold_analytics')
        table_id = f"{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = self.bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"[BQ] Loaded {table_id}")

    def _update_metadata(self, source_id, table_name, row_count, status, error_msg=None):
        now = datetime.now()
        if self.env == 'local':
            self.con.execute("INSERT INTO bronze.ingestion_metadata VALUES (?, ?, ?, ?, ?, ?)", 
                             [source_id, table_name, now, row_count, status, error_msg])
        else:
            dataset_id = os.getenv('GCP_DATASET', 'gold_analytics')
            table_id = f"{self.bq_client.project}.{dataset_id}.ingestion_metadata"
            rows = [{"source_id": source_id, "target_table": table_name, "last_updated": now.isoformat(), 
                     "row_count": row_count, "status": status, "error_message": error_msg}]
            self.bq_client.insert_rows_json(table_id, rows)

    def close(self):
        if hasattr(self, 'con'):
            self.con.close()
