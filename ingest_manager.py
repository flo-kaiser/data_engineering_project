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
        """Initializes the GoldIngestor with environment-specific clients and paths."""
        self.env = os.getenv('ENVIRONMENT', 'local').lower()
        self.db_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
        self.local_data_dir = 'data/bronze' 
        
        if self.env == 'local':
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.con = duckdb.connect(self.db_path)
            self._setup_warehouse()
            os.makedirs(self.local_data_dir, exist_ok=True)
            logger.info(f"Initialized GoldIngestor in 100% API LOCAL mode")
        else:
            key_path = os.getenv('GCP_KEYFILE_PATH')
            if key_path and os.path.exists(key_path):
                self.storage_client = storage.Client.from_service_account_json(key_path)
                self.bq_client = bigquery.Client.from_service_account_json(key_path)
                logger.info(f"Using service account key from: {key_path}")
            else:
                self.storage_client = storage.Client()
                self.bq_client = bigquery.Client()
                logger.info("Using default application credentials (ADC)")
                
            self.bucket_name = os.getenv('GCS_BUCKET_NAME')
            if not self.bucket_name:
                raise ValueError("GCS_BUCKET_NAME environment variable is required in PROD mode")
            self._setup_warehouse()
            logger.info(f"Initialized GoldIngestor in PROD mode (BQ: {self.bq_client.project})")

    def _setup_warehouse(self):
        """
        Sets up the warehouse by creating the metadata table.
        For local: creates bronze schema and table in DuckDB.
        For prod: creates table in BigQuery if it doesn't exist.
        """
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
        else:
            dataset_id = os.getenv('GCP_DATASET', 'gold_analytics')
            table_id = f"{self.bq_client.project}.{dataset_id}.ingestion_metadata"
            
            schema = [
                bigquery.SchemaField("source_id", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("last_updated", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("row_count", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("error_message", "STRING", mode="NULLABLE"),
            ]
            
            try:
                self.bq_client.get_table(table_id)
                logger.info(f"Table {table_id} already exists.")
            except Exception:
                logger.info(f"Table {table_id} not found, creating it...")
                table = bigquery.Table(table_id, schema=schema)
                self.bq_client.create_table(table)
                logger.info(f"Created table {table_id}")
                time.sleep(5) # Wait for BQ consistency

    def _fetch_with_retry(self, fetch_func: Callable, identifier: str, max_retries: int = 3, base_delay: int = 2):
        """
        Executes a fetch function with exponential backoff.

        Args:
            fetch_func: The function to execute.
            identifier: A string identifier for logging purposes.
            max_retries: Maximum number of retries before failing.
            base_delay: Initial delay in seconds between retries.

        Returns:
            The result of fetch_func if successful.

        Raises:
            Exception: If all retry attempts fail.
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
        """
        Fetches series from DBnomics and saves them to the target table.

        Args:
            series_id: The DBnomics series ID (e.g., 'IMF/IFS/M.W00.RAFAGOLDV_OZT').
            table_name: The name of the target table in bronze schema.
        """
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
        """
        Fetches historical data from Yahoo Finance and saves it to the target table.

        Args:
            symbol: The Yahoo Finance ticker symbol (e.g., 'GC=F').
            table_name: The name of the target table in bronze schema.
        """
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
        """
        Internal method to save the DataFrame to local Parquet/DuckDB or GCS/BigQuery.

        Args:
            df: The DataFrame to save.
            table_name: Target table name.
            source_id: Original source ID for metadata.
        """
        if self.env == 'local':
            # Use relative paths for portability between Host and Container
            # Path is relative to the project root
            file_path = os.path.join(self.local_data_dir, f"{table_name}.parquet")
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
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
        """Uploads a DataFrame as Parquet to GCS."""
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"bronze/{table_name}.parquet")
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
        return f"gs://{self.bucket_name}/bronze/{table_name}.parquet"

    def _load_to_bigquery(self, table_name: str, gcs_uri: str):
        """Loads a Parquet file from GCS into BigQuery (Truncate before Load)."""
        dataset_id = os.getenv('GCP_DATASET', 'gold_analytics')
        table_id = f"{dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        load_job = self.bq_client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
        load_job.result()
        logger.info(f"[BQ] Loaded {table_id}")

    def _update_metadata(self, source_id: str, table_name: str, row_count: int, status: str, error_msg: Optional[str] = None):
        """Updates the ingestion_metadata table with retry for BQ consistency."""
        now = datetime.now()
        if self.env == 'local':
            self.con.execute("INSERT INTO bronze.ingestion_metadata VALUES (?, ?, ?, ?, ?, ?)", 
                             [source_id, table_name, now, row_count, status, error_msg])
        else:
            dataset_id = os.getenv('GCP_DATASET', 'gold_analytics')
            table_id = f"{self.bq_client.project}.{dataset_id}.ingestion_metadata"
            rows = [{"source_id": source_id, "target_table": table_name, "last_updated": now.isoformat(), 
                     "row_count": row_count, "status": status, "error_message": error_msg}]
            
            # Simple retry for BQ insertion (eventual consistency)
            for attempt in range(3):
                try:
                    errors = self.bq_client.insert_rows_json(table_id, rows)
                    if not errors:
                        break
                    else:
                        logger.error(f"BQ Insert errors: {errors}")
                        break
                except Exception as e:
                    if attempt < 2:
                        logger.warning(f"Metadata update failed, retrying in 2s... ({str(e)})")
                        time.sleep(2)
                    else:
                        logger.error(f"Metadata update finally failed: {str(e)}")

    def close(self):
        """Closes the DuckDB connection."""
        if hasattr(self, 'con'):
            self.con.close()
