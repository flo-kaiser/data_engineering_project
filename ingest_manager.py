import os
import pandas as pd
import dbnomics
import duckdb
import logging
import io
from datetime import datetime
from typing import Dict, Optional
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
    Handles data from DBnomics, Yahoo Finance, and other APIs.
    """

    def __init__(self):
        self.env = os.getenv('ENVIRONMENT', 'local').lower()
        self.db_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
        self.local_data_dir = os.getenv('LOCAL_DATA_DIR', 'data/bronze')
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        self._setup_warehouse()

        if self.env == 'local':
            os.makedirs(self.local_data_dir, exist_ok=True)
            logger.info(f"Initialized GoldIngestor in 100% API LOCAL mode")
        else:
            self.storage_client = storage.Client()
            self.bq_client = bigquery.Client()
            logger.info(f"Initialized GoldIngestor in PROD mode")

    def _setup_warehouse(self):
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

    def fetch_and_ingest(self, series_id: str, table_name: str):
        logger.info(f"[API] Fetching: {series_id} -> {table_name}")
        try:
            df = dbnomics.fetch_series(series_id)
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
            df = ticker.history(period="max")
            df.reset_index(inplace=True)
            df['Date'] = df['Date'].dt.date
            df['source_id'] = symbol
            df['ingested_at'] = datetime.now()
            self._save_data(df, table_name, symbol)
            logger.info(f"[SUCCESS] Ingested {len(df)} rows")
        except Exception as e:
            logger.error(f"[ERROR] YFinance failed: {str(e)}")

    def _save_data(self, df: pd.DataFrame, table_name: str, source_id: str):
        if self.env == 'local':
            file_path = os.path.abspath(os.path.join(self.local_data_dir, f"{table_name}.parquet"))
            df.to_parquet(file_path, index=False)
            self.con.execute(f"DROP VIEW IF EXISTS bronze.{table_name}")
            self.con.execute(f"CREATE VIEW bronze.{table_name} AS SELECT * FROM read_parquet('{file_path}')")
            self._update_metadata(source_id, table_name, len(df), "SUCCESS")
        else:
            # Prod logic...
            pass

    def _update_metadata(self, source_id, table_name, row_count, status, error_msg=None):
        self.con.execute("INSERT INTO bronze.ingestion_metadata VALUES (?, ?, ?, ?, ?, ?)", 
                         [source_id, table_name, datetime.now(), row_count, status, error_msg])

    def close(self):
        self.con.close()
