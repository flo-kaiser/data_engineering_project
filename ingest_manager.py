import os
import pandas as pd
import dbnomics
import duckdb
import logging
import io
from datetime import datetime
from typing import Dict, Optional
from google.cloud import storage
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
    Environment-aware framework for ingesting market data from DBnomics.
    Supports local (DuckDB) and Cloud (GCS/BigQuery) targets.
    
    Attributes:
        env (str): 'local' or 'prod' controlled via ENVIRONMENT env var.
        db_path (str): File path to the DuckDB database (local only).
        bucket_name (str): GCS bucket name (prod only).
    """

    def __init__(self):
        """
        Initializes the Ingestor based on environment variables.
        """
        self.env = os.getenv('ENVIRONMENT', 'local').lower()
        self.db_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        
        if self.env == 'local':
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            self.con = duckdb.connect(self.db_path)
            self._setup_duckdb()
            logger.info(f"Initialized GoldIngestor in LOCAL mode (DuckDB: {self.db_path})")
        else:
            self.storage_client = storage.Client()
            logger.info(f"Initialized GoldIngestor in PROD mode (GCS: {self.bucket_name})")

    def _setup_duckdb(self):
        """Creates required schemas and metadata tracking tables for DuckDB."""
        self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bronze.ingestion_metadata (
                series_id VARCHAR,
                target_table VARCHAR,
                last_updated TIMESTAMP,
                row_count INTEGER,
                status VARCHAR,
                error_message VARCHAR
            )
        """)

    def fetch_and_ingest(self, series_id: str, table_name: str):
        """
        Fetches a series from DBnomics and performs an idempotent write.
        
        Args:
            series_id (str): The full DBnomics series identifier.
            table_name (str): The name of the target table/object.
        """
        logger.info(f"🚀 Starting ingestion: {series_id} -> {table_name} ({self.env})")
        
        try:
            df = dbnomics.fetch_series(series_id)
            
            if df is None or df.empty:
                raise ValueError(f"Series {series_id} returned no data.")

            # Clean and prepare dataframe
            df_cleaned = df[['period', 'value']].copy()
            df_cleaned['period'] = pd.to_datetime(df_cleaned['period']).dt.date
            df_cleaned['series_id'] = series_id
            df_cleaned['ingested_at'] = datetime.now()

            if self.env == 'local':
                self._ingest_to_duckdb(df_cleaned, table_name, series_id)
            else:
                self._ingest_to_gcs(df_cleaned, table_name)
            
            logger.info(f"✅ Successfully ingested {len(df_cleaned)} rows for {table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Ingestion failed for {series_id}: {error_msg}")
            if self.env == 'local':
                self._update_metadata(series_id, table_name, 0, "FAILED", error_msg)

    def _ingest_to_duckdb(self, df: pd.DataFrame, table_name: str, series_id: str):
        """Local DuckDB ingestion with Upsert logic."""
        self.con.register('tmp_df', df)
        self.con.execute(f"CREATE TABLE IF NOT EXISTS bronze.{table_name} AS SELECT * FROM tmp_df WHERE 1=0")
        self.con.execute(f"DELETE FROM bronze.{table_name} WHERE period IN (SELECT period FROM tmp_df)")
        self.con.execute(f"INSERT INTO bronze.{table_name} SELECT * FROM tmp_df")
        self.con.unregister('tmp_df')
        self._update_metadata(series_id, table_name, len(df), "SUCCESS")

    def _ingest_to_gcs(self, df: pd.DataFrame, table_name: str):
        """Cloud GCS ingestion as Parquet."""
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME must be set for PROD environment.")
        
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"bronze/{table_name}.parquet")
        
        # Buffer to hold parquet data
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')

    def _update_metadata(self, series_id: str, table_name: str, row_count: int, status: str, error_msg: Optional[str] = None):
        """Updates the internal metadata table in DuckDB."""
        now = datetime.now()
        self.con.execute("""
            INSERT INTO bronze.ingestion_metadata 
            (series_id, target_table, last_updated, row_count, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [series_id, table_name, now, row_count, status, error_msg])

    def close(self):
        """Closes connections."""
        if self.env == 'local' and hasattr(self, 'con'):
            self.con.close()

if __name__ == "__main__":
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
