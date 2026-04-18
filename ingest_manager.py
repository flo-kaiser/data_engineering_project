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
    Environment-aware framework for ingesting market data from DBnomics and Excel.
    Supports local (Parquet/DuckDB) and Cloud (GCS/BigQuery) targets.
    
    Attributes:
        env (str): 'local' or 'prod' controlled via ENVIRONMENT env var.
        db_path (str): File path to the DuckDB database.
        bucket_name (str): GCS bucket name (prod only).
        local_data_dir (str): Root directory for local Parquet files.
    """

    def __init__(self):
        """
        Initializes the Ingestor based on environment variables.
        """
        self.env = os.getenv('ENVIRONMENT', 'local').lower()
        self.db_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
        self.bucket_name = os.getenv('GCS_BUCKET_NAME')
        self.local_data_dir = os.getenv('LOCAL_DATA_DIR', 'data/bronze')
        
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        self._setup_warehouse()

        if self.env == 'local':
            os.makedirs(self.local_data_dir, exist_ok=True)
            logger.info(f"Initialized GoldIngestor in LOCAL mode (Storage: {self.local_data_dir})")
        else:
            self.storage_client = storage.Client()
            logger.info(f"Initialized GoldIngestor in PROD mode (GCS: {self.bucket_name})")

    def _setup_warehouse(self):
        """Creates required schemas and metadata tracking tables."""
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
        """
        Fetches a series from DBnomics and performs an idempotent write.
        
        Args:
            series_id (str): The full DBnomics series identifier.
            table_name (str): The name of the target table/object.
        """
        logger.info(f"🚀 Starting API ingestion: {series_id} -> {table_name} ({self.env})")
        
        try:
            df = dbnomics.fetch_series(series_id)
            
            if df is None or df.empty:
                raise ValueError(f"Series {series_id} returned no data.")

            # Clean and prepare dataframe
            df_cleaned = df[['period', 'value']].copy()
            df_cleaned['period'] = pd.to_datetime(df_cleaned['period']).dt.date
            df_cleaned['source_id'] = series_id
            df_cleaned['ingested_at'] = datetime.now()

            self._save_data(df_cleaned, table_name, series_id)
            logger.info(f"✅ Successfully ingested {len(df_cleaned)} rows for {table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Ingestion failed for {series_id}: {error_msg}")
            self._update_metadata(series_id, table_name, 0, "FAILED", error_msg)

    def ingest_excel(self, file_path: str, table_name: str, sheet_name: any = 0):
        """
        Ingests a local Excel file into the bronze layer.
        
        Args:
            file_path (str): Path to the .xlsx file.
            table_name (str): Target table name.
            sheet_name (any): Sheet name or index.
        """
        logger.info(f"📂 Starting Excel ingestion: {file_path} -> {table_name} ({self.env})")
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
            df = df.astype(str)
            df.columns = [f'col_{i}' for i in range(len(df.columns))]
            df['source_file'] = os.path.basename(file_path)
            df['ingested_at'] = datetime.now()

            self._save_data(df, table_name, file_path)
            logger.info(f"✅ Successfully ingested Excel data for {table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Excel ingestion failed for {file_path}: {error_msg}")
            self._update_metadata(file_path, table_name, 0, "FAILED", error_msg)

    def fetch_yfinance(self, symbol: str, table_name: str):
        """
        Fetches historical data from Yahoo Finance and performs an idempotent write.
        
        Args:
            symbol (str): The Yahoo Finance ticker symbol.
            table_name (str): The name of the target table/object.
        """
        import yfinance as yf
        logger.info(f"📈 Starting YFinance ingestion: {symbol} -> {table_name} ({self.env})")
        
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="max")
            if df.empty:
                raise ValueError(f"No data returned for symbol {symbol}")
            
            df.reset_index(inplace=True)
            # Standardize date format
            df['Date'] = df['Date'].dt.date
            # Convert all other columns to string for bronze layer flexibility
            for col in df.columns:
                if col != 'Date':
                    df[col] = df[col].astype(str)
            
            df['source_id'] = symbol
            df['ingested_at'] = datetime.now()

            self._save_data(df, table_name, symbol)
            logger.info(f"✅ Successfully ingested {len(df)} rows for {table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ YFinance ingestion failed for {symbol}: {error_msg}")
            self._update_metadata(symbol, table_name, 0, "FAILED", error_msg)

    def _save_data(self, df: pd.DataFrame, table_name: str, source_id: str):
        """Saves dataframe to local Parquet or GCS based on environment."""
        if self.env == 'local':
            file_path = os.path.join(self.local_data_dir, f"{table_name}.parquet")
            df.to_parquet(file_path, index=False)
            # Create/Update DuckDB table as a view over the Parquet file for ease of use in dbt
            self.con.execute(f"CREATE OR REPLACE VIEW bronze.{table_name} AS SELECT * FROM read_parquet('{file_path}')")
            self._update_metadata(source_id, table_name, len(df), "SUCCESS")
        else:
            self._ingest_to_gcs(df, table_name)
            # In Prod, we assume dbt will handle BigQuery loading or external tables
            self._update_metadata(source_id, table_name, len(df), "SUCCESS")

    def _ingest_to_gcs(self, df: pd.DataFrame, table_name: str):
        """Cloud GCS ingestion as Parquet."""
        if not self.bucket_name:
            raise ValueError("GCS_BUCKET_NAME must be set for PROD environment.")
        
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(f"bronze/{table_name}.parquet")
        
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')

    def _update_metadata(self, source_id: str, table_name: str, row_count: int, status: str, error_msg: Optional[str] = None):
        """Updates the internal metadata table."""
        now = datetime.now()
        self.con.execute("""
            INSERT INTO bronze.ingestion_metadata 
            (source_id, target_table, last_updated, row_count, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [source_id, table_name, now, row_count, status, error_msg])

    def close(self):
        """Closes connections."""
        if hasattr(self, 'con'):
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
