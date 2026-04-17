import pandas as pd
import dbnomics
import duckdb
import logging
import os
from datetime import datetime
from typing import Dict, Optional

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
logger = logging.getLogger("DBnomicsIngestor")

class DBnomicsIngestor:
    """
    Enterprise-grade framework for ingesting market data from DBnomics into DuckDB.
    
    This class handles the connection to DuckDB, schema creation, metadata tracking,
    and idempotent ingestion of time-series data.
    
    Attributes:
        db_path (str): File path to the DuckDB database.
        con (duckdb.DuckDBPyConnection): Active connection to DuckDB.
    """

    def __init__(self, db_path: str = 'gold_dbt/data/gold_market.duckdb'):
        """
        Initializes the Ingestor and ensures the environment is ready.
        
        Args:
            db_path: Path to the DuckDB database file.
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        self._setup_database()
        logger.info(f"Initialized DBnomicsIngestor with database: {self.db_path}")

    def _setup_database(self):
        """Creates required schemas and metadata tracking tables."""
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
        Fetches a series from DBnomics and performs an idempotent write to DuckDB.
        
        Args:
            series_id: The full DBnomics series identifier (Provider/Dataset/Series).
            table_name: The name of the target table within the 'bronze' schema.
            
        Raises:
            Exception: If ingestion fails, details are logged to both file and metadata table.
        """
        logger.info(f"🚀 Starting ingestion: {series_id} -> bronze.{table_name}")
        start_time = datetime.now()
        
        try:
            # Fetch data from DBnomics
            df = dbnomics.fetch_series(series_id)
            
            if df is None or df.empty:
                raise ValueError(f"Series {series_id} returned no data.")

            # Clean and prepare dataframe
            df_cleaned = df[['period', 'value']].copy()
            df_cleaned['period'] = pd.to_datetime(df_cleaned['period']).dt.date
            df_cleaned['series_id'] = series_id
            df_cleaned['ingested_at'] = datetime.now()

            # Idempotent Write: We use CREATE OR REPLACE for the raw bronze layer
            # This ensures that we have a clean, deduplicated copy of the latest API state.
            self.con.register('tmp_df', df_cleaned)
            self.con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM tmp_df")
            self.con.unregister('tmp_df')
            
            row_count = len(df_cleaned)
            self._update_metadata(series_id, table_name, row_count, "SUCCESS")
            logger.info(f"✅ Successfully ingested {row_count} rows into bronze.{table_name}")

        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Ingestion failed for {series_id}: {error_msg}")
            self._update_metadata(series_id, table_name, 0, "FAILED", error_msg)

    def _update_metadata(self, series_id: str, table_name: str, row_count: int, status: str, error_msg: Optional[str] = None):
        """Updates the internal metadata table with the status of the last run."""
        now = datetime.now()
        self.con.execute("""
            INSERT INTO bronze.ingestion_metadata 
            (series_id, target_table, last_updated, row_count, status, error_message)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [series_id, table_name, now, row_count, status, error_msg])

    def close(self):
        """Releases the database connection."""
        if self.con:
            self.con.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    # Define the core API series for the Gold Intelligence Framework
    series_map = {
        'WB/commodity_prices/FGOLD-1W': 'gold_prices_api',
        'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
        'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
        'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api'
    }
    
    ingestor = DBnomicsIngestor()
    try:
        for sid, table in series_map.items():
            ingestor.fetch_and_ingest(sid, table)
    finally:
        ingestor.close()
