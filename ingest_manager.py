import pandas as pd
import dbnomics
import duckdb
import logging
import os
from datetime import datetime

# Setup logging
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{log_dir}/ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DBnomicsIngestor:
    """
    Framework for ingesting market data from DBnomics into DuckDB.
    
    Attributes:
        db_path (str): Path to the DuckDB database file.
        con (duckdb.DuckDBPyConnection): Connection object to DuckDB.
    """

    def __init__(self, db_path='gold_dbt/data/gold_market.duckdb'):
        """
        Initializes the ingestor and creates the bronze schema if not exists.
        
        Args:
            db_path (str): The path to the DuckDB database. Defaults to 'gold_dbt/data/gold_market.duckdb'.
        """
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.con = duckdb.connect(self.db_path)
        self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self._init_metadata_table()

    def _init_metadata_table(self):
        """Creates a metadata table to track the last ingestion for each series."""
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS bronze.ingestion_metadata (
                series_id VARCHAR,
                last_updated TIMESTAMP,
                status VARCHAR
            )
        """)

    def fetch_and_ingest(self, series_id, table_name):
        """
        Fetches a time series from DBnomics and writes it to a DuckDB table.
        
        Args:
            series_id (str): The DBnomics series ID (e.g., 'WB/commodity_prices/FGOLD-1W').
            table_name (str): The target table name in the 'bronze' schema.
        """
        logger.info(f"Starting ingestion for series: {series_id} -> bronze.{table_name}")
        
        try:
            # Fetch data using dbnomics
            df = dbnomics.fetch_series(series_id)
            
            if df is None or df.empty:
                logger.warning(f"No data returned for series {series_id}")
                return

            # Basic transformation: select relevant columns and normalize names
            # DBnomics usually returns 'period' for date and 'value' for the data point
            df = df[['period', 'value']].copy()
            df['period'] = pd.to_datetime(df['period']).dt.strftime('%Y-%m-%d')
            df['series_id'] = series_id
            
            # Idempotency / Upsert Logic:
            # We use a temporary table and then insert only new or changed records
            # For simplicity in this bronze layer, we replace the table but 
            # we could implement a true delta load if needed. 
            # Requirement says "Idempotenz: Mehrfache Ausführungen dürfen keine Dubletten erzeugen".
            # Overwriting the table for a specific series is idempotent.
            
            self.con.register('tmp_df', df)
            self.con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM tmp_df")
            self.con.unregister('tmp_df')
            
            # Update metadata
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.con.execute("""
                INSERT INTO bronze.ingestion_metadata (series_id, last_updated, status)
                VALUES (?, ?, ?)
            """, [series_id, now, 'SUCCESS'])
            
            logger.info(f"Successfully ingested {len(df)} rows into bronze.{table_name}")
            
        except Exception as e:
            logger.error(f"Failed to ingest {series_id}: {str(e)}")
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.con.execute("""
                INSERT INTO bronze.ingestion_metadata (series_id, last_updated, status)
                VALUES (?, ?, ?)
            """, [series_id, now, f'FAILED: {str(e)}'])

    def close(self):
        """Closes the DuckDB connection."""
        if self.con:
            self.con.close()
            logger.info("DuckDB connection closed.")

if __name__ == "__main__":
    # Test ingestion for the requested series
    ingestor = DBnomicsIngestor()
    
    series_map = {
        'WB/commodity_prices/FGOLD-1W': 'gold_prices_api',
        'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
        'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
        'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api'
    }
    
    for sid, table in series_map.items():
        ingestor.fetch_and_ingest(sid, table)
    
    ingestor.close()
