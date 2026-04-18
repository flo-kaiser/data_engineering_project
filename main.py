import os
import subprocess
import logging
import sys
from ingest_manager import GoldIngestor

# --- Setup Logging ---
LOG_DIR = 'logs'
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(f"{LOG_DIR}/pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GoldPipelineOrchestrator")

def run_dbt_command(command_list):
    """Executes a dbt command and logs the output."""
    logger.info(f"Running dbt command: {' '.join(command_list)}")
    try:
        # We need to run dbt from within the gold_dbt directory
        result = subprocess.run(
            command_list,
            cwd='gold_dbt',
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed with return code {e.returncode}")
        logger.error(e.stdout)
        logger.error(e.stderr)
        return False

def main():
    """
    Main orchestration loop for the Gold Intelligence Framework.
    Sequentially executes: Ingestion -> Transformation -> Validation -> Reporting.
    """
    logger.info("======================================================")
    logger.info("🚀 GOLD INTELLIGENCE FRAMEWORK: FULL PIPELINE START")
    logger.info("======================================================")
    
    # 1. Ingestion Phase
    logger.info("--- Phase 1: Ingestion (Bronze Layer) ---")
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
    except Exception as e:
        logger.critical(f"❌ Critical error during ingestion: {str(e)}")
        sys.exit(1)
    finally:
        ingestor.close()

    # 2. Transformation Phase (dbt Run)
    logger.info("--- Phase 2: Transformation (Silver/Gold Layers) ---")
    if not run_dbt_command(['dbt', 'run']):
        logger.error("❌ dbt run failed. Aborting pipeline.")
        sys.exit(1)

    # 3. Validation Phase (dbt Test)
    logger.info("--- Phase 3: Validation (Data Quality Tests) ---")
    if not run_dbt_command(['dbt', 'test']):
        logger.warning("⚠️ dbt tests encountered issues. Check data quality logs.")
    
    # 4. Final Summary
    logger.info("======================================================")
    logger.info("✅ PIPELINE EXECUTION COMPLETE")
    logger.info("======================================================")
    logger.info("📊 DATABASE SUMMARY:")
    logger.info(f"   - Database: {os.path.abspath('gold_dbt/data/gold_market.duckdb')}")
    logger.info("   - Schemas: bronze, main (dbt default)")
    logger.info("   - Key Marts: fct_market_summary, fct_gold_valuation_index")
    logger.info("")
    logger.info("🖥️  DASHBOARD:")
    logger.info("   - Run: 'streamlit run dashboard.py'")
    logger.info("")
    logger.info("📜 DOCUMENTATION SUMMARY:")
    logger.info("   - Architecture: README.md (Mermaid Diagram)")
    logger.info("   - Data Logic: Documented in SQL Headers & marts.yml")
    logger.info("   - dbt Docs: Run 'dbt docs generate && dbt docs serve' in gold_dbt/")
    logger.info("======================================================")
    logger.info("The Gold Intelligence Framework is ready for analysis.")

if __name__ == "__main__":
    main()
