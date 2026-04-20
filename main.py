import os
import subprocess
import logging
import sys
from ingest_manager import GoldIngestor
from config import SERIES_MAP, EXCEL_MAP, YFINANCE_MAP

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
    logger.info("[START] GOLD INTELLIGENCE FRAMEWORK: FULL PIPELINE START")
    logger.info("======================================================")
    
    ingestor = GoldIngestor()
    
    try:
        # 1. Ingestion Phase
        logger.info("[PHASE 1] Ingestion (Bronze Layer)")
        
        # 1a. DBnomics API Data
        for sid, table in SERIES_MAP.items():
            ingestor.fetch_and_ingest(sid, table)

        # 1b. Excel File Data
        for table, (path, sheet) in EXCEL_MAP.items():
            ingestor.ingest_excel(path, table, sheet)

        # 1c. Yahoo Finance Data
        for table, symbol in YFINANCE_MAP.items():
            ingestor.fetch_yfinance(symbol, table)

    except Exception as e:
        logger.critical(f"[ERROR] Critical error during ingestion: {str(e)}")
        sys.exit(1)
    finally:
        ingestor.close()

    # 2. Transformation Phase (dbt Run)
    logger.info("[PHASE 2] Transformation (Silver/Gold Layers)")
    
    # Sicherstellen, dass die dbt-Abhängigkeiten aktuell sind und keine Altlasten vorhanden sind
    logger.info("Cleaning dbt artifacts and installing dependencies...")
    run_dbt_command(['dbt', 'clean'])
    run_dbt_command(['dbt', 'deps'])
    
    if not run_dbt_command(['dbt', 'run']):
        logger.error("[ERROR] dbt run failed. Aborting pipeline.")
        sys.exit(1)

    # 3. Validation Phase (dbt Test)
    logger.info("[PHASE 3] Validation (Data Quality Tests)")
    if not run_dbt_command(['dbt', 'test']):
        logger.warning("[WARNING] dbt tests encountered issues. Check data quality logs.")
    
    # 4. Final Summary
    logger.info("======================================================")
    logger.info("[SUCCESS] PIPELINE EXECUTION COMPLETE")
    logger.info("======================================================")
    logger.info("[REPORT] DATABASE SUMMARY:")
    logger.info(f"   - Database: {os.path.abspath('gold_dbt/data/gold_market.duckdb')}")
    logger.info("   - Schemas: bronze, main (dbt default)")
    logger.info("   - Key Marts: fct_market_summary, fct_gold_valuation_index")
    logger.info("")
    logger.info("[REPORT] DASHBOARD:")
    logger.info("   - Run: 'streamlit run dashboard.py'")
    logger.info("")
    logger.info("[REPORT] DOCUMENTATION SUMMARY:")
    logger.info("   - Architecture: README.md (Mermaid Diagram)")
    logger.info("   - Data Logic: Documented in SQL Headers & marts.yml")
    logger.info("   - dbt Docs: Run 'dbt docs generate && dbt docs serve' in gold_dbt/")
    logger.info("======================================================")
    logger.info("The Gold Intelligence Framework is ready for analysis.")

if __name__ == "__main__":
    main()
