import os
import subprocess
import logging
import sys
import time
import platform
import shutil
from dotenv import load_dotenv
from ingest_manager import GoldIngestor
from config import SERIES_MAP, EXCEL_MAP, YFINANCE_MAP

# --- Load Environment Variables ---
load_dotenv(override=True)
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
DBT_TARGET = os.getenv("DBT_TARGET", "dev")

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

def run_dbt_command(command_list, env_vars=None):
    """Executes a dbt command and logs the output. Returns (success, output)."""
    # Ensure dbt runs from the project root but uses the gold_dbt directory
    command_list.extend(['--project-dir', 'gold_dbt', '--profiles-dir', 'gold_dbt'])
    
    if DBT_TARGET:
        command_list.extend(['--target', DBT_TARGET])
        
    current_env = os.environ.copy()
    if env_vars:
        current_env.update(env_vars)
        
    logger.info(f"Running dbt command: {' '.join(command_list)}")
    try:
        # Run from root (cwd=None) instead of gold_dbt subfolder
        result = subprocess.run(
            command_list,
            cwd=None,
            capture_output=True,
            text=True,
            check=True,
            env=current_env
        )
        logger.info(result.stdout)
        return True, result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed with return code {e.returncode}")
        full_output = e.stdout + e.stderr
        logger.error(full_output)
        return False, full_output

def force_clean_dbt(package_dir):
    """Radically removes dbt artifacts."""
    logger.info("Force cleaning dbt target and packages...")
    
    # Aggressively remove target to avoid manifest issues
    target_path = os.path.join('gold_dbt', 'target')
    
    paths_to_clean = [target_path, package_dir]
    
    for p in paths_to_clean:
        if not p: continue
        try:
            if os.path.exists(p):
                if platform.system() == "Windows":
                    shutil.rmtree(p)
                else:
                    subprocess.run(['rm', '-rf', p], check=True)
        except Exception as e:
            logger.warning(f"Could not clean {p}: {str(e)}")
    
    time.sleep(1)
    return True

def main():
    logger.info("======================================================")
    logger.info(f"[START] GOLD INTELLIGENCE FRAMEWORK: {ENVIRONMENT.upper()} PIPELINE START")
    logger.info("======================================================")
    
    # Determine OS-specific dbt package path
    is_docker = os.path.exists('/.dockerenv')
    if (platform.system() != "Windows" and os.getcwd().startswith('/mnt/')) or is_docker:
        dbt_package_dir = "/tmp/dbt_packages_gold"
        logger.info(f"Environment detected as WSL mount or Docker. Using safe package path: {dbt_package_dir}")
    else:
        dbt_package_dir = "dbt_packages"
    
    dbt_env = {"DBT_PACKAGES_DIR": dbt_package_dir}
    
    ingestor = GoldIngestor()
    try:
        logger.info("[PHASE 1] Ingestion")
        for sid, table in SERIES_MAP.items():
            ingestor.fetch_and_ingest(sid, table)
        for table, symbol in YFINANCE_MAP.items():
            ingestor.fetch_yfinance(symbol, table)
    except Exception as e:
        logger.critical(f"Ingestion failed: {str(e)}")
        sys.exit(1)
    finally:
        ingestor.close()

    # Fixed: Force absolute paths for critical files
    key_path = os.getenv('GCP_KEYFILE_PATH', 'auth/service_account.json')
    if not os.path.isabs(key_path):
        os.environ['GCP_KEYFILE_PATH'] = os.path.abspath(key_path)

    duckdb_path = os.getenv('DUCKDB_PATH', 'gold_dbt/data/gold_market.duckdb')
    if not os.path.isabs(duckdb_path):
        os.environ['DUCKDB_PATH'] = os.path.abspath(duckdb_path)
        logger.info(f"Converted DUCKDB_PATH to absolute: {os.environ['DUCKDB_PATH']}")

    logger.info("[PHASE 2] Transformation")
    
    # ALWAYS clean target in PROD to avoid "found more than one package" errors
    force_clean_dbt(dbt_package_dir)
    
    if not run_dbt_command(['dbt', 'deps'], env_vars=dbt_env)[0]:
        logger.error("dbt deps failed.")
        sys.exit(1)
        
    if not run_dbt_command(['dbt', 'run'], env_vars=dbt_env)[0]:
        logger.error("dbt run failed.")
        sys.exit(1)

    logger.info("[PHASE 3] Validation")
    run_dbt_command(['dbt', 'test'], env_vars=dbt_env)
    
    logger.info("======================================================")
    logger.info("[SUCCESS] PIPELINE EXECUTION COMPLETE")
    logger.info("======================================================")

if __name__ == "__main__":
    main()
