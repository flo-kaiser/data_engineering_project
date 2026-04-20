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
load_dotenv()
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
    if DBT_TARGET:
        command_list.extend(['--target', DBT_TARGET])
        
    # Merge current env with custom env vars
    current_env = os.environ.copy()
    if env_vars:
        current_env.update(env_vars)
        
    logger.info(f"Running dbt command: {' '.join(command_list)}")
    try:
        result = subprocess.run(
            command_list,
            cwd='gold_dbt',
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
    """Radically removes dbt artifacts depending on the OS."""
    logger.info(f"Force cleaning dbt artifacts (Package Dir: {package_dir})...")
    
    paths_to_clean = [
        os.path.join('gold_dbt', 'target'),
        os.path.join('gold_dbt', 'dbt_packages'),
        package_dir
    ]
    
    for p in paths_to_clean:
        if not p: continue
        try:
            if os.path.exists(p):
                if platform.system() == "Windows":
                    if os.path.isdir(p): shutil.rmtree(p)
                    else: os.remove(p)
                else:
                    subprocess.run(['rm', '-rf', p], check=True)
        except Exception as e:
            logger.warning(f"Could not clean path {p}: {str(e)}")
    
    time.sleep(1)
    return True

def main():
    """
    Main orchestration loop for the Gold Intelligence Framework.
    """
    logger.info("======================================================")
    logger.info(f"[START] GOLD INTELLIGENCE FRAMEWORK: {ENVIRONMENT.upper()} PIPELINE START")
    logger.info("======================================================")
    
    # Determine OS-specific dbt package path
    # WSL/Linux on mounted drive: use /tmp to avoid locks. Windows: use local dir.
    if platform.system() != "Windows" and os.getcwd().startswith('/mnt/'):
        dbt_package_dir = "/tmp/dbt_packages_gold"
    else:
        dbt_package_dir = "dbt_packages" # relative to gold_dbt/
    
    dbt_env = {"DBT_PACKAGES_DIR": dbt_package_dir}
    
    ingestor = GoldIngestor()
    
    try:
        # 1. Ingestion Phase
        logger.info("[PHASE 1] Ingestion (Bronze Layer)")
        for sid, table in SERIES_MAP.items():
            ingestor.fetch_and_ingest(sid, table)
        for table, symbol in YFINANCE_MAP.items():
            ingestor.fetch_yfinance(symbol, table)
    except Exception as e:
        logger.critical(f"[ERROR] Critical error during ingestion: {str(e)}")
        sys.exit(1)
    finally:
        ingestor.close()

    # 2. Transformation Phase (dbt Run)
    logger.info("[PHASE 2] Transformation (Silver/Gold Layers)")
    
    max_attempts = 3
    for attempt in range(max_attempts):
        logger.info(f"dbt execution attempt {attempt + 1}/{max_attempts}...")
        
        # In PROD/WSL or on retry, we ensure a clean state
        package_check_path = dbt_package_dir if dbt_package_dir.startswith('/') else os.path.join('gold_dbt', dbt_package_dir)
        
        if not os.path.exists(package_check_path) or attempt > 0:
            force_clean_dbt(package_check_path)
            success_deps, _ = run_dbt_command(['dbt', 'deps'], env_vars=dbt_env)
            if not success_deps:
                logger.warning("dbt deps failed. Retrying...")
                time.sleep(2)
                continue

        # Try dbt run
        success_run, run_output = run_dbt_command(['dbt', 'run'], env_vars=dbt_env)
        
        if success_run:
            logger.info("[SUCCESS] dbt run completed.")
            break
        else:
            if "found more than one package with the name" in run_output or "Permission denied" in run_output:
                logger.warning("[RECOVERY] Detected dbt package/filesystem issues. Retrying with force clean...")
                continue
            else:
                logger.error("[ERROR] dbt run failed. Aborting.")
                sys.exit(1)
    else:
        logger.error("[FATAL] Could not resolve dbt issues after multiple attempts.")
        sys.exit(1)

    # 3. Validation Phase (dbt Test)
    logger.info("[PHASE 3] Validation (Data Quality Tests)")
    run_dbt_command(['dbt', 'test'], env_vars=dbt_env)
    
    # 4. Final Summary
    logger.info("======================================================")
    logger.info("[SUCCESS] PIPELINE EXECUTION COMPLETE")
    logger.info("======================================================")

if __name__ == "__main__":
    main()
