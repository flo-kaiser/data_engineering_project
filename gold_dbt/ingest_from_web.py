import os
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import time
import pandas as pd
import duckdb
from dbnomics import fetch_series

# Load credentials
load_dotenv()
EMAIL = os.getenv('GOLD_ORG_EMAIL')
PASSWORD = os.getenv('GOLD_ORG_PASSWORD')

# Proprietary data that is HARD to get from DBnomics
GOLD_ORG_URLS = [
    "https://www.gold.org/download/file/20717/ETF_Flows_March_2026.xlsx",
    "https://www.gold.org/download/file/7588/above-ground-gold-stocks.xlsx?referrerNid=7160",
    "https://www.gold.org/download/file/7593/Gold-Mining-Production-Volumes-Data-2025.xlsx?referrerNid=7323",
    "https://www.gold.org/download/file/20499/GDT_Tables_Q425_EN.xlsx?referrerNid=7329",
    "https://www.gold.org/download/file/11657/gold-premiums.xlsx?referrerNid=6533"
]

# DBnomics Series IDs
# WB/commodity_prices/FGOLD-1W: Gold Price (World Bank Pink Sheet)
# IMF/IFS/M.W00.RAFAGOLDV_OZT: Gold Reserves Volume (IMF World Aggregate)
DBNOMICS_SERIES = {
    'gold_prices': 'WB/commodity_prices/FGOLD-1W',
    'central_bank_holdings': 'IMF/IFS/M.W00.RAFAGOLDV_OZT'
}

LOGIN_URL = "https://www.gold.org/user/login"
XLS_DIR = "xls"
DB_PATH = 'gold_dbt/data/gold_market.duckdb'

def download_gold_org():
    print("--- 1. Download von proprietären Daten (gold.org) ---")
    if not EMAIL or not PASSWORD:
        print("Hinweis: Keine Zugangsdaten für gold.org gefunden. Überspringe Downloads.")
        return

    os.makedirs(XLS_DIR, exist_ok=True)
    scraper = cloudscraper.create_scraper(browser={'browser': 'chrome','platform': 'windows','mobile': False})
    
    try:
        res = scraper.get(LOGIN_URL)
        soup = BeautifulSoup(res.text, 'html.parser')
        form_build_id = soup.find('input', {'name': 'form_build_id'})['value'] if soup.find('input', {'name': 'form_build_id'}) else ""
        login_data = {'name': EMAIL, 'pass': PASSWORD, 'form_build_id': form_build_id, 'form_id': 'user_login_form', 'op': 'Log in'}
        scraper.post(LOGIN_URL, data=login_data, allow_redirects=True)
    except Exception as e:
        print(f"Login fehlgeschlagen: {e}")

    for url in GOLD_ORG_URLS:
        filename = url.split('/')[-1].split('?')[0]
        path = os.path.join(XLS_DIR, filename)
        print(f"Versuche Download: {filename}...")
        try:
            r = scraper.get(url)
            if r.status_code == 200:
                with open(path, 'wb') as f:
                    f.write(r.content)
                print(f"Erfolgreich: {path}")
            else:
                print(f"Blockiert (Status {r.status_code}). Bitte manuell in '{XLS_DIR}' legen.")
        except Exception as e:
            print(f"Fehler: {e}")
        time.sleep(2)

def ingest_dbnomics(con):
    print("\n--- 2. Lade makroökonomische Daten von DBnomics ---")
    for table_name, series_id in DBNOMICS_SERIES.items():
        try:
            print(f"Rufe {series_id} ab...")
            df = fetch_series(series_id)
            if df is not None and not df.empty:
                # Standardisierung für DuckDB
                df = df.astype(str)
                df.columns = [f'col_{i}' for i in range(len(df.columns))]
                
                con.register('tmp_db', df)
                con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM tmp_db")
                con.unregister('tmp_db')
                print(f"Tabelle bronze.{table_name} aktualisiert.")
            else:
                print(f"Leeres Ergebnis für {series_id}")
        except Exception as e:
            print(f"DBnomics Fehler bei {table_name}: {e}")

def ingest_xls_to_db(con):
    print("\n--- 3. Verarbeite lokale Excel-Dateien ---")
    xls_files = {
        'gold_stocks': ('xls/above-ground-gold-stocks.xlsx', 0),
        'etf_flows': ('xls/ETF_Flows_March_2026.xlsx', 'Fund flows by month'),
        'gdt_demand': ('xls/GDT_Tables_Q425_EN.xlsx', 'Gold Balance'),
        'mining_production': ('xls/Gold-Mining-Production-Volumes-Data-2025.xlsx', 'MINE SUPPLY DATA'),
        'gold_premiums': ('xls/gold-premiums.xlsx', 0)
    }

    for table_name, (path, sheet) in xls_files.items():
        if os.path.exists(path):
            try:
                print(f"Ingestiere {path}...")
                df = pd.read_excel(path, sheet_name=sheet, header=None).astype(str)
                df.columns = [f'col_{i}' for i in range(len(df.columns))]
                con.register('tmp_xls', df)
                con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM tmp_xls")
                con.unregister('tmp_xls')
            except Exception as e:
                print(f"Fehler bei {table_name}: {e}")
        else:
            print(f"Datei nicht gefunden: {path}")

def run_ingestion():
    # Downloads von gold.org (Backup/Proprietär)
    download_gold_org()
    
    # Datenbank-Verbindung
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    
    # DBnomics (Preise & Reserven)
    ingest_dbnomics(con)
    
    # Lokale Excel-Dateien
    ingest_xls_to_db(con)
    
    con.close()
    print("\nIngestion abgeschlossen.")

if __name__ == "__main__":
    run_ingestion()
