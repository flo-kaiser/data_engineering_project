import pandas as pd
import duckdb
import os
import yfinance as yf

def ingest():
    db_path = 'gold_dbt/data/gold_market.duckdb'
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    # 1. Ingest Excel files
    xls_files = {
        'gold_stocks': ('xls/above-ground-gold-stocks.xlsx', 0),
        'etf_flows': ('xls/ETF_Flows_March_2026.xlsx', 'Fund flows by month'),
        'gdt_demand': ('xls/GDT_Tables_Q425_EN.xlsx', 'Gold Balance'),
        'gold_prices': ('xls/Gold_price_averages_in_a range_of_currencies_since_1978.xlsx', 'Monthly_Avg'),
        'mining_production': ('xls/Gold-Mining-Production-Volumes-Data-2025.xlsx', 'MINE SUPPLY DATA'),
        'gold_premiums': ('xls/gold-premiums.xlsx', 0),
        'central_bank_holdings': ('xls/World_official_gold_holdings_as_of_Apr2026_IFS.xlsx', 0)
    }

    for table_name, (file_path, sheet) in xls_files.items():
        print(f"Ingesting {file_path} (sheet: {sheet}) into bronze.{table_name}...")
        try:
            df = pd.read_excel(file_path, sheet_name=sheet, header=None)
            # Convert to string to ensure "raw" ingestion doesn't fail on mixed types
            df = df.astype(str)
            # Rename columns to avoid numbers which can be tricky in SQL
            df.columns = [f'col_{i}' for i in range(len(df.columns))]
            
            con.register('tmp_df', df)
            con.execute(f"CREATE OR REPLACE TABLE bronze.{table_name} AS SELECT * FROM tmp_df")
            con.unregister('tmp_df')
        except Exception as e:
            print(f"Failed to ingest {table_name}: {e}")

    # 2. Ingest External Data (Yahoo Finance)
    print("Fetching external data from Yahoo Finance...")
    symbols = {
        'dxy': 'DX-Y.NYB', 
        'sp500': '^GSPC', 
        'yield_10y': '^TNX',
        'crude_oil': 'CL=F'
    }
    for name, symbol in symbols.items():
        try:
            print(f"Fetching {symbol}...")
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="max")
            df.reset_index(inplace=True)
            # Standardize date and convert to string for bronze
            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
            for col in df.columns:
                if col != 'Date':
                    df[col] = df[col].astype(str)
            
            con.register('tmp_df', df)
            con.execute(f"CREATE OR REPLACE TABLE bronze.{name} AS SELECT * FROM tmp_df")
            con.unregister('tmp_df')
        except Exception as e:
            print(f"Failed to fetch {symbol}: {e}")

    con.close()
    print("Ingestion complete.")

if __name__ == "__main__":
    ingest()
