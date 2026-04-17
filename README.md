# Gold Market Intelligence Platform

This project implements a "Medallion" architecture data pipeline using **DuckDB** and **dbt** to transform messy gold market Excel data into structured analytical views.

## Project Structure
- `xls/`: Raw Excel data files (Mining, Prices, ETF Flows, etc.).
- `gold_dbt/`: dbt project directory.
  - `ingest.py`: Python script to load raw Excel and Yahoo Finance data into the `bronze` schema.
  - `models/staging/`: Silver layer models (cleaning, unpivoting/melting).
  - `models/marts/`: Gold layer models (analytical joins and frequency alignment).
  - `data/`: Contains the `gold_market.duckdb` database.

## Setup & Execution
1. Install dependencies:
   ```bash
   pip install duckdb dbt-duckdb pandas openpyxl yfinance
   ```
2. Ingest raw data:
   ```bash
   python gold_dbt/ingest.py
   ```
3. Run dbt transformations:
   ```bash
   cd gold_dbt
   dbt deps
   dbt run
   ```

## Key Transformations
- **The "Melt" Task:** wide-format monthly prices and ETF flows are unpivoted into long-format time series.
- **Frequency Alignment:** Daily market data (DXY, S&P 500, Yields) is aggregated to monthly averages to align with monthly gold prices and quarterly demand data.
- **Data Cleaning:** Messy headers and disclaimers in the raw Excel files are stripped using dbt SQL logic.
