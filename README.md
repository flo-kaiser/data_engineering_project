# 🏆 Gold Intelligence Framework (Full-API Driven)

Professional Market Data Platform using a Medallion Architecture.

## 🏗 Architecture
This project implements a three-tier data architecture (Medallion Architecture) powered by **DuckDB** and **dbt**.

1.  **Bronze (Raw/Ingestion):** Full-API integration via DBnomics. Data is ingested "as-is" with metadata tracking and idempotency.
2.  **Silver (Staging/Intermediate):** Data cleaning, normalization (e.g., metric tons conversion), and complex financial engineering (Pearson correlation).
3.  **Gold (Marts):** Business-ready tables (Marts) for BI tools, aggregating monthly market metrics.

## 🛠 Tech Stack
- **Database:** DuckDB
- **Transformation:** dbt (dbt-duckdb)
- **Ingestion:** Python (DBnomics API)
- **Orchestration:** Integrated Python Main Script

## 📂 Project Structure
- `gold_dbt/`: dbt project for transformation logic.
  - `models/staging/`: Silver layer (cleaning).
  - `models/intermediate/`: Silver layer (complex logic like correlations).
  - `models/marts/`: Gold layer (analytical views).
- `ingest_manager.py`: Modular Python framework for API ingestion.
- `main.py`: Central orchestration script.
- `project_description.md`: Detailed enterprise specification.

## 🚀 Getting Started
1. **Environment Setup:**
   ```bash
   pip install duckdb dbt-duckdb pandas dbnomics
   ```
2. **Execution:**
   ```bash
   python main.py
   ```
   This will trigger ingestion, dbt runs, and tests.

## 📊 Monitoring & Logging
Detailed logs are stored in the `logs/` directory. Metadata-tracking ensures traceability of every API request.
