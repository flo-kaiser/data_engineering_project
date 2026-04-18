# 🏆 ENTERPRISE SPECIFICATION: Gold Intelligence Framework (Hybrid-Cloud Edition)

## 1. Vision & Dokumentations-Standard
Dieses Projekt demonstriert den Aufbau einer professionellen, **Environment-Aware** Datenplattform zur Analyse des globalen Goldmarktes. 
**Kernanforderung:** Jede Komponente ist lückenlos dokumentiert. Ziel ist eine "Self-Documenting Pipeline", die technisches Design und finanzmathematische Logik (z. B. rollierende Korrelationen) für Stakeholder und Recruiter transparent macht.

## 2. Daten-Architektur (100% API-Driven via DBnomics)
Das Projekt verzichtet auf manuelle Excel-Downloads und nutzt die **DBnomics-API** zur Aggregation offizieller Zeitreihen von Weltbank (WB), IWF (IMF) und Fed:
* **Gold Spot Price (USD/oz):** `WB/commodity_prices/FGOLD-1W`
* **Official Gold Reserves (Global):** `IMF/IFS/M.W00.RAFAGOLDV_OZT`
* **Real Interest Rates (10Y TIPS):** `FED/DFII10`
* **FX-Impact (EUR/USD):** `ECB/EXR/M.USD.EUR.SP00.A`

## 3. Hybrid-Environment Design (Local vs. Cloud)
Das Framework ist so konzipiert, dass es ohne Code-Änderungen zwischen zwei Welten wechselt (gesteuert über Umgebungsvariablen):

* **Local (Development/Debugging):**
    * **Storage:** Lokale Parquet-Files.
    * **Warehouse:** DuckDB (`.duckdb` Datei).
    * **dbt Target:** `dev`.
    * **Zweck:** Schnelle Iteration, lokales Testen, 0€ Kosten.
* **Cloud (Production - Google Cloud Platform):**
    * **Storage:** Google Cloud Storage (GCS Buckets).
    * **Warehouse:** Google BigQuery.
    * **dbt Target:** `prod`.
    * **Zweck:** Skalierbarkeit, Persistenz, professioneller Betrieb.

## 4. Dependency & Workflow Management
Stabilität wird durch ein vierstufiges Management-System garantiert:
1.  **Software:** **UV** für deterministische Python-Abhängigkeiten (`uv.lock`).
2.  **Container:** **Docker** für identische Laufzeitumgebungen (Dev-Prod-Parity).
3.  **Transformation:** **dbt (ref/source)** zur Steuerung der SQL-Abhängigkeiten im Warehouse.
4.  **Orchestrierung:** **Apache Airflow** zur zeitlichen Steuerung (Scheduling) und Überwachung der Pipeline.

## 5. Medallion-Pipeline & Analytics
* **Bronze (Raw):** Idempotenter API-Ingest via Python. Speicherung als Parquet mit Metadaten-Tracking.
* **Silver (Staging):** Normalisierung der Zeitreihen und Einheiten-Harmonisierung (Unzen zu Tonnen).
* **Gold (Marts):** Berechnung komplexer Metriken wie der **Pearson-Korrelation** zwischen Realzinsen und Goldpreis über rollierende 12-Monats-Fenster.

---

## 🛠 ARBEITSANWEISUNG FÜR DIE GEMINI CLI

**Bitte implementiere das Framework basierend auf dieser Spezifikation mit Fokus auf Modularität und Dokumentation. Zwischenstände in git speichern.:**

1.  **Environment-Aware Ingestor:** Schreibe eine Python-Klasse `GoldIngestor`, die `.env`-Variablen nutzt, um zu entscheiden, ob sie Daten lokal in DuckDB oder in einen GCS-Bucket schreibt. Implementiere Docstrings für alle Methoden.
2.  **dbt BigQuery-Profil:** Erstelle eine `profiles.yml`, die sowohl ein `dev` Target (DuckDB) als auch ein `prod` Target (BigQuery via Service Account) enthält.
3.  **Financial Analytics (Gold Layer):** Erstelle ein dbt-Modell `fct_gold_correlation.sql`, das mittels `WINDOW`-Funktionen die rollierende Korrelation zwischen Goldpreis und Realzinsen berechnet. Dokumentiere die Logik im SQL-Header.
4.  **Airflow DAG:** Entwirf einen DAG `gold_pipeline_master`, der die Ingestion (`PythonOperator`) und danach die Transformation (`DbtRunOperator` oder `BashOperator`) startet.
5.  **Dokumentation:** Erstelle für alle Komponenten die entsprechenden `schema.yml` Dateien und eine `README.md`-Vorlage für das gesamte Repo.

