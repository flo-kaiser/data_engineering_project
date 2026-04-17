# 🏆 ENTERPRISE SPECIFICATION: Gold Intelligence Framework (100% API-Driven)

## 1. Vision & Dokumentations-Standard
Dieses Projekt simuliert eine professionelle Marktdaten-Plattform. 
**Kernanforderung:** Jede Komponente (Skripte, SQL-Modelle, Infrastruktur) muss lückenlos dokumentiert werden. Das Ziel ist eine "Self-Documenting Pipeline", die für externe Prüfer (Recruiter/Architekten) sofort verständlich ist.

## 2. Daten-Architektur (Full-API via DBnomics)
Wir eliminieren alle manuellen Excel-Abhängigkeiten. Die Pipeline aggregiert folgende globale Zeitreihen ausschließlich über die DBnomics-Schnittstelle:
* **Gold Spot Price (USD/oz):** `WB/commodity_prices/FGOLD-1W` (World Bank)
* **Official Gold Reserves (Global):** `IMF/IFS/M.W00.RAFAGOLDV_OZT` (IWF)
* **Real Interest Rates (10Y TIPS):** `FED/DFII10` (U.S. Federal Reserve)
* **FX-Impact (EUR/USD):** `ECB/EXR/M.USD.EUR.SP00.A` (EZB)

## 3. Technische Meilensteine & Anforderungen

### A. Python Ingestion Engine (`ingest_manager.py`)
Das Skript muss als modulares Framework aufgebaut sein:
* **Idempotenz:** Mehrfache Ausführungen dürfen keine Dubletten erzeugen (Upsert-Logik).
* **Logging & Traceability:** Implementierung eines detaillierten Loggings für den Ingest-Status.
* **Metadata-Tracking:** Speicherung der "Last-Updated"-Zeitstempel für jede API-Serie in einer Metadaten-Tabelle in DuckDB.
* **In-Code Dokumentation:** Nutzung von Google-Style Docstrings für alle Klassen und Methoden.

### B. dbt Transformation & Financial Engineering (Silver/Gold Layer)
Die mathematische Aufbereitung erfolgt im dbt-Projekt:
* **Silver (Staging):** Einheitliche Zeitstempel-Normalisierung und Umrechnung von Feinunzen in metrische Tonnen ($1t = 32150.7 oz$).
* **Intermediate (Analytics):** Berechnung der **Pearson-Korrelation** zwischen Realzinsen und Goldpreis über ein rollierendes 12-Monats-Fenster mittels SQL-Window-Functions.
* **Gold (Marts):** Erstellung einer `fct_gold_valuation_index`, die einen gewichteten Score aus Zinsniveau, Währungseffekt und Zentralbank-Kaufvolumen berechnet.

### C. Dokumentations- & Test-Suite
* **README.md:** Automatische Generierung einer Projektübersicht inklusive Architektur-Diagramm (Mermaid.js).
* **dbt Docs:** Vollständige `schema.yml` Beschreibungen für jede Spalte.
* **Data Quality:** dbt-Tests für Referenzintegrität und Schwellenwerte (z.B. Goldpreis > 0).

---

## 🛠 ARBEITSANWEISUNG FÜR DIE GEMINI CLI

**Bitte bearbeite die folgenden Aufgaben mit maximalem Detailgrad und erstelle für jeden Schritt die dazugehörige Dokumentation:**

1. **System-Design:** Erstelle eine vollständige Verzeichnisstruktur für dieses Projekt. Generiere eine `README.md`, die den Full-API-Ansatz und die Medallion-Architektur erklärt.

2. **Ingestion-Framework:** Schreibe die Python-Klasse `DBnomicsIngestor`. Nutze `dbnomics.fetch_series`, transformiere das Ergebnis in ein sauberes Schema und schreibe es via `duckdb.connect()` in DuckDB. Implementiere ein robustes Logging-System.

3. **Komplexe dbt-Modelle:** * Erstelle `stg_gold_prices.sql` und `stg_real_rates.sql`.
    * Erstelle ein Modell `int_gold_correlation.sql`, das die rollierende Korrelation (Pearson) zwischen Goldpreis und Realzinsen berechnet. Dokumentiere die mathematische Logik im SQL-Header.

4. **Finanzmathematischer Mart:** Erstelle `fct_market_summary.sql`, das monatliche Metriken (Preis, Bestand, Korrelation, FX-Rate) in einer flachen Tabelle für BI-Tools zusammenführt.

5. **Orchestrierung:** Erstelle ein `main.py` Skript, das den Ingest startet, danach `dbt run` und `dbt test` ausführt und am Ende eine Zusammenfassung der Dokumentation ausgibt.

**Wichtig:** Der Output muss so umfangreich sein, dass er als Grundlage für eine professionelle Enterprise-Anwendung dienen kann.
