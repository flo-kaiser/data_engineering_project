import dbnomics
import pandas as pd

def search_wgc():
    print("Searching for World Gold Council (WGC) data on DBnomics...")
    try:
        # Search for datasets related to gold council
        datasets = dbnomics.fetch_providers()
        wgc_provider = datasets[datasets['name'].str.contains('World Gold Council', case=False, na=False)]
        print("\nProvider Found:")
        print(wgc_provider)
        
        # If found, list datasets for that provider
        # Note: In dbnomics python client, we usually explore via fetch_series or through the website
        # Let's try to search specifically for ETF and Mining terms
        search_terms = ['etf_demand', 'mine_production', 'gold_demand_trends']
        for term in search_terms:
            print(f"\nSearching for term: {term}")
            try:
                # This is a broad search
                df = dbnomics.fetch_series_by_api_link(f"https://api.db.nomics.world/v22/series?q={term}&limit=5")
                if not df.empty:
                    print(f"Found {len(df)} series for {term}")
                    print(df[['series_code', 'series_name']].head(3))
            except Exception as e:
                print(f"Search for {term} failed: {e}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    search_wgc()
