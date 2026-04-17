import requests
try:
    # Search for PZGOLD_USD indicator across all series
    url = "https://api.db.nomics.world/v22/series?q=PZGOLD_USD&limit=5"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        for doc in data['series']['docs']:
            print(f"Full Code: {doc['provider_code']}/{doc['dataset_code']}/{doc['series_code']} - Name: {doc['series_name']}")
except Exception as e:
    print(f"Error: {e}")
