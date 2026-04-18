import requests
try:
    url = "https://api.db.nomics.world/v22/series/WB/commodity_prices"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        for doc in data['series']['docs']:
            print(f"Code: {doc['series_code']} - Name: {doc['series_name']}")
except Exception as e:
    print(f"Error: {e}")
