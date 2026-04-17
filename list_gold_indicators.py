import requests
try:
    url = "https://api.db.nomics.world/v22/series/IMF/IFS?q=Gold&dimensions=%7B%22FREQ%22%3A%5B%22M%22%5D%7D"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        indicators = set()
        for doc in data['series']['docs']:
            parts = doc['series_code'].split('.')
            if len(parts) >= 3:
                indicators.add(parts[2])
        print(f"Indicators related to Gold: {sorted(list(indicators))}")
except Exception as e:
    print(f"Error: {e}")
