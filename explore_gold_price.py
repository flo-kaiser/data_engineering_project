import requests
try:
    url = "https://api.db.nomics.world/v22/series/IMF/IFS?q=PZGOLD_USD&dimensions=%7B%22FREQ%22%3A%5B%22M%22%5D%7D"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        for doc in data['series']['docs']:
            print(f"Code: {doc['series_code']} - Name: {doc['series_name']}")
except Exception as e:
    print(f"Error: {e}")
