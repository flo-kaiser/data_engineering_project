import requests

url = "https://api.db.nomics.world/v22/series/FED/H15?limit=1000"
r = requests.get(url)
data = r.json()
if 'series' in data and 'docs' in data['series']:
    for doc in data['series']['docs']:
        if 'Inflation' in doc['series_name'] or 'Indexed' in doc['series_name'] or 'TIPS' in doc['series_name']:
            print(f"Code: {doc['series_code']} - Name: {doc['series_name']}")
else:
    print("Could not find series in FED/H15")
