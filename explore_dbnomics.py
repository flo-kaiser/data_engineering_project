import requests
import pandas as pd

# Use DBnomics API directly to explore series in dataset IMF/IFS for US
try:
    url = "https://api.db.nomics.world/v22/series/IMF/IFS?q=Gold&dimensions=%7B%22FREQ%22%3A%5B%22M%22%5D%2C%22REF_AREA%22%3A%5B%22US%22%5D%7D"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        for doc in data['series']['docs']:
            print(f"Series Code: {doc['series_code']} - Name: {doc['series_name']}")
    else:
        print("No gold series found for US")
except Exception as e:
    print(f"Error: {e}")
