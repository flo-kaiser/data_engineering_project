import requests

url = "https://api.db.nomics.world/v22/search?q=TIPS"
r = requests.get(url)
search_results = r.json()
if 'series' in search_results and 'docs' in search_results['series']:
    for doc in search_results['series']['docs'][:20]:
        print(f"Found series: {doc['series_code']} in dataset {doc['dataset_code']} by provider {doc['provider_code']} - {doc['series_name']}")
else:
    print("No results found for TIPS")
