import requests

url = "https://api.db.nomics.world/v22/search?q=10-Year%20Real%20Interest%20Rate"
r = requests.get(url)
search_results = r.json()
if 'series' in search_results and 'docs' in search_results['series']:
    for doc in search_results['series']['docs']:
        print(f"Found series: {doc['series_code']} in dataset {doc['dataset_code']} by provider {doc['provider_code']}")
else:
    print("No results found for 10-Year Real Interest Rate")

url = "https://api.db.nomics.world/v22/search?q=DFII10"
r = requests.get(url)
search_results = r.json()
if 'series' in search_results and 'docs' in search_results['series']:
    for doc in search_results['series']['docs']:
        print(f"Found series: {doc['series_code']} in dataset {doc['dataset_code']} by provider {doc['provider_code']}")
else:
    print("No results found for DFII10")
