import requests

# Search for DFII10 in FED provider
url = "https://api.db.nomics.world/v22/providers"
r = requests.get(url)
providers = r.json()
fed_provider = [p for p in providers['providers']['docs'] if 'FED' in p['code']]
print(f"FED Providers: {fed_provider}")

url = "https://api.db.nomics.world/v22/datasets/FED"
r = requests.get(url)
if r.status_code == 200:
    datasets = r.json()
    # print(datasets)
    dfii10_datasets = [d for d in datasets['datasets']['docs'] if 'DFII10' in d['code'] or 'DFII10' in d['name']]
    print(f"DFII10 Datasets: {dfii10_datasets}")
else:
    print(f"Failed to fetch datasets for FED: {r.status_code}")

# Try searching globally
url = "https://api.db.nomics.world/v22/search?q=DFII10"
r = requests.get(url)
search_results = r.json()
if 'series' in search_results and 'docs' in search_results['series']:
    for doc in search_results['series']['docs']:
        print(f"Found series: {doc['series_code']} in dataset {doc['dataset_code']} by provider {doc['provider_code']}")
