import requests
try:
    url = "https://api.db.nomics.world/v22/datasets/WB"
    r = requests.get(url)
    data = r.json()
    if 'datasets' in data and 'docs' in data['datasets']:
        for doc in data['datasets']['docs']:
            print(f"Dataset: {doc['code']} - Name: {doc['name']}")
except Exception as e:
    print(f"Error: {e}")
