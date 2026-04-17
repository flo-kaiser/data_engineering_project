import requests

url = "https://api.db.nomics.world/v22/datasets/FED?limit=1000"
r = requests.get(url)
data = r.json()
for d in data['datasets']['docs']:
    if 'H15' in d['code']:
        print(f"Dataset: {d['code']} - {d['name']}")
