import requests

url = "https://api.db.nomics.world/v22/datasets/FED"
r = requests.get(url)
data = r.json()
print(f"FED has {data['datasets']['total_nb_docs']} datasets")
for d in data['datasets']['docs']:
    print(f"Dataset: {d['code']} - {d['name']}")
