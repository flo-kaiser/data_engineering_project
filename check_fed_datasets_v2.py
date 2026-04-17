import requests

url = "https://api.db.nomics.world/v22/datasets/FED"
r = requests.get(url)
data = r.json()
print(data.keys())
if 'datasets' in data:
    print(data['datasets'].keys())
    if 'docs' in data['datasets']:
        for d in data['datasets']['docs'][:10]:
            print(f"Dataset: {d['code']} - {d['name']}")
else:
    print(data)
