import requests

url = "https://api.db.nomics.world/v22/datasets/StLouisFed"
r = requests.get(url)
if r.status_code == 200:
    data = r.json()
    print(f"StLouisFed has {data['datasets']['total_nb_docs']} datasets")
    for d in data['datasets']['docs'][:10]:
        print(f"Dataset: {d['code']} - {d['name']}")
else:
    print(f"Failed to fetch datasets for StLouisFed: {r.status_code}")
