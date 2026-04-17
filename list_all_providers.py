import requests

url = "https://api.db.nomics.world/v22/providers"
r = requests.get(url)
data = r.json()
print([p['code'] for p in data['providers']['docs']])
