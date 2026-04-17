import requests

url = "https://api.db.nomics.world/v22/providers"
r = requests.get(url)
data = r.json()
for p in data['providers']['docs']:
    if 'Fed' in p['name'] or 'FRED' in p['name'] or 'Federal' in p['name']:
        print(f"Provider: {p['code']} - {p['name']}")
