import requests

url = "https://api.db.nomics.world/v22/providers"
r = requests.get(url)
data = r.json()
for p in data['providers']['docs']:
    if 'FRED' in p['code'] or 'StLouisFed' in p['code']:
        print(f"Provider: {p['code']} - {p['name']}")
