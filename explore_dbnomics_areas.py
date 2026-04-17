import requests

# Find which areas have RAFAGOLDV_OZT
try:
    url = "https://api.db.nomics.world/v22/series/IMF/IFS?q=RAFAGOLDV_OZT&dimensions=%7B%22FREQ%22%3A%5B%22M%22%5D%7D"
    r = requests.get(url)
    data = r.json()
    if 'series' in data and 'docs' in data['series']:
        areas = set()
        for doc in data['series']['docs']:
            # The series code is M.AREA.INDICATOR
            parts = doc['series_code'].split('.')
            if len(parts) >= 2:
                areas.add(parts[1])
        print(f"Available Areas for RAFAGOLDV_OZT: {sorted(list(areas))[:20]} ... (Total: {len(areas)})")
        if '001' in areas: print("Found 001 (World)")
        if '1C_001' in areas: print("Found 1C_001")
        if 'W00' in areas: print("Found W00")
        if 'WLD' in areas: print("Found WLD")
except Exception as e:
    print(f"Error: {e}")
