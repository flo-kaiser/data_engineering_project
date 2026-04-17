import os
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import time

# Load credentials from .env
load_dotenv()

EMAIL = os.getenv('GOLD_ORG_EMAIL')
PASSWORD = os.getenv('GOLD_ORG_PASSWORD')

# URLs to download
URLS = [
    "https://www.gold.org/download/file/20717/ETF_Flows_March_2026.xlsx",
    "https://www.gold.org/download/file/7739/World_official_gold_holdings_as_of_Apr2026_IFS.xlsx?referrerNid=7150",
    "https://www.gold.org/download/file/7588/above-ground-gold-stocks.xlsx?referrerNid=7160",
    "https://www.gold.org/download/file/7593/Gold-Mining-Production-Volumes-Data-2025.xlsx?referrerNid=7323",
    "https://www.gold.org/download/file/20717/ETF_Flows_March_2026.xlsx?referrerNid=17629",
    "https://www.gold.org/download/file/20499/GDT_Tables_Q425_EN.xlsx?referrerNid=7329",
    "https://www.gold.org/download/file/8369/Gold_price_averages_in_a%20range_of_currencies_since_1978.xlsx?referrerNid=6485",
    "https://www.gold.org/download/file/11657/gold-premiums.xlsx?referrerNid=6533"
]

LOGIN_URL = "https://www.gold.org/user/login"
OUTPUT_DIR = "xls"

def download_files():
    if not EMAIL or not PASSWORD:
        print("Error: GOLD_ORG_EMAIL or GOLD_ORG_PASSWORD not found in .env file.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Use cloudscraper to bypass Cloudflare
    scraper = cloudscraper.create_scraper()
    
    # 1. Get login page to extract form tokens
    print(f"Accessing {LOGIN_URL}...")
    try:
        response = scraper.get(LOGIN_URL)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract form_build_id
        form_build_id_input = soup.find('input', {'name': 'form_build_id'})
        form_build_id = form_build_id_input['value'] if form_build_id_input else ""
    except Exception as e:
        print(f"Failed to access login page: {e}")
        form_build_id = ""

    # 2. Perform Login
    login_data = {
        'name': EMAIL,
        'pass': PASSWORD,
        'form_build_id': form_build_id,
        'form_id': 'user_login_form',
        'op': 'Log in'
    }
    
    print("Logging in...")
    try:
        response = scraper.post(LOGIN_URL, data=login_data, allow_redirects=True)
        if "Log out" in response.text or "My account" in response.text:
            print("Login successful!")
        else:
            print("Login response did not indicate success. Proceeding anyway...")
    except Exception as e:
        print(f"Login attempt failed: {e}")

    # 3. Download files
    for url in URLS:
        # Extract filename from URL
        filename = url.split('/')[-1].split('?')[0]
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        print(f"Downloading {filename}...")
        try:
            res = scraper.get(url, stream=True)
            res.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in res.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"Saved to {filepath}")
        except Exception as e:
            print(f"Failed to download {url}: {e}")
        
        # Polite delay
        time.sleep(1)

if __name__ == "__main__":
    download_files()
