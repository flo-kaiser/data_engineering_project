from dbnomics import fetch_series
import pandas as pd

try:
    print("Testing IMF/IFS/M.US.RAFA_USD...")
    df = fetch_series('IMF/IFS/M.US.RAFA_USD')
    print(f"Success! Shape: {df.shape}")
except Exception as e:
    print(f"Failed: {e}")
