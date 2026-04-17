from dbnomics import fetch_series
import pandas as pd

try:
    # Testing known valid components
    print("Testing IMF/IFS/M.W00.RAFAGOLDV_OZT...")
    df = fetch_series('IMF/IFS/M.W00.RAFAGOLDV_OZT')
    print(f"Success! Shape: {df.shape}")
except Exception as e:
    print(f"Failed: {e}")

try:
    # Testing IMF Primary Commodity Prices (PNF)
    # The gold price is often here
    print("Testing IMF/PNF/PZGOLD_USD...")
    df = fetch_series('IMF/PNF/M.W00.PZGOLD_USD')
    print(f"Success! Shape: {df.shape}")
except Exception as e:
    print(f"Failed: {e}")
