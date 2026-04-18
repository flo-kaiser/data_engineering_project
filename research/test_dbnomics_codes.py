from dbnomics import fetch_series
import pandas as pd

# List datasets for IMF
try:
    # Try a broader search or different codes
    # Common codes are sometimes Provider/Dataset/Dimensions
    # Let's try to fetch just the dataset metadata if possible, but fetch_series is for data.
    # I'll try some common variations
    variations = [
        'IMF/IFS/M.W00.PZGOLD_USD',
        'IMF/IFS/M.001.PZGOLD_USD',
        'IMF/IFS/PZGOLD_USD.M.W00',
        'WB/GEMCP/GOLD'
    ]
    
    for v in variations:
        try:
            print(f"Testing {v}...")
            df = fetch_series(v)
            print(f"Success! Shape: {df.shape}")
            print(df.head(2))
        except Exception as e:
            print(f"Failed {v}: {e}")

except Exception as e:
    print(f"Error: {e}")
