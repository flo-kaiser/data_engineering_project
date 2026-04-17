from dbnomics import fetch_series
import pandas as pd

variations = [
    'IMF/IFS/M.001.RAFAGOLDV_OZT',
    'IMF/IFS/M.1C_001.RAFAGOLDV_OZT',
    'WB/commodity_prices/GOLD'
]

for v in variations:
    try:
        print(f"Testing {v}...")
        df = fetch_series(v)
        if df is not None and not df.empty:
            print(f"Success! Shape: {df.shape}")
            # print(df.columns.tolist())
        else:
            print(f"Empty result for {v}")
    except Exception as e:
        print(f"Failed {v}: {e}")
