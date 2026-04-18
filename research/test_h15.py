import dbnomics
import pandas as pd

try:
    df = dbnomics.fetch_series('FED/H15/RIFGFSY10_N.B')
    print(f"Success! Shape: {df.shape}")
    print(df.tail())
except Exception as e:
    print(f"Failed: {e}")
