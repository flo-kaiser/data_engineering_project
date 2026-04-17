import dbnomics
try:
    df = dbnomics.fetch_series('ECB/EXR/M.USD.EUR.SP00.A')
    print(f"Success! {df.shape}")
except Exception as e:
    print(f"Failed: {e}")
