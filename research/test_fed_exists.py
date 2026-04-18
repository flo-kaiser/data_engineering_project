import dbnomics
try:
    df = dbnomics.fetch_series('FED/H15/RIFSPBLP_N.M')
    print(f"Success! {df.shape}")
except Exception as e:
    print(f"Failed: {e}")
