import os

# --- 100% STABLE API Mappings ---
SERIES_MAP = {
    # Reserven & Zinsen (DBnomics ist hier stabil)
    'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
    'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
    'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api',
}

EXCEL_MAP = {}

# --- Yahoo Finance (Maximale Stabilität für Marktpreise) ---
YFINANCE_MAP = {
    'gold_prices_api': 'GC=F',   # Echter Gold-Future Preis (USD/oz)
    'dxy': 'DX-Y.NYB', 
    'sp500': '^GSPC', 
    'yield_10y': '^TNX',
    'crude_oil': 'CL=F',
    'etf_flows_proxy': 'GLD' 
}
