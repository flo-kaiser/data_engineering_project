import os

# --- 100% STABLE API Mappings ---
SERIES_MAP = {
    # Preise & Reserven (Absolut stabil)
    'WB/commodity_prices/FGOLD-1W': 'gold_prices_api',
    'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
    'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
    'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api',
    
    # Alternative für Minenproduktion (Weltbank GEM - Globaler Index)
    'WB/GEM/M.GOLD.PROD': 'mining_production',
}

EXCEL_MAP = {}

# --- Yahoo Finance (Sehr stabil für ETF-Flows Ersatz) ---
YFINANCE_MAP = {
    'dxy': 'DX-Y.NYB', 
    'sp500': '^GSPC', 
    'yield_10y': '^TNX',
    'crude_oil': 'CL=F',
    'etf_flows_proxy': 'GLD' # Wir nutzen den GLD ETF als Proxy für ETF-Flows
}
