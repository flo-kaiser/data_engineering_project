import os

# --- DBnomics API Mappings ---
SERIES_MAP = {
    'WB/commodity_prices/FGOLD-1W': 'gold_prices_api',
    'IMF/IFS/M.W00.RAFAGOLDV_OZT': 'gold_reserves_api',
    'FED/H15/RIFLGFCY10_XII_N.M': 'real_interest_rates_api',
    'ECB/EXR/M.USD.EUR.SP00.A': 'fx_usd_eur_api'
}

# --- Excel Fundamental Data Mappings ---
EXCEL_MAP = {
    'gold_stocks': ('xls/above-ground-gold-stocks.xlsx', 0),
    'etf_flows': ('xls/ETF_Flows_March_2026.xlsx', 'Fund flows by month'),
    'gdt_demand': ('xls/GDT_Tables_Q425_EN.xlsx', 'Gold Balance'),
    'gold_prices': ('xls/Gold_price_averages_in_a range_of_currencies_since_1978.xlsx', 'Monthly_Avg'),
    'mining_production': ('xls/Gold-Mining-Production-Volumes-Data-2025.xlsx', 'MINE SUPPLY DATA'),
    'gold_premiums': ('xls/gold-premiums.xlsx', 0),
    'central_bank_holdings': ('xls/World_official_gold_holdings_as_of_Apr2026_IFS.xlsx', 0)
}

# --- Yahoo Finance Indicator Mappings ---
YFINANCE_MAP = {
    'dxy': 'DX-Y.NYB', 
    'sp500': '^GSPC', 
    'yield_10y': '^TNX',
    'crude_oil': 'CL=F'
}
