# Databricks notebook source
# COMMAND ----------

# Central configuration for the GS AWM demo ingestion pipeline.
# All notebooks %run this file to get shared symbols, paths, and constants.

# COMMAND ----------

# Unity Catalog paths
UC_CATALOG     = "uc"
UC_SCHEMA      = "wealth"
UC_VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/documents"

def uc_table(name):
    """Return fully-qualified UC table name."""
    return f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# FMP API — key loaded from Databricks UC secret scope
# To set up: databricks secrets put-secret --scope awm-demo --key fmp-api-key
SECRET_SCOPE   = "awm-demo"
SECRET_KEY_FMP = "fmp-api-key"

FMP_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY_FMP)  # noqa: F821
FMP_BASE_URL_STABLE = "https://financialmodelingprep.com/stable"
FMP_BASE_URL_V3     = "https://financialmodelingprep.com/api/v3"

# COMMAND ----------

# Anchor BDC tickers — core private credit vehicles for covenant analysis
BDC_TICKERS = ["AINV", "OCSL"]

# Equity watchlist — public holdings for portfolio dashboard
EQUITY_TICKERS = [
    "AINV", "OCSL",     # BDCs
    "GS", "MS", "JPM",  # Financials
    "BX", "APO", "KKR", # Alt managers
    "SPY", "AGG",        # Benchmark ETFs
    "BKLN", "HYG",       # Credit ETFs
]

# ETF symbols to fully ingest (holdings + sectors)
ETF_TICKERS = ["SPY", "AGG", "BKLN", "HYG", "QQQ", "IWM"]

# Benchmark index symbols
INDEX_SYMBOLS = ["^GSPC", "^DJI", "^IXIC"]
VIX_SYMBOL    = "^VIX"

# Lookback window for historical price data
HISTORY_START_DATE = "2023-01-01"

# COMMAND ----------

print(f"Config loaded — catalog: {UC_CATALOG}, schema: {UC_SCHEMA}")
print(f"BDC anchors: {BDC_TICKERS}")
print(f"Watchlist: {len(EQUITY_TICKERS)} tickers")
