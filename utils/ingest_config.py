# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Central configuration for the GS AWM demo ingestion pipeline.
# All notebooks %run this file to get shared symbols, paths, and constants.

# COMMAND ----------

import datetime

# Unity Catalog paths
UC_CATALOG     = "ahtsa"
UC_SCHEMA      = "awm"
SECRET_KEY   = "fmapi"
UC_VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/raw_fmapi"

def volume_subdir(name):
    """Return path to a named subdirectory within the UC Volume."""
    return f"{UC_VOLUME_PATH}/{name}"

def ts_prefix():
    """Return a timestamp prefix for output filenames: YYYY_MM_DD_HH"""
    return datetime.datetime.now().strftime("%Y_%m_%d_%H")

def clear_directory(path):
    """Remove all files and subdirectories under path via dbutils."""
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Cleared: {path}")
    except Exception as e:
        print(f"Could not clear {path}: {e}")

# COMMAND ----------

# FMP API — key loaded from Databricks UC secret scope
# To set up: databricks secrets put-secret --scope awm-demo --key fmp-api-key
SECRET_SCOPE   = "awm-demo"
SECRET_KEY_FMP = "fmp-api-key"

# read a specific secret
FMP_API_KEY= dbutils.secrets.get(catalog=UC_CATALOG , schema=UC_SCHEMA, key=SECRET_KEY)

FMP_BASE_URL_STABLE = "https://financialmodelingprep.com/stable"
FMP_BASE_URL_V3     = "https://financialmodelingprep.com/api/v3"

# COMMAND ----------

# ── Run mode ──────────────────────────────────────────────────────────────────
# Set TEST_MODE = True to use the sample ticker lists (minimal API calls).
# Set TEST_MODE = False to use the full production lists.
TEST_MODE = True

# COMMAND ----------

# Anchor BDC tickers — core private credit vehicles for covenant analysis
BDC_TICKERS = ["AINV", "OCSL"]

# ── Ticker lists ──────────────────────────────────────────────────────────────
_EQUITY_TICKERS_SAMPLE = [
    "GS",   # one equity
    "SPY",  # one index fund
]

_EQUITY_TICKERS_FULL = [
    "GS", "MS", "JPM",  # Financials
    "BX", "APO", "KKR", # Alt managers
    "SPY", "AGG",        # Benchmark ETFs
    "BKLN", "HYG",       # Credit ETFs
] + BDC_TICKERS

_ETF_TICKERS_SAMPLE = ["SPY"]
_ETF_TICKERS_FULL   = ["SPY", "AGG", "BKLN", "HYG", "QQQ", "IWM"]

# Active lists — resolved by TEST_MODE
EQUITY_TICKERS = _EQUITY_TICKERS_SAMPLE if TEST_MODE else _EQUITY_TICKERS_FULL
ETF_TICKERS    = _ETF_TICKERS_SAMPLE    if TEST_MODE else _ETF_TICKERS_FULL

# Benchmark index symbols
INDEX_SYMBOLS = ["^GSPC", "^DJI", "^IXIC"]
VIX_SYMBOL    = "^VIX"

# Lookback window for historical price data
HISTORY_START_DATE = "2023-01-01"

# COMMAND ----------

print(f"Config loaded — volume: {UC_VOLUME_PATH}")
print(f"Mode: {'TEST' if TEST_MODE else 'FULL'}")
print(f"BDC anchors: {BDC_TICKERS}")
print(f"Equity tickers ({len(EQUITY_TICKERS)}): {EQUITY_TICKERS}")
print(f"ETF tickers ({len(ETF_TICKERS)}): {ETF_TICKERS}")
