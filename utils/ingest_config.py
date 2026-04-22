# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# Central configuration for the GS AWM demo ingestion pipeline.
# All notebooks %run this file to get shared symbols, paths, and constants.

# COMMAND ----------

import datetime

# COMMAND ----------

# Job parameters — set defaults here; override via Databricks job params or widget UI
dbutils.widgets.text(    "uc_catalog",         "ahtsa")
dbutils.widgets.text(    "uc_schema",          "awm")
dbutils.widgets.text(    "secret_key",         "fmapi")
dbutils.widgets.dropdown("limited_load",       "true", ["true", "false"])
dbutils.widgets.text(    "history_start_date", "2023-01-01")

# COMMAND ----------

# Unity Catalog paths
UC_CATALOG     = dbutils.widgets.get("uc_catalog")
UC_SCHEMA      = dbutils.widgets.get("uc_schema")
SECRET_KEY     = dbutils.widgets.get("secret_key")
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

# ── Full-refresh configuration ─────────────────────────────────────────────────
# Set full_refresh=True for a section to wipe its volume directory (and drop its
# log table, if any) at the start of the next run.
#
# SAFE to full-refresh (API calls are lightweight / quick to re-fetch):
#   company_profiles, historical_prices, financials, key_metrics,
#   etf_data, analyst_data, indexes
#
# NOT SAFE by default (web-scraped, rate-limited, or large downloads):
#   stock_news        — article full-text requires individual HTTP fetches
#   sec_filings       — EDGAR HTML documents, fetched one-by-one
#   financial_reports — large structured JSON reports per ticker/period
#   transcripts       — earnings call transcripts, limited availability
#   (For these, full_refresh also drops the log table so items are re-fetched.)

REFRESH_CONFIG = {
    "company_profiles":  {"full_refresh": True, "log_table": None},
    "historical_prices": {"full_refresh": True, "log_table": None},
    "financials":        {"full_refresh": True, "log_table": None},
    "key_metrics":       {"full_refresh": True, "log_table": None},
    "etf_data":          {"full_refresh": True, "log_table": None},
    "analyst_data":      {"full_refresh": True, "log_table": None},
    "indexes":           {"full_refresh": True, "log_table": None},
    # These have log tables — full refresh clears the dir AND drops the log table
    "stock_news":        {"full_refresh": False, "log_table": "stock_news_log"},
    "financial_reports": {"full_refresh": False, "log_table": "financial_reports_log"},
    "sec_filings":       {"full_refresh": False, "log_table": "sec_filings_log"},
    "transcripts":       {"full_refresh": False, "log_table": "transcripts_log"},
}


def apply_full_refresh(section: str) -> None:
    """Clear the volume directory (and drop its log table) for *section* if
    REFRESH_CONFIG[section]['full_refresh'] is True.  Call this at the top of
    each ingestion notebook, before any writes."""
    cfg = REFRESH_CONFIG.get(section)
    if cfg is None:
        print(f"[refresh] Unknown section '{section}' — skipping")
        return
    if not cfg["full_refresh"]:
        return
    print(f"[refresh] Full refresh enabled for '{section}'")
    clear_directory(volume_subdir(section))
    log_table = cfg.get("log_table")
    if log_table:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{log_table}")
            print(f"[refresh] Dropped log table: {UC_CATALOG}.{UC_SCHEMA}.{log_table}")
        except Exception as e:
            print(f"[refresh] Could not drop log table {log_table}: {e}")

# ── Ticker configuration ───────────────────────────────────────────────────────
# Single source of truth for every ticker in the pipeline.
#   type     — asset class: "equity", "etf", "private_credit"
#   limited  — include when LIMITED_LOAD = True (one representative per type)
#   sec_forms — {form_type: max_count} for 05_sec_filings.py;
#               omit or leave empty for tickers that don't file those forms
#
# Benchmark index symbols (^GSPC, ^VIX …) are market-data feeds, not equities;
# they stay in INDEX_SYMBOLS / VIX_SYMBOL below and are not ticker-config driven.

TICKER_CONFIG = {
    # ── Equities / alt managers & large banks ─────────────────────────────────
    "GS":   {"type": "equity", "limited": True,  "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "MS":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "JPM":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "BAC":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "C":    {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "WFC":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "BX":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "APO":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "KKR":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "BLK":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "SCHW": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "ICE":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "CME":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── Equities / technology ─────────────────────────────────────────────────
    "AAPL": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "MSFT": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "NVDA": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "GOOGL":{"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "META": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "AMZN": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "TSLA": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "AVGO": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "ORCL": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "CRM":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "ADBE": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "AMD":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "QCOM": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── Equities / healthcare ─────────────────────────────────────────────────
    "JNJ":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "UNH":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "LLY":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "ABBV": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "PFE":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "MRK":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "TMO":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "AMGN": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── Equities / consumer ───────────────────────────────────────────────────
    "WMT":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "COST": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "HD":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "MCD":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "NKE":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "PG":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "KO":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "PEP":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "SBUX": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "TGT":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── Equities / industrials & energy ──────────────────────────────────────
    "CAT":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "DE":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "HON":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "GE":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "RTX":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "BA":   {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "UPS":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "XOM":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "CVX":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "COP":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "NEE":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── Equities / materials & REITs ──────────────────────────────────────────
    "FCX":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "NEM":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "LIN":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "AMT":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "PLD":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "WELL": {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    "CCI":  {"type": "equity", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5}},
    # ── ETFs — broad market ───────────────────────────────────────────────────
    "SPY":  {"type": "etf", "limited": True,  "sec_forms": {}},
    "QQQ":  {"type": "etf", "limited": False, "sec_forms": {}},
    "IWM":  {"type": "etf", "limited": False, "sec_forms": {}},
    "VTI":  {"type": "etf", "limited": False, "sec_forms": {}},
    "VOO":  {"type": "etf", "limited": False, "sec_forms": {}},
    "DIA":  {"type": "etf", "limited": False, "sec_forms": {}},
    # ── ETFs — fixed income ───────────────────────────────────────────────────
    "AGG":  {"type": "etf", "limited": False, "sec_forms": {}},
    "TLT":  {"type": "etf", "limited": False, "sec_forms": {}},
    "LQD":  {"type": "etf", "limited": False, "sec_forms": {}},
    "HYG":  {"type": "etf", "limited": False, "sec_forms": {}},
    "JNK":  {"type": "etf", "limited": False, "sec_forms": {}},
    "EMB":  {"type": "etf", "limited": False, "sec_forms": {}},
    "BIL":  {"type": "etf", "limited": False, "sec_forms": {}},
    "SHY":  {"type": "etf", "limited": False, "sec_forms": {}},
    "BKLN": {"type": "etf", "limited": False, "sec_forms": {}},
    # ── ETFs — sector ─────────────────────────────────────────────────────────
    "XLF":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLK":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLE":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLV":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLI":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLY":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLU":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLP":  {"type": "etf", "limited": False, "sec_forms": {}},
    "XLRE": {"type": "etf", "limited": False, "sec_forms": {}},
    # ── ETFs — international ──────────────────────────────────────────────────
    "EFA":  {"type": "etf", "limited": False, "sec_forms": {}},
    "EEM":  {"type": "etf", "limited": False, "sec_forms": {}},
    "VEU":  {"type": "etf", "limited": False, "sec_forms": {}},
    # ── ETFs — commodities & alternatives ────────────────────────────────────
    "GLD":  {"type": "etf", "limited": False, "sec_forms": {}},
    "SLV":  {"type": "etf", "limited": False, "sec_forms": {}},
    "DBC":  {"type": "etf", "limited": False, "sec_forms": {}},
    "VNQ":  {"type": "etf", "limited": False, "sec_forms": {}},
    # ── Private credit / BDCs ─────────────────────────────────────────────────
    "ARCC": {"type": "private_credit", "limited": True,  "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "MAIN": {"type": "private_credit", "limited": True,  "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "GBDC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "FSK":  {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "BXSL": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "OBDC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "HTGC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "NMFC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "PSEC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "SLRC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "GSBD": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "CGBD": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "MFIC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "OCSL": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "TCPC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
    "CSWC": {"type": "private_credit", "limited": False, "sec_forms": {"10-K": 3, "10-Q": 3, "8-K": 5, "424B2": 5, "424B5": 5}},
}

# COMMAND ----------

# FMP API — key loaded from Databricks UC secret scope
# To set up: databricks secrets put-secret --scope awm-demo --key fmp-api-key
SECRET_SCOPE   = "awm-demo"
SECRET_KEY_FMP = "fmp-api-key"

# read a specific secret
try:
    FMP_API_KEY = dbutils.secrets.get(catalog=UC_CATALOG, schema=UC_SCHEMA, key=SECRET_KEY)
except Exception:
    FMP_API_KEY = None  # not available in synthetic notebooks

FMP_BASE_URL_STABLE = "https://financialmodelingprep.com/stable"
FMP_BASE_URL_V3     = "https://financialmodelingprep.com/api/v3"

# COMMAND ----------

# ── Load mode ─────────────────────────────────────────────────────────────────
# limited_load widget: "true" → only tickers flagged limited=True in TICKER_CONFIG
# limited_load widget: "false" → all tickers in TICKER_CONFIG
LIMITED_LOAD = dbutils.widgets.get("limited_load").lower() == "true"

def get_tickers(types=None):
    """Return ticker symbols from TICKER_CONFIG, respecting LIMITED_LOAD.

    types: list of type strings to include, e.g. ["etf"] or ["equity", "private_credit"].
           Pass None to include all types.
    """
    result = []
    for symbol, cfg in TICKER_CONFIG.items():
        if types is not None and cfg["type"] not in types:
            continue
        if LIMITED_LOAD and not cfg.get("limited", False):
            continue
        result.append(symbol)
    return result

# COMMAND ----------

# Derived ticker lists — consumed by ingestion notebooks
EQUITY_TICKERS = get_tickers()                           # all types (respects LIMITED_LOAD)
ETF_TICKERS    = get_tickers(types=["etf"])              # ETF-specific endpoints (06_etf_data)
BDC_TICKERS    = get_tickers(types=["private_credit"])   # transcripts, covenant analysis

# Benchmark index symbols — market-data feeds, not in TICKER_CONFIG
INDEX_SYMBOLS = ["^GSPC", "^DJI", "^IXIC"]
VIX_SYMBOL    = "^VIX"

# Lookback window for historical price data
HISTORY_START_DATE = dbutils.widgets.get("history_start_date")

# COMMAND ----------

print(f"Config loaded — volume: {UC_VOLUME_PATH}")
print(f"Load mode: {'LIMITED' if LIMITED_LOAD else 'FULL'}")
print(f"All tickers  ({len(EQUITY_TICKERS)}): {EQUITY_TICKERS}")
print(f"ETF tickers  ({len(ETF_TICKERS)}): {ETF_TICKERS}")
print(f"BDC tickers  ({len(BDC_TICKERS)}): {BDC_TICKERS}")
