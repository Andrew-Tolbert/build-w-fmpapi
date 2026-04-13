# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "requests",
# ]
# ///
# Scratchpad for testing FMP API calls interactively.
# Run cells individually to inspect raw responses.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import json
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)
TEST_TICKER = "GS"

def pp(data, rows=3):
    """Pretty-print the first N rows of a list or a dict."""
    if isinstance(data, list):
        print(json.dumps(data[:rows], indent=2, default=str))
        print(f"  ... ({len(data)} total records)")
    else:
        print(json.dumps(data, indent=2, default=str))

# COMMAND ----------

# --- Company profile ---
print(f"=== Profile: {TEST_TICKER} ===")
profile = client.get_profile(TEST_TICKER)
pp(profile)

# COMMAND ----------

# --- Historical prices (last 30 days) ---
print(f"=== Historical prices: {TEST_TICKER} ===")
from_date = (pd.Timestamp.today() - pd.Timedelta(days=30)).strftime("%Y-%m-%d")
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")
prices = client.get_historical_prices(TEST_TICKER, from_date, to_date)
pp(prices)

# COMMAND ----------

# --- Income statement (quarterly, last 2) ---
print(f"=== Income statement: {TEST_TICKER} ===")
income = client.get_income_statement(TEST_TICKER, period="quarterly", limit=2)
pp(income)

# COMMAND ----------

# --- Key metrics ---
print(f"=== Key metrics: {TEST_TICKER} ===")
metrics = client.get_key_metrics(TEST_TICKER, period="quarterly", limit=2)
pp(metrics)

# COMMAND ----------

# --- Analyst estimates ---
print(f"=== Analyst estimates: {TEST_TICKER} ===")
estimates = client.get_analyst_estimates(TEST_TICKER, limit=2)
pp(estimates)

# COMMAND ----------

# --- Price target consensus ---
print(f"=== Price target consensus: {TEST_TICKER} ===")
pt = client.get_price_target_consensus(TEST_TICKER)
pp(pt)

# COMMAND ----------

# --- Grades summary ---
print(f"=== Grades summary: {TEST_TICKER} ===")
grades = client.get_grades_summary(TEST_TICKER)
pp(grades)

# COMMAND ----------

# --- Stock news (5 articles) ---
print(f"=== News: {TEST_TICKER} ===")
news = client.get_stock_news(TEST_TICKER, limit=5)
pp(news, rows=2)

# COMMAND ----------

# --- SEC filings (10-Q, last 2) ---
print(f"=== SEC filings: {TEST_TICKER} ===")
filings = client.get_sec_filings(TEST_TICKER, filing_type="10-Q", limit=2)
pp(filings)

# COMMAND ----------

# --- ETF holdings (SPY) ---
print("=== ETF holdings: SPY ===")
holdings = client.get_etf_holdings("SPY")
pp(holdings)

# COMMAND ----------

# --- Index quotes ---
print("=== Index quotes ===")
quotes = client.get_index_quote(INDEX_SYMBOLS + [VIX_SYMBOL])
pp(quotes)

# COMMAND ----------

# --- Earnings transcript (AINV, most recent) ---
import datetime
yr = datetime.date.today().year
print(f"=== Transcript: AINV Q1 {yr} ===")
transcript = client.get_transcript("AINV", year=yr, quarter=1)
pp(transcript)
