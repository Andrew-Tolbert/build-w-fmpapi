# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull key metrics and financial ratios for all watchlist tickers.
# These provide pre-computed covenant proxies: netDebtToEBITDA, interestCoverage.
# Output: UC_VOLUME_PATH/key_metrics/{TICKER}/{ts}_{key_metrics,financial_ratios}.json
# FMP Sources: F6/F7

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

apply_full_refresh("key_metrics")

# COMMAND ----------

out_base = volume_subdir("key_metrics")
_ts = ts_prefix()

for ticker in EQUITY_TICKERS:
    try:
        metrics = client.get_key_metrics(ticker, period="quarterly", limit=24)
        ratios  = client.get_ratios(ticker, period="quarterly", limit=24)

        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        ingested_at = pd.Timestamp.now().isoformat()

        mdf = pd.DataFrame(metrics); mdf["symbol"] = ticker; mdf["ingested_at"] = ingested_at
        rdf = pd.DataFrame(ratios);  rdf["symbol"] = ticker; rdf["ingested_at"] = ingested_at

        mdf.to_json(f"{ticker_dir}/{_ts}_key_metrics.json",      orient="records", indent=2)
        rdf.to_json(f"{ticker_dir}/{_ts}_financial_ratios.json", orient="records", indent=2)

        print(f"  {ticker}: {len(mdf)} metrics rows, {len(rdf)} ratio rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
