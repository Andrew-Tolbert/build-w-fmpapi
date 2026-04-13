# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull recent stock news for all watchlist tickers.
# Output: UC_VOLUME_PATH/stock_news/{TICKER}/{ts}_news.json
# FMP Source: F15 — /stable/stock-news

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
# clear_directory(volume_subdir("stock_news"))

# COMMAND ----------

NEWS_LIMIT = 50  # articles per ticker
out_base = volume_subdir("stock_news")
_ts = ts_prefix()

for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_stock_news(ticker, limit=NEWS_LIMIT)
        df = pd.DataFrame(rows)
        df["symbol"] = ticker
        df["ingested_at"] = pd.Timestamp.now().isoformat()
        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        df.to_json(f"{ticker_dir}/{_ts}_news.json", orient="records", indent=2)
        print(f"  {ticker}: {len(df)} articles")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
