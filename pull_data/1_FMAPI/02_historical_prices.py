# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull historical daily price data for all watchlist tickers.
# Output: UC_VOLUME_PATH/historical_prices/{TICKER}/{ts}_prices.json
# FMP Source: F2 — /stable/historical-price-eod/dividend-adjusted?symbol=...

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
# clear_directory(volume_subdir("historical_prices"))

# COMMAND ----------

from_date = HISTORY_START_DATE
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")
_ts = ts_prefix()

print(f"Date range: {from_date} → {to_date}")
print(f"Fetching price history for {len(EQUITY_TICKERS)} tickers...")

# COMMAND ----------

out_base = volume_subdir("historical_prices")

for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_historical_prices(ticker, from_date, to_date)
        df = pd.DataFrame(rows)
        df["symbol"] = ticker
        df["ingested_at"] = pd.Timestamp.now().isoformat()
        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        df.to_json(f"{ticker_dir}/{_ts}_prices.json", orient="records", indent=2)
        print(f"  {ticker}: {len(df)} rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
