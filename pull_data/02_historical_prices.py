# Databricks notebook source
# COMMAND ----------

# Pull historical daily price data for all watchlist tickers.
# Output: UC_VOLUME_PATH/historical_prices/historical_prices.json
# FMP Source: F2 — /stable/historical-price-eod/dividend-adjusted?symbol=...

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

from_date = HISTORY_START_DATE
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")

print(f"Date range: {from_date} → {to_date}")
print(f"Fetching price history for {len(EQUITY_TICKERS)} tickers...")

# COMMAND ----------

all_frames = []
for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_historical_prices(ticker, from_date, to_date)
        df = pd.DataFrame(rows)
        df["symbol"] = ticker
        all_frames.append(df)
        print(f"  {ticker}: {len(df)} rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
combined["ingested_at"] = pd.Timestamp.now().isoformat()
print(f"\nTotal rows: {len(combined)}")

# COMMAND ----------

out_dir = volume_subdir("historical_prices")
os.makedirs(out_dir, exist_ok=True)
out_path = f"{out_dir}/historical_prices.json"

combined.to_json(out_path, orient="records", indent=2)
print(f"Written {len(combined)} rows to {out_path}")
