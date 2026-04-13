# Databricks notebook source
# COMMAND ----------

# Pull index quotes, historical index prices, VIX, and index constituents.
# Output: UC_VOLUME_PATH/indexes/{index_quotes,index_history,index_constituents}.json
# FMP Sources: F16/F17/F18/F19
# Symbols: ^GSPC (S&P 500), ^DJI (Dow Jones), ^IXIC (Nasdaq), ^VIX

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

ALL_INDEX_SYMBOLS = INDEX_SYMBOLS + [VIX_SYMBOL]  # ^GSPC, ^DJI, ^IXIC, ^VIX

# COMMAND ----------

# --- F16/F18: Real-time index + VIX quotes ---
print("Fetching index + VIX quotes...")
quotes = client.get_index_quote(ALL_INDEX_SYMBOLS)
quotes_df = pd.DataFrame(quotes)
print(f"  {len(quotes_df)} index records")

# COMMAND ----------

# --- F17/F18: Historical prices for all four symbols ---
from_date = HISTORY_START_DATE
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")

hist_frames = []
for symbol in ALL_INDEX_SYMBOLS:
    try:
        rows = client.get_index_historical(symbol, from_date, to_date)
        df = pd.DataFrame(rows)
        df["symbol"] = symbol
        hist_frames.append(df)
        print(f"  {symbol}: {len(df)} days")
    except Exception as e:
        print(f"  {symbol}: ERROR — {e}")

hist_df = pd.concat(hist_frames, ignore_index=True) if hist_frames else pd.DataFrame()

# COMMAND ----------

# --- F19: Index constituents (S&P 500, Nasdaq 100, Dow Jones) ---
print("\nFetching index constituents...")
sp500    = pd.DataFrame(client.get_sp500_constituents());    sp500["index"]   = "SP500"
nasdaq   = pd.DataFrame(client.get_nasdaq_constituents());   nasdaq["index"]  = "NASDAQ100"
dowjones = pd.DataFrame(client.get_dowjones_constituents()); dowjones["index"] = "DJIA"
constituents_df = pd.concat([sp500, nasdaq, dowjones], ignore_index=True)
print(f"  Total constituents: {len(constituents_df)}")

# COMMAND ----------

out_dir = volume_subdir("indexes")
os.makedirs(out_dir, exist_ok=True)

for df, filename in [
    (quotes_df,       "index_quotes"),
    (hist_df,         "index_history"),
    (constituents_df, "index_constituents"),
]:
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    out_path = f"{out_dir}/{filename}.json"
    df.to_json(out_path, orient="records", indent=2)
    print(f"Written {len(df)} rows to {out_path}")
