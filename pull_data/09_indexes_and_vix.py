# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull index quotes, historical index prices, VIX, and index constituents.
# Output: UC_VOLUME_PATH/indexes/{SYMBOL}/{ts}_{quote,history,constituents}.json
# FMP Sources: F16/F17/F18/F19
# Symbols: ^GSPC (S&P 500), ^DJI (Dow Jones), ^IXIC (Nasdaq), ^VIX

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

ALL_INDEX_SYMBOLS = INDEX_SYMBOLS + [VIX_SYMBOL]  # ^GSPC, ^DJI, ^IXIC, ^VIX
out_base = volume_subdir("indexes")
_ts = ts_prefix()

# Uncomment to wipe all data for this feed before re-ingesting:
# clear_directory(volume_subdir("indexes"))

def clean_symbol(s):
    """Strip ^ for use as a directory name (e.g. ^GSPC → GSPC)."""
    return s.lstrip("^")

# COMMAND ----------

# --- F16/F18: Real-time index + VIX quotes — write one file per symbol ---
print("Fetching index + VIX quotes...")
quotes = client.get_index_quote(ALL_INDEX_SYMBOLS)
quotes_by_symbol = {q["symbol"]: q for q in quotes} if quotes else {}

for symbol in ALL_INDEX_SYMBOLS:
    sym_dir = f"{out_base}/{clean_symbol(symbol)}"
    os.makedirs(sym_dir, exist_ok=True)
    record = quotes_by_symbol.get(symbol, {})
    df = pd.DataFrame([record] if record else [])
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    df.to_json(f"{sym_dir}/{_ts}_quote.json", orient="records", indent=2)
    print(f"  {symbol}: quote written")

# COMMAND ----------

# --- F17/F18: Historical prices per symbol ---
from_date = HISTORY_START_DATE
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")

for symbol in ALL_INDEX_SYMBOLS:
    try:
        rows = client.get_index_historical(symbol, from_date, to_date)
        df = pd.DataFrame(rows)
        df["symbol"] = symbol
        df["ingested_at"] = pd.Timestamp.now().isoformat()
        sym_dir = f"{out_base}/{clean_symbol(symbol)}"
        os.makedirs(sym_dir, exist_ok=True)
        df.to_json(f"{sym_dir}/{_ts}_history.json", orient="records", indent=2)
        print(f"  {symbol}: {len(df)} days")
    except Exception as e:
        print(f"  {symbol}: ERROR — {e}")

# COMMAND ----------

# --- F19: Index constituents — one file per index ---
print("\nFetching index constituents...")
for fetch_fn, index_name in [
    (client.get_sp500_constituents,    "SP500"),
    (client.get_nasdaq_constituents,   "NASDAQ100"),
    (client.get_dowjones_constituents, "DJIA"),
]:
    try:
        df = pd.DataFrame(fetch_fn())
        df["index"] = index_name
        df["ingested_at"] = pd.Timestamp.now().isoformat()
        idx_dir = f"{out_base}/{index_name}"
        os.makedirs(idx_dir, exist_ok=True)
        df.to_json(f"{idx_dir}/{_ts}_constituents.json", orient="records", indent=2)
        print(f"  {index_name}: {len(df)} constituents")
    except Exception as e:
        print(f"  {index_name}: ERROR — {e}")
