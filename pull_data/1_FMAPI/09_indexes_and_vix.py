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

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

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
