# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull analyst estimates, price target consensus, and analyst ratings.
# Output: UC_VOLUME_PATH/analyst_data/{TICKER}/{ts}_{analyst_estimates,price_targets,analyst_ratings}.json
# FMP Sources: F12/F13/F14

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

apply_full_refresh("analyst_data")

# COMMAND ----------

out_base = volume_subdir("analyst_data")
_ts = ts_prefix()

for ticker in get_tickers():
    try:
        est = client.get_analyst_estimates(ticker, period="quarterly",limit=25)
        pt  = client.get_price_target_consensus(ticker)
        gr  = client.get_grades_summary(ticker)

        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        ingested_at = pd.Timestamp.now().isoformat()

        edf = pd.DataFrame(est)
        edf["symbol"] = ticker; edf["ingested_at"] = ingested_at
        edf.to_json(f"{ticker_dir}/{_ts}_analyst_estimates.json", orient="records", indent=2)

        tdf = pd.DataFrame([pt] if isinstance(pt, dict) else pt)
        tdf["symbol"] = ticker; tdf["ingested_at"] = ingested_at
        tdf.to_json(f"{ticker_dir}/{_ts}_price_targets.json", orient="records", indent=2)

        gdf = pd.DataFrame([gr] if isinstance(gr, dict) else gr)
        gdf["symbol"] = ticker; gdf["ingested_at"] = ingested_at
        gdf.to_json(f"{ticker_dir}/{_ts}_analyst_ratings.json", orient="records", indent=2)

        print(f"  {ticker}: {len(edf)} estimate rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
