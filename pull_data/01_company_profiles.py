# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "requests",
# ]
# ///
# Pull FMP company profiles for all tickers in the watchlist.
# Output: UC_VOLUME_PATH/company_profiles/{TICKER}/profile.json
# FMP Source: F1 — /stable/profile

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd
from datetime import datetime, timedelta


client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

out_base = volume_subdir("company_profiles")
print(f"Fetching profiles for {len(EQUITY_TICKERS)} tickers...")

for ticker in EQUITY_TICKERS:
    try:
        ticker_dir = f"{out_base}/{ticker}"
        profile_path = f"{ticker_dir}/profile.json"
        exists_and_recent = False
        try:
            mod_time = dbutils.fs.ls(ticker_dir)[0].modificationTime / 1000
            is_recent = datetime.fromtimestamp(mod_time) > datetime.now() - timedelta(days=30)
            if is_recent:
                print(f"  {ticker}: already exists and is recent, skipping")
                exists_and_recent = True
        except Exception:
            pass
        if exists_and_recent:
            continue
        profile = client.get_profile(ticker)
        os.makedirs(ticker_dir, exist_ok=True)
        df = pd.DataFrame([profile] if isinstance(profile, dict) else profile)
        df["ingested_at"] = pd.Timestamp.now().isoformat()
        df.to_json(profile_path, orient="records", indent=2)
        print(f"  {ticker}: written")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
