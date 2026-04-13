# Databricks notebook source
# COMMAND ----------

# Pull FMP company profiles for all tickers in the watchlist.
# Output: UC_VOLUME_PATH/company_profiles/company_profiles.json
# FMP Source: F1 — /stable/profile

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

print(f"Fetching profiles for {len(EQUITY_TICKERS)} tickers...")
raw = client.get_profiles(EQUITY_TICKERS)

df = pd.DataFrame(raw) if isinstance(raw, list) else pd.DataFrame([raw])
df["ingested_at"] = pd.Timestamp.now().isoformat()
print(f"  Received {len(df)} records")

# COMMAND ----------

out_dir = volume_subdir("company_profiles")
os.makedirs(out_dir, exist_ok=True)
out_path = f"{out_dir}/company_profiles.json"

df.to_json(out_path, orient="records", indent=2)
print(f"Written {len(df)} records to {out_path}")
