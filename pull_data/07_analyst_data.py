# Databricks notebook source
# COMMAND ----------

# Pull analyst estimates, price target consensus, and analyst ratings.
# Output: UC_VOLUME_PATH/analyst_data/{analyst_estimates,price_targets,analyst_ratings}.json
# FMP Sources: F12/F13/F14

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

estimates_frames = []
targets_frames   = []
ratings_frames   = []

for ticker in EQUITY_TICKERS:
    try:
        est = client.get_analyst_estimates(ticker, limit=8)
        edf = pd.DataFrame(est); edf["symbol"] = ticker; estimates_frames.append(edf)

        pt = client.get_price_target_consensus(ticker)
        tdf = pd.DataFrame([pt] if isinstance(pt, dict) else pt)
        tdf["symbol"] = ticker; targets_frames.append(tdf)

        gr = client.get_grades_summary(ticker)
        gdf = pd.DataFrame([gr] if isinstance(gr, dict) else gr)
        gdf["symbol"] = ticker; ratings_frames.append(gdf)

        print(f"  {ticker}: {len(edf)} estimate rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

# COMMAND ----------

out_dir = volume_subdir("analyst_data")
os.makedirs(out_dir, exist_ok=True)

for df, filename in [
    (pd.concat(estimates_frames, ignore_index=True) if estimates_frames else pd.DataFrame(), "analyst_estimates"),
    (pd.concat(targets_frames,   ignore_index=True) if targets_frames   else pd.DataFrame(), "price_targets"),
    (pd.concat(ratings_frames,   ignore_index=True) if ratings_frames   else pd.DataFrame(), "analyst_ratings"),
]:
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    out_path = f"{out_dir}/{filename}.json"
    df.to_json(out_path, orient="records", indent=2)
    print(f"Written {len(df)} rows to {out_path}")
