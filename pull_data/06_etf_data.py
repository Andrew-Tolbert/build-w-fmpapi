# Databricks notebook source
# COMMAND ----------

# Pull ETF information, holdings, and sector weightings.
# Output: UC_VOLUME_PATH/etf_data/{etf_info,etf_holdings,etf_sectors}.json
# FMP Sources: F9/F10/F11

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

info_frames    = []
holdings_frames = []
sector_frames  = []

for etf in ETF_TICKERS:
    print(f"Fetching ETF data for {etf}...")
    try:
        # ETF info
        info = client.get_etf_info(etf)
        idf = pd.DataFrame([info] if isinstance(info, dict) else info)
        idf["symbol"] = etf
        info_frames.append(idf)

        # ETF holdings
        holdings = client.get_etf_holdings(etf)
        hdf = pd.DataFrame(holdings)
        hdf["etf_symbol"] = etf
        holdings_frames.append(hdf)
        print(f"  {etf}: {len(hdf)} holdings")

        # Sector weightings
        sectors = client.get_etf_sector_weightings(etf)
        sdf = pd.DataFrame(sectors)
        sdf["etf_symbol"] = etf
        sector_frames.append(sdf)
        print(f"  {etf}: {len(sdf)} sector buckets")

    except Exception as e:
        print(f"  {etf}: ERROR — {e}")

# COMMAND ----------

out_dir = volume_subdir("etf_data")
os.makedirs(out_dir, exist_ok=True)

for df, filename in [
    (pd.concat(info_frames,     ignore_index=True) if info_frames     else pd.DataFrame(), "etf_info"),
    (pd.concat(holdings_frames, ignore_index=True) if holdings_frames else pd.DataFrame(), "etf_holdings"),
    (pd.concat(sector_frames,   ignore_index=True) if sector_frames   else pd.DataFrame(), "etf_sectors"),
]:
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    out_path = f"{out_dir}/{filename}.json"
    df.to_json(out_path, orient="records", indent=2)
    print(f"Written {len(df)} rows to {out_path}")
