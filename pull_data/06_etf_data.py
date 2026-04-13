# Databricks notebook source
# COMMAND ----------

# Pull ETF information, holdings, and sector weightings.
# Output: UC_VOLUME_PATH/etf_data/{TICKER}/{etf_info,etf_holdings,etf_sectors}.json
# FMP Sources: F9/F10/F11

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

out_base = volume_subdir("etf_data")

for etf in ETF_TICKERS:
    print(f"Fetching ETF data for {etf}...")
    try:
        info     = client.get_etf_info(etf)
        holdings = client.get_etf_holdings(etf)
        sectors  = client.get_etf_sector_weightings(etf)

        etf_dir = f"{out_base}/{etf}"
        os.makedirs(etf_dir, exist_ok=True)
        ingested_at = pd.Timestamp.now().isoformat()

        idf = pd.DataFrame([info] if isinstance(info, dict) else info)
        idf["symbol"] = etf; idf["ingested_at"] = ingested_at
        idf.to_json(f"{etf_dir}/etf_info.json", orient="records", indent=2)

        hdf = pd.DataFrame(holdings)
        hdf["etf_symbol"] = etf; hdf["ingested_at"] = ingested_at
        hdf.to_json(f"{etf_dir}/etf_holdings.json", orient="records", indent=2)
        print(f"  {etf}: {len(hdf)} holdings")

        sdf = pd.DataFrame(sectors)
        sdf["etf_symbol"] = etf; sdf["ingested_at"] = ingested_at
        sdf.to_json(f"{etf_dir}/etf_sectors.json", orient="records", indent=2)
        print(f"  {etf}: {len(sdf)} sector buckets")

    except Exception as e:
        print(f"  {etf}: ERROR — {e}")
