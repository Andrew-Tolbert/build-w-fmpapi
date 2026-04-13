# Databricks notebook source
# COMMAND ----------

# Pull key metrics and financial ratios for all watchlist tickers.
# These provide pre-computed covenant proxies: netDebtToEBITDA, interestCoverage.
# Output: UC_VOLUME_PATH/key_metrics/{key_metrics,financial_ratios}.json
# FMP Sources: F6/F7

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

metrics_frames = []
ratios_frames  = []

for ticker in EQUITY_TICKERS:
    try:
        m = client.get_key_metrics(ticker, period="quarterly", limit=12)
        r = client.get_ratios(ticker, period="quarterly", limit=12)
        mdf = pd.DataFrame(m); mdf["symbol"] = ticker; metrics_frames.append(mdf)
        rdf = pd.DataFrame(r); rdf["symbol"] = ticker; ratios_frames.append(rdf)
        print(f"  {ticker}: {len(mdf)} metrics rows, {len(rdf)} ratio rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

metrics_combined = pd.concat(metrics_frames, ignore_index=True) if metrics_frames else pd.DataFrame()
ratios_combined  = pd.concat(ratios_frames,  ignore_index=True) if ratios_frames  else pd.DataFrame()

# COMMAND ----------

out_dir = volume_subdir("key_metrics")
os.makedirs(out_dir, exist_ok=True)

for df, filename in [
    (metrics_combined, "key_metrics"),
    (ratios_combined,  "financial_ratios"),
]:
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    out_path = f"{out_dir}/{filename}.json"
    df.to_json(out_path, orient="records", indent=2)
    print(f"Written {len(df)} rows to {out_path}")
