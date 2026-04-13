# Databricks notebook source
# COMMAND ----------

# Pull SEC filing metadata and download raw filing documents to UC Volume.
# Focuses on BDC anchor tickers (AINV, OCSL) for covenant analysis.
# Output: UC_VOLUME_PATH/sec_filings/metadata.json + raw .htm files
# FMP Sources: F8/D1 — /stable/sec-filings

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd
import requests

client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

# Filing types to ingest — 10-Q and 8-K for covenant analysis; also 10-K annual
FILING_TYPES = ["10-Q", "8-K", "10-K"]
FILING_LIMIT  = 10  # most recent N per type per ticker

# COMMAND ----------

all_metadata = []

for ticker in BDC_TICKERS:
    for ftype in FILING_TYPES:
        try:
            rows = client.get_sec_filings(ticker, filing_type=ftype, limit=FILING_LIMIT)
            df = pd.DataFrame(rows)
            df["requested_type"] = ftype
            all_metadata.append(df)
            print(f"  {ticker} {ftype}: {len(df)} filings")
        except Exception as e:
            print(f"  {ticker} {ftype}: ERROR — {e}")

metadata_df = pd.concat(all_metadata, ignore_index=True) if all_metadata else pd.DataFrame()
metadata_df["ingested_at"] = pd.Timestamp.now().isoformat()
print(f"\nTotal filing metadata rows: {len(metadata_df)}")

# COMMAND ----------

volume_dir = volume_subdir("sec_filings")
os.makedirs(volume_dir, exist_ok=True)

# Write metadata as JSON
metadata_path = f"{volume_dir}/metadata.json"
metadata_df.to_json(metadata_path, orient="records", indent=2)
print(f"Written {len(metadata_df)} metadata rows to {metadata_path}")

downloaded = 0
for _, row in metadata_df.iterrows():
    final_link = row.get("finalLink") or row.get("link")
    if not final_link:
        continue
    ticker   = row.get("symbol", "UNKNOWN")
    ftype    = row.get("type", "UNKNOWN")
    date_str = str(row.get("fillingDate", ""))[:10]
    filename = f"{ticker}_{ftype}_{date_str}.htm".replace("/", "-")
    dest     = os.path.join(volume_dir, filename)

    if os.path.exists(dest):
        print(f"  Skipping (exists): {filename}")
        continue
    try:
        resp = requests.get(final_link, timeout=30)
        resp.raise_for_status()
        with open(dest, "w", encoding="utf-8") as f:
            f.write(resp.text)
        downloaded += 1
        print(f"  Downloaded: {filename}")
    except Exception as e:
        print(f"  Failed to download {filename}: {e}")

print(f"\nDownloaded {downloaded} new filing documents to {volume_dir}")
