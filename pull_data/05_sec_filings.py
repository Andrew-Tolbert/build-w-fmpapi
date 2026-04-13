# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull SEC filing metadata and download raw filing documents to UC Volume.
# Focuses on BDC anchor tickers (AINV, OCSL) for covenant analysis.
# Output: UC_VOLUME_PATH/sec_filings/{TICKER}/{ts}_metadata.json + {ts}_{type}_{date}.htm
# FMP Sources: F8/D1 — /stable/sec-filings

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd
import requests

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
# clear_directory(volume_subdir("sec_filings"))

# COMMAND ----------

# Filing types to ingest — 10-Q and 8-K for covenant analysis; also 10-K annual
FILING_TYPES = ["10-Q", "8-K", "10-K"]
FILING_LIMIT  = 10  # most recent N per type per ticker

# COMMAND ----------

out_base = volume_subdir("sec_filings")
_ts = ts_prefix()

for ticker in BDC_TICKERS:
    ticker_dir = f"{out_base}/{ticker}"
    os.makedirs(ticker_dir, exist_ok=True)

    ticker_metadata = []
    for ftype in FILING_TYPES:
        try:
            rows = client.get_sec_filings(ticker, filing_type=ftype, limit=FILING_LIMIT)
            for row in rows:
                row["requested_type"] = ftype
            ticker_metadata.extend(rows)
            print(f"  {ticker} {ftype}: {len(rows)} filings")
        except Exception as e:
            print(f"  {ticker} {ftype}: ERROR — {e}")

    if ticker_metadata:
        metadata_df = pd.DataFrame(ticker_metadata)
        metadata_df["ingested_at"] = pd.Timestamp.now().isoformat()
        metadata_df.to_json(f"{ticker_dir}/{_ts}_metadata.json", orient="records", indent=2)
        print(f"  {ticker}: written {len(metadata_df)} metadata rows")

        downloaded = 0
        for row in ticker_metadata:
            final_link = row.get("finalLink") or row.get("link")
            if not final_link:
                continue
            ftype    = row.get("type", "UNKNOWN")
            date_str = str(row.get("fillingDate", ""))[:10]
            filename = f"{_ts}_{ftype}_{date_str}.htm".replace("/", "-")
            dest     = os.path.join(ticker_dir, filename)

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

        print(f"  {ticker}: {downloaded} new filing documents downloaded")
