# Databricks notebook source
# COMMAND ----------

# Pull earnings call transcripts for BDC anchor tickers (AINV, OCSL).
# Output: UC_VOLUME_PATH/transcripts/{TICKER}/metadata.json + raw .txt files
# FMP Source: D3 — /stable/earning-call-transcript

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import datetime
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

# Fetch last 4 quarters for each BDC ticker (2 years of transcripts)
current_year = datetime.date.today().year
QUARTERS_TO_FETCH = [
    (current_year,     1), (current_year,     2), (current_year,     3), (current_year,     4),
    (current_year - 1, 1), (current_year - 1, 2), (current_year - 1, 3), (current_year - 1, 4),
]

# COMMAND ----------

out_base = volume_subdir("transcripts")

for ticker in BDC_TICKERS:
    ticker_dir = f"{out_base}/{ticker}"
    os.makedirs(ticker_dir, exist_ok=True)

    transcript_records = []
    for year, quarter in QUARTERS_TO_FETCH:
        try:
            data = client.get_transcript(ticker, year=year, quarter=quarter)
            if not data:
                continue
            record = data if isinstance(data, dict) else data[0] if data else None
            if not record or not record.get("content"):
                continue
            transcript_records.append({
                "symbol":  ticker,
                "year":    year,
                "quarter": quarter,
                "date":    record.get("date"),
                "content": record.get("content", ""),
            })
            filename = f"Q{quarter}_{year}.txt"
            with open(os.path.join(ticker_dir, filename), "w", encoding="utf-8") as f:
                f.write(record.get("content", ""))
            print(f"  {ticker} Q{quarter} {year}: saved")
        except Exception as e:
            print(f"  {ticker} Q{quarter} {year}: {e}")

    df = pd.DataFrame(transcript_records)
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    df.to_json(f"{ticker_dir}/metadata.json", orient="records", indent=2)
    print(f"  {ticker}: {len(df)} transcripts written to {ticker_dir}")
