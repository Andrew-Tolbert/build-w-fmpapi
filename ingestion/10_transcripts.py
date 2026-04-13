# Databricks notebook source
# COMMAND ----------

# Ingest earnings call transcripts for BDC anchor tickers (AINV, OCSL).
# Stores full text in UC table and raw text files in UC Volume for RAG indexing.
# Target table: uc.wealth.transcripts
# Target volume: UC_VOLUME_PATH/transcripts/
# FMP Source: D3 — /stable/earning-call-transcript

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

# Fetch last 4 quarters for each BDC ticker (2 years of transcripts)
import datetime
current_year = datetime.date.today().year
QUARTERS_TO_FETCH = [
    (current_year,     1), (current_year,     2), (current_year,     3), (current_year,     4),
    (current_year - 1, 1), (current_year - 1, 2), (current_year - 1, 3), (current_year - 1, 4),
]

# COMMAND ----------

transcript_records = []
volume_dir = f"{UC_VOLUME_PATH}/transcripts"
os.makedirs(volume_dir, exist_ok=True)

for ticker in BDC_TICKERS:
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
            # Save raw text to Volume for vector indexing
            filename = f"{ticker}_Q{quarter}_{year}.txt"
            with open(os.path.join(volume_dir, filename), "w", encoding="utf-8") as f:
                f.write(record.get("content", ""))
            print(f"  {ticker} Q{quarter} {year}: saved")
        except Exception as e:
            print(f"  {ticker} Q{quarter} {year}: {e}")

print(f"\nTotal transcripts: {len(transcript_records)}")

# COMMAND ----------

target = uc_table("transcripts")
transcripts_df = pd.DataFrame(transcript_records)
sdf = spark.createDataFrame(transcripts_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
print(f"Raw text files saved to: {volume_dir}")
