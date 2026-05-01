# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull earnings call transcripts for BDC anchor tickers.
# Each transcript is saved as a JSON file containing all metadata + content.
# A Delta table (transcripts_log) tracks what has been downloaded so re-runs are
# idempotent — only new transcripts are fetched.
# Output: UC_VOLUME_PATH/transcripts/{TICKER}/Q{q}_{y}.json
# FMP Source: D3 — /stable/earning-call-transcript

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import json
import datetime

client = FMPClient(api_key=FMP_API_KEY)

apply_full_refresh("transcripts")

# Build company-name lookup once — used to populate company_name in each payload.
# bronze_company_profiles is the canonical source; fall back to the ticker if missing.
_company_name_map = {
    row["symbol"]: row["companyName"]
    for row in spark.sql(f"""
        SELECT symbol, companyName
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles
        WHERE companyName IS NOT NULL
    """).collect()
}

# COMMAND ----------

# Fetch last 4 quarters for each BDC ticker (2 years of transcripts)
current_year = datetime.date.today().year
QUARTERS_TO_FETCH = [
    (current_year,     1), (current_year,     2), (current_year,     3), (current_year,     4),
    (current_year - 1, 1), (current_year - 1, 2), (current_year - 1, 3), (current_year - 1, 4),
]

# COMMAND ----------

# Create download log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.transcripts_log (
        symbol        STRING,
        year          INT,
        quarter       INT,
        filename      STRING,
        downloaded_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

out_base = volume_subdir("transcripts")

for ticker in get_tickers():
    ticker_dir = f"{out_base}/{ticker}"
    os.makedirs(ticker_dir, exist_ok=True)

    for year, quarter in QUARTERS_TO_FETCH:
        # Skip if already in the log
        already_downloaded = spark.sql(f"""
            SELECT 1 FROM {UC_CATALOG}.{UC_SCHEMA}.transcripts_log
            WHERE symbol = '{ticker}' AND year = {year} AND quarter = {quarter}
        """).count() > 0
        if already_downloaded:
            print(f"  {ticker} Q{quarter} {year}: already downloaded, skipping")
            continue

        try:
            data = client.get_transcript(ticker, year=year, quarter=quarter)
            if not data:
                continue
            record = data if isinstance(data, dict) else data[0] if data else None
            if not record or not record.get("content"):
                continue

            company_name = _company_name_map.get(ticker, ticker)
            payload = {
                "symbol":       ticker,
                "year":         year,
                "quarter":      quarter,
                "date":         record.get("date"),
                "company_name": company_name,
                # FMP's earning-call-transcript endpoint does not return a title field.
                "title":        record.get("title") or f"{company_name} Q{quarter} {year} Earnings Call",
                "content":      record.get("content", ""),
            }

            filename = f"Q{quarter}_{year}.json"
            dest     = os.path.join(ticker_dir, filename)

            with open(dest, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
            print(f"  {ticker} Q{quarter} {year}: saved {filename}")

            spark.sql(f"""
                MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.transcripts_log AS log
                USING (
                    SELECT
                        '{ticker}'  AS symbol,
                        {year}      AS year,
                        {quarter}   AS quarter,
                        '{filename}' AS filename,
                        current_timestamp() AS downloaded_at
                ) AS src
                ON  log.symbol  = src.symbol
                AND log.year    = src.year
                AND log.quarter = src.quarter
                WHEN MATCHED THEN
                    UPDATE SET
                        log.filename      = src.filename,
                        log.downloaded_at = src.downloaded_at
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
        except Exception as e:
            print(f"  {ticker} Q{quarter} {year}: {e}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.transcripts_log"))

# COMMAND ----------

# spark.sql(f"DROP TABLE {UC_CATALOG}.{UC_SCHEMA}.transcripts_log")
