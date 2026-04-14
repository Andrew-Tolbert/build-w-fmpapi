# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull SEC filing metadata from FMP and download raw filing documents to UC Volume.
# 10-K / 10-Q filings → {out_base}/10k/{ticker}/
# 8-K filings         → {out_base}/8k/{ticker}/
# A Delta table (sec_filings_log) tracks what has been downloaded so re-runs are
# idempotent — only new filings are fetched.
# FMP Source: /stable/sec-filings-search/symbol

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

# MAGIC %run ../../utils/sec_downloader

# COMMAND ----------

import os
import re
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
#clear_directory(volume_subdir("sec_filings"))


# COMMAND ----------

# Filing types to ingest — 10-K and 8-K for covenant / material-event analysis
FILING_TYPES = ["10-K", "10-Q", "8-K"]
_10K_TYPES   = {"10-K", "10-Q"}
_8K_TYPES    = {"8-K"}

# Date range — pull from history start through today
TO_DATE = pd.Timestamp.now().strftime("%Y-%m-%d")

# COMMAND ----------

# Create download log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log (
        symbol        STRING,
        form_type     STRING,
        accession     STRING,
        filing_date   STRING,
        link          STRING,
        final_link    STRING,
        subdir        STRING,
        filename      STRING,
        downloaded_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

def _accession_id(url: str) -> str:
    """Extract the 18-digit EDGAR accession number from a filing URL."""
    m = re.search(r"/(\d{18})/", url or "")
    return m.group(1) if m else "NOACC"


out_base = volume_subdir("sec_filings")

for ticker in BDC_TICKERS:
    try:
        all_filings = client.get_sec_filings(ticker, from_date=HISTORY_START_DATE, to_date=TO_DATE)
        matching    = [f for f in all_filings if f.get("formType") in FILING_TYPES]
        print(f"  {ticker}: {len(all_filings)} total filings, {len(matching)} matching {FILING_TYPES}")
    except Exception as e:
        print(f"  {ticker}: ERROR fetching filings — {e}")
        continue

    for row in matching:
        ftype       = row.get("formType", "UNKNOWN")
        filing_date = str(row.get("filingDate", ""))[:10]
        link        = row.get("link", "")
        final_link  = row.get("finalLink") or link
        if not final_link:
            continue

        accession = _accession_id(link or final_link)

        # Skip if already in the log
        already_downloaded = spark.sql(f"""
            SELECT 1 FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log
            WHERE symbol = '{ticker}' AND accession = '{accession}'
        """).count() > 0
        if already_downloaded:
            print(f"  {ticker} {ftype} {filing_date}: already downloaded, skipping")
            continue

        # Route to the correct subdirectory based on form type
        subdir     = "10k" if ftype in _10K_TYPES else "8k"
        ticker_dir = f"{out_base}/{subdir}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)

        # Stable filename — accession number guarantees uniqueness across re-runs
        filename = f"{ftype}_{filing_date}_{accession}.htm".replace("/", "-")
        dest     = f"{ticker_dir}/{filename}"

        text = fetch_sec_document(final_link)
        if text is None:
            print(f"  {ticker} {ftype} {filing_date}: download failed, skipping")
            continue

        with open(dest, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"  {ticker} {ftype} {filing_date}: written {subdir}/{ticker}/{filename}")

        spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log AS log
            USING (
                SELECT
                    '{ticker}'      AS symbol,
                    '{ftype}'       AS form_type,
                    '{accession}'   AS accession,
                    '{filing_date}' AS filing_date,
                    '{link}'        AS link,
                    '{final_link}'  AS final_link,
                    '{subdir}'      AS subdir,
                    '{filename}'    AS filename,
                    current_timestamp() AS downloaded_at
            ) AS src
            ON  log.symbol    = src.symbol
            AND log.accession = src.accession
            WHEN MATCHED THEN
                UPDATE SET
                    log.final_link    = src.final_link,
                    log.subdir        = src.subdir,
                    log.filename      = src.filename,
                    log.downloaded_at = src.downloaded_at
            WHEN NOT MATCHED THEN
                INSERT *
        """)

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.sec_filings_log"))

# COMMAND ----------

# spark.sql(f"DROP TABLE {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log")
