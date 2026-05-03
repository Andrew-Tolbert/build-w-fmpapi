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

apply_full_refresh("sec_filings")


# COMMAND ----------

# Create download log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log (
        symbol        STRING,
        form_type     STRING,
        accession     STRING,
        filing_date   STRING,
        fiscal_year   STRING,
        quarter       INT,
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


_PERIOD_TO_QUARTER = {"Q1": 1, "Q2": 2, "Q3": 3, "Q4": 4, "FY": 4}

# Pre-load fiscal period mapping from bronze_income_statements once per run.
# One Spark scan instead of one query per filing — ~100x faster for a full batch.
#
# _filing_date_to_period: (symbol, filingDate) -> (fiscalYear, quarter)
#   Used for 10-K and 10-Q — the income statement records the exact SEC filing date.
#
# _period_by_symbol: symbol -> [(period_end_date, fiscalYear, quarter), ...]  sorted DESC
#   Used for 8-K, 424B2, 424B5 — find the most recent period whose end date is
#   on or before the filing date (the quarter the event occurred in).

_filing_date_to_period = {}
_period_by_symbol      = {}

for row in spark.sql(f"""
    SELECT symbol, filingDate, date, fiscalYear, period
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements
    WHERE fiscalYear IS NOT NULL
""").collect():
    sym = row["symbol"]
    fy  = row["fiscalYear"]
    qtr = _PERIOD_TO_QUARTER.get(row["period"])
    if row["filingDate"]:
        _filing_date_to_period[(sym, row["filingDate"])] = (fy, qtr)
    if sym not in _period_by_symbol:
        _period_by_symbol[sym] = []
    _period_by_symbol[sym].append((row["date"], fy, qtr))

# Sort each symbol's list newest-first so the first match is the most recent
for sym in _period_by_symbol:
    _period_by_symbol[sym].sort(key=lambda x: x[0], reverse=True)

print(f"Loaded fiscal period map: {len(_filing_date_to_period)} filingDate entries, {len(_period_by_symbol)} symbols")


def _get_fiscal_period(ticker: str, filing_date: str, form_type: str):
    """Return (fiscal_year, quarter) from the pre-loaded mapping — no Spark calls.

    10-K / 10-Q: exact filingDate match from bronze_income_statements.
    8-K / 424Bx: nearest prior quarter (most recent period end <= filing_date).
    Returns (None, None) when no match exists (ticker not in income statements).
    """
    if form_type in ("10-K", "10-Q"):
        return _filing_date_to_period.get((ticker, filing_date), (None, None))

    periods = _period_by_symbol.get(ticker, [])
    for period_end, fy, qtr in periods:
        if str(period_end) <= filing_date:
            return fy, qtr
    return None, None


out_base  = volume_subdir("sec_filings")
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")
from_date = HISTORY_START_DATE  # wide enough window to find multiple annual 10-Ks

for ticker in get_tickers(types=["equity", "private_credit"]):
    target_forms = TICKER_CONFIG.get(ticker, {}).get("sec_forms")
    if not target_forms:
        print(f"  {ticker}: no SEC filing config, skipping")
        continue

    try:
        filings_by_type = client.get_sec_filings_paginated(
            ticker, from_date=from_date, to_date=to_date, target_forms=target_forms
        )
        counts = {ft: len(v) for ft, v in filings_by_type.items()}
        print(f"  {ticker}: found {counts}")
    except Exception as e:
        print(f"  {ticker}: ERROR fetching filings — {e}")
        continue

    selected = [row for filings in filings_by_type.values() for row in filings]

    for row in selected:
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

        # Derive subdir from form type: "10-K" → "10k", "8-K" → "8k", etc.
        subdir     = ftype.lower().replace("-", "")
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

        fiscal_year, quarter = _get_fiscal_period(ticker, filing_date, ftype)
        _fy_sql  = f"'{fiscal_year}'" if fiscal_year else "NULL"
        _qtr_sql = str(quarter)       if quarter     else "NULL"
        print(f"  {ticker} {ftype} {filing_date}: written {subdir}/{ticker}/{filename} (fy={fiscal_year} q={quarter})")

        spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log AS log
            USING (
                SELECT
                    '{ticker}'      AS symbol,
                    '{ftype}'       AS form_type,
                    '{accession}'   AS accession,
                    '{filing_date}' AS filing_date,
                    {_fy_sql}       AS fiscal_year,
                    {_qtr_sql}      AS quarter,
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
                    log.fiscal_year   = src.fiscal_year,
                    log.quarter       = src.quarter,
                    log.final_link    = src.final_link,
                    log.subdir        = src.subdir,
                    log.filename      = src.filename,
                    log.downloaded_at = src.downloaded_at
            WHEN NOT MATCHED THEN
                INSERT *
        """)

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.sec_filings_log"))
