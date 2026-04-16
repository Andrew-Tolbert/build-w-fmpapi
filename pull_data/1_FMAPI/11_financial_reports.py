# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull structured 10-K JSON reports from FMP for all watchlist tickers.
# A Delta table (financial_reports_log) tracks what has already been downloaded
# so re-runs are idempotent — only new periods are fetched.
# Output: UC_VOLUME_PATH/financial_reports/{TICKER}/{year}_{period}.json
# FMP Sources: /stable/financial-reports-dates, /stable/financial-reports-json

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import json

client = FMPClient(api_key=FMP_API_KEY)

apply_full_refresh("financial_reports")

# COMMAND ----------

# Create download log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.financial_reports_log (
        symbol        STRING,
        fiscal_year   STRING,
        period        STRING,
        link_json     STRING,
        link_xlsx     STRING,
        filename      STRING,
        downloaded_at TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

out_base = volume_subdir("financial_reports")

for ticker in EQUITY_TICKERS:
    try:
        dates = client.get_financial_report_dates(ticker)
        # Sort by fiscal year desc, then period desc (Q4 > Q3 > Q2 > Q1 > FY), take most recent 5
        period_order = {"Q4": 4, "Q3": 3, "Q2": 2, "Q1": 1, "FY": 0}
        dates = sorted(
            dates,
            key=lambda x: (int(x.get("fiscalYear", 0)), period_order.get(x.get("period", ""), -1)),
            reverse=True
        )[:5]
        print(f"  {ticker}: {len(dates)} report periods (most recent 5)")
    except Exception as e:
        print(f"  {ticker}: ERROR fetching report dates — {e}")
        continue

    ticker_dir = f"{out_base}/{ticker}"
    os.makedirs(ticker_dir, exist_ok=True)

    for entry in dates:
        fiscal_year = str(entry.get("fiscalYear", "")).strip()
        period      = str(entry.get("period", "")).strip()
        link_json   = str(entry.get("linkJson", "")).strip()
        link_xlsx   = str(entry.get("linkXlsx", "")).strip()
        if not fiscal_year or not period:
            continue

        # Skip if already logged
        already_downloaded = spark.sql(f"""
            SELECT 1 FROM {UC_CATALOG}.{UC_SCHEMA}.financial_reports_log
            WHERE symbol = '{ticker}' AND fiscal_year = '{fiscal_year}' AND period = '{period}'
        """).count() > 0
        if already_downloaded:
            print(f"  {ticker} {fiscal_year} {period}: already downloaded, skipping")
            continue

        try:
            data     = client.get_financial_report_json(ticker, year=fiscal_year, period=period)
            filename = f"{fiscal_year}_{period}.json"
            dest     = f"{ticker_dir}/{filename}"

            with open(dest, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"  {ticker} {fiscal_year} {period}: written {filename}")

            spark.sql(f"""
                MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.financial_reports_log AS log
                USING (
                    SELECT
                        '{ticker}'     AS symbol,
                        '{fiscal_year}' AS fiscal_year,
                        '{period}'     AS period,
                        '{link_json}'  AS link_json,
                        '{link_xlsx}'  AS link_xlsx,
                        '{filename}'   AS filename,
                        current_timestamp() AS downloaded_at
                ) AS src
                ON  log.symbol      = src.symbol
                AND log.fiscal_year = src.fiscal_year
                AND log.period      = src.period
                WHEN MATCHED THEN
                    UPDATE SET
                        log.link_json     = src.link_json,
                        log.link_xlsx     = src.link_xlsx,
                        log.filename      = src.filename,
                        log.downloaded_at = src.downloaded_at
                WHEN NOT MATCHED THEN
                    INSERT *
            """)
        except Exception as e:
            print(f"  {ticker} {fiscal_year} {period}: ERROR — {e}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.financial_reports_log"))

# COMMAND ----------

#spark.sql(f"DROP TABLE {UC_CATALOG}.{UC_SCHEMA}.financial_reports_log")
