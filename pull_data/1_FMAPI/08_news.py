# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull recent stock news for all watchlist tickers.
# Each article is saved as an individual JSON file containing the FMP metadata,
# summary, and full article text fetched from the source URL.
# A Delta table (stock_news_log) tracks downloaded articles so re-runs are
# idempotent — only new articles have their full text fetched.
# Output: UC_VOLUME_PATH/stock_news/{TICKER}/{published_date}_{url_hash}.json
# FMP Source: F15 — /stable/stock-news

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

# MAGIC %run ../../utils/news_downloader

# COMMAND ----------

import os
import json
import hashlib
import datetime
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
# clear_directory(volume_subdir("stock_news"))

# COMMAND ----------

# Create download log table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.stock_news_log (
        symbol         STRING,
        url            STRING,
        published_date STRING,
        filename       STRING,
        downloaded_at  TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# Pull all news from the start of the current month through today
today     = datetime.date.today()
from_date = today.replace(day=1).strftime("%Y-%m-%d")
to_date   = today.strftime("%Y-%m-%d")

out_base = volume_subdir("stock_news")

for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_stock_news(ticker, from_date=from_date, to_date=to_date)
        if not rows:
            print(f"  {ticker}: no articles")
            continue
        print(f"  {ticker}: {len(rows)} articles from FMP")
    except Exception as e:
        print(f"  {ticker}: ERROR fetching news — {e}")
        continue

    ticker_dir = f"{out_base}/{ticker}"
    os.makedirs(ticker_dir, exist_ok=True)

    for row in rows:
        url            = row.get("url", "")
        published_date = str(row.get("publishedDate", ""))[:10]

        if not url:
            continue

        # Skip if already in the log
        url_escaped = url.replace("'", "\\'")
        already_downloaded = spark.sql(f"""
            SELECT 1 FROM {UC_CATALOG}.{UC_SCHEMA}.stock_news_log
            WHERE symbol = '{ticker}' AND url = '{url_escaped}'
        """).count() > 0
        if already_downloaded:
            print(f"  {ticker} {published_date}: already downloaded, skipping")
            continue

        # Stable filename: date + first 8 chars of URL hash
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{published_date}_{url_hash}.json"
        dest     = f"{ticker_dir}/{filename}"

        full_text = fetch_article_text(url)

        article = dict(row)
        article["summary"]  = article.pop("text", None)
        article["full_text"] = full_text
        article["symbol"]    = ticker
        article["ingested_at"] = pd.Timestamp.now().isoformat()

        with open(dest, "w", encoding="utf-8") as f:
            json.dump(article, f, indent=2)
        print(f"  {ticker} {published_date}: saved {filename}")

        spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.stock_news_log AS log
            USING (
                SELECT
                    '{ticker}'        AS symbol,
                    '{url_escaped}'   AS url,
                    '{published_date}' AS published_date,
                    '{filename}'      AS filename,
                    current_timestamp() AS downloaded_at
            ) AS src
            ON  log.symbol = src.symbol
            AND log.url    = src.url
            WHEN MATCHED THEN
                UPDATE SET
                    log.filename      = src.filename,
                    log.downloaded_at = src.downloaded_at
            WHEN NOT MATCHED THEN
                INSERT *
        """)

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.stock_news_log"))

# COMMAND ----------

# spark.sql(f"DROP TABLE {UC_CATALOG}.{UC_SCHEMA}.stock_news_log")
