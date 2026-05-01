# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# News Signals — Holdings × News Exposure
#
# One row per (account × ticker) that has news coverage in the date window.
# News is pre-aggregated per ticker in a subquery then joined to holdings,
# so signal metrics repeat cleanly across client/account rows without duplication.
# Lakeview aggregates on top for any grain (advisor, client, sector, etc.).
#
# Use :date.min / :date.max to scope the news window.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ahtsa;
# MAGIC USE SCHEMA awm;

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] News Signals by Ticker
# MAGIC %sql
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   h.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   c.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   ROUND(SUM(h.market_value), 2)  AS total_market_value,
# MAGIC
# MAGIC   -- ── News signals (pre-aggregated per ticker) ──────────────────────────────
# MAGIC   n.companyName,
# MAGIC   n.sector,
# MAGIC   n.article_count,
# MAGIC   n.positive,
# MAGIC   n.neutral,
# MAGIC   n.negative,
# MAGIC   n.pct_negative,
# MAGIC   n.net_sentiment_score,
# MAGIC   n.high_materiality_count,
# MAGIC   n.advisor_action_count,
# MAGIC   n.signal_types,
# MAGIC   n.latest_article_date
# MAGIC
# MAGIC FROM holdings h
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC JOIN (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     MAX(companyName)                                                    AS companyName,
# MAGIC     MAX(sector)                                                         AS sector,
# MAGIC     COUNT(*)                                                            AS article_count,
# MAGIC     SUM(CASE WHEN sentiment = 'positive'            THEN 1 ELSE 0 END) AS positive,
# MAGIC     SUM(CASE WHEN sentiment IN ('neutral', 'mixed') THEN 1 ELSE 0 END) AS neutral,
# MAGIC     SUM(CASE WHEN sentiment = 'negative'            THEN 1 ELSE 0 END) AS negative,
# MAGIC     ROUND(
# MAGIC       SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)
# MAGIC       / NULLIF(COUNT(*), 0) * 100, 1)                                  AS pct_negative,
# MAGIC     ROUND(
# MAGIC       (SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)
# MAGIC        - SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END))
# MAGIC       / NULLIF(COUNT(*), 0), 3)                                        AS net_sentiment_score,
# MAGIC     SUM(CASE WHEN materiality = 'High'  THEN 1 ELSE 0 END)            AS high_materiality_count,
# MAGIC     SUM(CASE WHEN advisor_action = true THEN 1 ELSE 0 END)            AS advisor_action_count,
# MAGIC     ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(signal_type)), ', ')             AS signal_types,
# MAGIC     MAX(TO_DATE(publishedDate))                                         AS latest_article_date
# MAGIC   FROM silver_news_signals
# MAGIC   WHERE is_relevant = true
# MAGIC     AND signal_type IS NOT NULL
# MAGIC     AND TO_DATE(publishedDate) BETWEEN :date.min AND :date.max
# MAGIC   GROUP BY symbol
# MAGIC ) n ON h.ticker = n.symbol
# MAGIC
# MAGIC GROUP BY
# MAGIC   h.ticker, h.account_id, a.account_name, a.account_type,
# MAGIC   c.client_id, c.client_name, c.advisor_id, c.tier, c.risk_profile,
# MAGIC   n.companyName, n.sector, n.article_count, n.positive, n.neutral,
# MAGIC   n.negative, n.pct_negative, n.net_sentiment_score,
# MAGIC   n.high_materiality_count, n.advisor_action_count,
# MAGIC   n.signal_types, n.latest_article_date
# MAGIC ORDER BY n.negative DESC, total_market_value DESC
