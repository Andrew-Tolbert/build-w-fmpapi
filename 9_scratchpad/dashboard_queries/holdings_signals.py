# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Holdings × Unified Signals
#
# One row per (account × ticker × source_type) that has signal coverage in the date window.
# Signals are pre-aggregated per (ticker × source_type) in a subquery then joined to holdings,
# so signal metrics repeat cleanly across client/account rows without duplication.
# Filter by source_type in Lakeview to scope to news, sec_filings, earnings, bdc, etc.
#
# Use :date.min / :date.max to scope the signal window.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ahtsa;
# MAGIC USE SCHEMA awm;

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Holdings × Signals
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
# MAGIC   -- Company metadata
# MAGIC   cp.companyName,
# MAGIC   cp.sector,
# MAGIC
# MAGIC   -- Signal aggregates (per ticker × source_type — filter source_type in Lakeview)
# MAGIC   s.source_type,
# MAGIC   s.signal_count,
# MAGIC   s.positive,
# MAGIC   s.neutral,
# MAGIC   s.negative,
# MAGIC   s.pct_negative,
# MAGIC   s.net_sentiment_score,
# MAGIC   s.high_severity_count,
# MAGIC   s.advisor_action_count,
# MAGIC   s.signal_types,
# MAGIC   s.latest_signal_date
# MAGIC
# MAGIC FROM holdings h
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC LEFT JOIN bronze_company_profiles cp ON h.ticker = cp.symbol
# MAGIC JOIN (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     source_type,
# MAGIC     COUNT(*)                                                            AS signal_count,
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
# MAGIC     SUM(CASE WHEN signal_value = 'High'       THEN 1 ELSE 0 END)      AS high_severity_count,
# MAGIC     SUM(CASE WHEN advisor_action_needed = true THEN 1 ELSE 0 END)      AS advisor_action_count,
# MAGIC     ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(signal_type)), ', ')             AS signal_types,
# MAGIC     MAX(signal_date)                                                    AS latest_signal_date
# MAGIC   FROM gold_unified_signals
# MAGIC   WHERE signal_type IS NOT NULL
# MAGIC     AND signal_date BETWEEN :date.min AND :date.max
# MAGIC   GROUP BY symbol, source_type
# MAGIC ) s ON h.ticker = s.symbol
# MAGIC
# MAGIC GROUP BY
# MAGIC   h.ticker, h.account_id, a.account_name, a.account_type,
# MAGIC   c.client_id, c.client_name, c.advisor_id, c.tier, c.risk_profile,
# MAGIC   cp.companyName, cp.sector,
# MAGIC   s.source_type, s.signal_count, s.positive, s.neutral,
# MAGIC   s.negative, s.pct_negative, s.net_sentiment_score,
# MAGIC   s.high_severity_count, s.advisor_action_count,
# MAGIC   s.signal_types, s.latest_signal_date
# MAGIC ORDER BY s.negative DESC, total_market_value DESC
