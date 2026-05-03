# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Unified Signal Queries for Lakeview
#
# Query 1 — Signal Feed: one row per signal, with company metadata and numeric sentiment score.
#   Use this for time-series charts, signal type breakdowns, sentiment trends.
#
# Query 2 — Exposure: one row per (signal × account). Fans out intentionally.
#   Use this to answer "which advisors/clients are exposed to negative signals right now."
#
# Use :date.min / :date.max to scope both queries to a signal window.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ahtsa;
# MAGIC USE SCHEMA awm;

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Signal Feed
# MAGIC %sql
# MAGIC SELECT
# MAGIC   g.signal_id,
# MAGIC   g.signal_date                                                         AS date,
# MAGIC   g.symbol,
# MAGIC   cp.companyName,
# MAGIC   cp.sector,
# MAGIC   g.source_type,
# MAGIC   CASE
# MAGIC     WHEN g.source_type LIKE 'sec_filing_%'
# MAGIC       THEN CONCAT('SEC Filing (', SUBSTRING(g.source_type, 12), ')')
# MAGIC     WHEN g.source_type = 'news'                THEN 'News'
# MAGIC     WHEN g.source_type = 'bdc_early_warning'   THEN 'BDC Early Warning'
# MAGIC     WHEN g.source_type = 'earnings_transcript' THEN 'Earnings Transcript'
# MAGIC     ELSE INITCAP(REPLACE(g.source_type, '_', ' '))
# MAGIC   END                                                                   AS source_type_display,
# MAGIC   g.signal_type,
# MAGIC   g.sentiment,
# MAGIC   CASE WHEN g.sentiment = 'Positive' THEN  1.0  ELSE 0 END             AS positive,
# MAGIC   CASE WHEN g.sentiment = 'Mixed'    THEN -0.5  ELSE 0 END             AS mixed,
# MAGIC   CASE WHEN g.sentiment = 'Neutral'  THEN  0.0  ELSE 0 END             AS neutral,
# MAGIC   CASE WHEN g.sentiment = 'Negative' THEN -1.0  ELSE 0 END             AS negative,
# MAGIC   g.severity_score,
# MAGIC   g.signal_value,
# MAGIC   g.advisor_action_needed,
# MAGIC   CASE WHEN g.advisor_action_needed THEN '⚠️ Action Needed' ELSE '🟩 No Action' END AS advisor_action_display,
# MAGIC   g.signal,
# MAGIC   g.rationale
# MAGIC FROM gold_unified_signals g
# MAGIC LEFT JOIN bronze_company_profiles cp ON g.symbol = cp.symbol
# MAGIC ORDER BY g.signal_date DESC

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Signal Exposure by Account
# MAGIC %sql
# MAGIC SELECT
# MAGIC   g.signal_id,
# MAGIC   g.signal_date                                                         AS date,
# MAGIC   g.symbol,
# MAGIC   cp.companyName,
# MAGIC   cp.sector,
# MAGIC   g.source_type,
# MAGIC   CASE
# MAGIC     WHEN g.source_type LIKE 'sec_filing_%'
# MAGIC       THEN CONCAT('SEC Filing (', SUBSTRING(g.source_type, 12), ')')
# MAGIC     WHEN g.source_type = 'news'                THEN 'News'
# MAGIC     WHEN g.source_type = 'bdc_early_warning'   THEN 'BDC Early Warning'
# MAGIC     WHEN g.source_type = 'earnings_transcript' THEN 'Earnings Transcript'
# MAGIC     ELSE INITCAP(REPLACE(g.source_type, '_', ' '))
# MAGIC   END                                                                   AS source_type_display,
# MAGIC   g.signal_type,
# MAGIC   g.sentiment,
# MAGIC   CASE WHEN g.sentiment = 'Positive' THEN  1.0  ELSE 0 END             AS positive,
# MAGIC   CASE WHEN g.sentiment = 'Mixed'    THEN -0.5  ELSE 0 END             AS mixed,
# MAGIC   CASE WHEN g.sentiment = 'Neutral'  THEN  0.0  ELSE 0 END             AS neutral,
# MAGIC   CASE WHEN g.sentiment = 'Negative' THEN -1.0  ELSE 0 END             AS negative,
# MAGIC   g.severity_score,
# MAGIC   g.signal_value,
# MAGIC   g.advisor_action_needed,
# MAGIC   h.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   c.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile
# MAGIC FROM gold_unified_signals g
# MAGIC JOIN holdings h ON g.symbol = h.ticker
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC LEFT JOIN bronze_company_profiles cp ON g.symbol = cp.symbol
# MAGIC WHERE g.signal_date BETWEEN :date.min AND :date.max
# MAGIC ORDER BY g.signal_date DESC
