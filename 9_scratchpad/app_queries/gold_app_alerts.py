# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# gold_app_alerts — Curated alert feed for the AWM advisor app
#
# gold_unified_signals contains ~6,900 rows across 8 source types.
# This notebook maintains an explicit allowlist of signal_ids that should
# surface in the app's Active Alerts panel.
#
# For each included signal, one row is produced per advisor who currently holds
# the underlying ticker — so the app can filter with a simple WHERE advisor_id = :id.
#
# To add a new alert: append its signal_id to INCLUDED_SIGNAL_IDS below and re-run.
# To remove an alert: delete its signal_id and re-run.
#
# Schema:
#   signal_id           STRING  — original gold_unified_signals ID
#   symbol              STRING  — ticker
#   company_name        STRING  — display name from bronze_company_profiles
#   advisor_id          STRING  — advisor who holds this ticker (one row per advisor)
#   signal_date         DATE
#   source_type         STRING  — bdc_early_warning | news | earnings_transcript | sec_filing_*
#   source_description  STRING  — e.g. "10-K Q1 2026"
#   sentiment           STRING  — Negative | Positive | Mixed | Neutral | Deteriorating
#   severity_score      DOUBLE  — 0.0 – 1.0
#   advisor_action_needed BOOLEAN
#   signal_type         STRING  — category label
#   signal              STRING  — specific metric or signal name
#   signal_value        STRING  — formatted value (e.g. "33.1% (CONCERN)")
#   rationale           STRING  — AI rationale, trimmed to 400 chars for the app
#   processed_at        TIMESTAMP

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# ── Allowlist — add signal_ids here to surface them in the app ─────────────────
INCLUDED_SIGNAL_IDS = [
    "038b38d2a2da53c1e1c4a5a0342885b9",  # FSK — PIK/NII Ratio 33.1% (CONCERN) — bdc_early_warning
    '645efced353ecf84bb7c2fe65fa65fc6',  #TCPC NAV Share Collapse 
    '8e8c35c0c590d3947f75eb9ad950e3d0',  # Revenue Collapse
    '0431030d7534791fc1594d6df7c93f8a',  #surprise Adobe Disclosure 
    'd99ffec7648e947a0a4fb749f220433f', #surprise positive disclosure 
]

# COMMAND ----------

if not INCLUDED_SIGNAL_IDS:
    raise Exception("INCLUDED_SIGNAL_IDS is empty — add at least one signal_id.")

ids_sql = ", ".join(f"'{sid}'" for sid in INCLUDED_SIGNAL_IDS)
print(f"Building gold_app_alerts from {len(INCLUDED_SIGNAL_IDS)} signal(s):")
for sid in INCLUDED_SIGNAL_IDS:
    print(f"  {sid}")

# COMMAND ----------

# DBTITLE 1,Preview — signals to be included
display(spark.sql(f"""
    SELECT signal_id, symbol, signal_date, source_type, sentiment,
           severity_score, signal_type, signal, signal_value,
           LEFT(rationale, 120) AS rationale_preview
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
    WHERE signal_id IN ({ids_sql})
    ORDER BY severity_score DESC, signal_date DESC
"""))

# COMMAND ----------

# DBTITLE 1,Create gold_app_alerts
spark.sql(f"""
CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.gold_app_alerts AS

WITH

-- Most recent holdings snapshot
latest_date AS (
    SELECT MAX(date) AS max_date FROM {UC_CATALOG}.{UC_SCHEMA}.holdings
),

-- One row per (ticker, advisor_id) from the most recent snapshot, with total exposure
holding_advisors AS (
    SELECT
        h.ticker,
        c.advisor_id,
        SUM(h.market_value) AS total_exposure
    FROM {UC_CATALOG}.{UC_SCHEMA}.holdings h
    JOIN {UC_CATALOG}.{UC_SCHEMA}.accounts a  ON a.account_id = h.account_id
    JOIN {UC_CATALOG}.{UC_SCHEMA}.clients  c  ON c.client_id  = a.client_id
    JOIN latest_date ld ON h.date = ld.max_date
    WHERE h.ticker != 'CASH'
    GROUP BY h.ticker, c.advisor_id
),

-- Curated signals joined with company names
curated AS (
    SELECT
        s.signal_id,
        s.symbol,
        COALESCE(cp.companyName, s.symbol)       AS company_name,
        s.signal_date,
        s.source_type,
        s.source_description,
        s.sentiment,
        s.severity_score,
        s.advisor_action_needed,
        s.signal_type,
        s.signal,
        s.signal_value,
        -- Trim rationale — full text lives in gold_unified_signals
        s.rationale                  AS rationale,
        s.processed_at
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals s
    LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
        ON cp.symbol = s.symbol
    WHERE s.signal_id IN ({ids_sql})
)

-- Fan out: one row per (signal, advisor) for app-side filtering
SELECT
    c.signal_id,
    c.symbol,
    c.company_name,
    ha.advisor_id,
    ha.total_exposure,
    c.signal_date,
    c.source_type,
    c.source_description,
    c.sentiment,
    c.severity_score,
    c.advisor_action_needed,
    c.signal_type,
    c.signal,
    c.signal_value,
    c.rationale,
    c.processed_at
FROM curated c
JOIN holding_advisors ha ON ha.ticker = c.symbol
ORDER BY c.severity_score DESC, c.signal_date DESC, ha.advisor_id
""")

total = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_alerts").collect()[0][0]
print(f"gold_app_alerts created — {total} rows ({len(INCLUDED_SIGNAL_IDS)} signal(s) × advisors holding the ticker)")

# COMMAND ----------

# DBTITLE 1,Verify — rows per signal and advisor coverage
display(spark.sql(f"""
    SELECT signal_id, symbol, company_name, signal_date, source_type,
           sentiment, severity_score, signal, signal_value,
           COUNT(advisor_id) AS advisor_count,
           COLLECT_LIST(advisor_id) AS advisors
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_alerts
    GROUP BY signal_id, symbol, company_name, signal_date, source_type,
             sentiment, severity_score, signal, signal_value
    ORDER BY severity_score DESC
"""))

# COMMAND ----------

# DBTITLE 1,Final table
display(spark.sql(f"""
    SELECT *
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_alerts
    ORDER BY severity_score DESC, signal_date DESC, advisor_id
"""))
