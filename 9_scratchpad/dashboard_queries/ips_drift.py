# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# IPS Drift — Dashboard & Genie Queries
#
# Investment Policy Statement (IPS) drift occurs when a portfolio's actual asset class
# allocations deviate from the target ranges defined in the client's IPS.
#
# SECTION A — Single Lakeview Query
#   One row per (account_id × asset_class). All drift metrics inline — no gold tables.
#   Lakeview aggregates on top; filter by advisor, client, account_type, asset_class,
#   drift_status on the front end.
#
# SECTION B — Genie Context Queries
#   Paste into Genie space as context for natural language questions.
#
# Drift metrics:
#   drift_from_target_pct  — actual % − target % (+ = overweight, − = underweight)
#   out_of_bounds_pct      — distance outside the min/max band (0 when in band)
#   drift_status           — 'Over Band' | 'Under Band' | 'Within Band'
#   drift_severity         — 'Critical' | 'Warning' | 'OK'
#   rebalance_to_target    — $ to hit exact target (negative = sell)
#   rebalance_to_band      — $ to get just back inside band (0 when in band)
#
# Filtering is handled on the Lakeview front end — no :param syntax needed since
# the result set is static (unlike holdings which rebuilds positions over a date range).

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,─── SECTION A: LAKEVIEW QUERY ──────────────────────────────────────────────────

# COMMAND ----------

# DBTITLE 1,gold_ips_drift — View (SELECT * for Lakeview / Genie)
# MAGIC %sql
# MAGIC -- One row per (account_id, asset_class). Query this view directly in Lakeview
# MAGIC -- and Genie — filter and aggregate on the front end.
# MAGIC -- holdings.asset_class is already the true economic class — ETFs reclassified at
# MAGIC -- write time via bronze_etf_info in 07_validate_and_rebuild_holdings.py.
# MAGIC -- Asset classes with zero holdings still appear via cross-join so every IPS cell is visible.
# MAGIC CREATE OR REPLACE VIEW gold_ips_drift AS
# MAGIC WITH
# MAGIC
# MAGIC -- ── Total value per account ───────────────────────────────────────────────────
# MAGIC account_totals AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     SUM(market_value) AS total_account_value
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC
# MAGIC -- ── Actual holdings by (account, asset_class) ────────────────────────────────
# MAGIC actual_by_class AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     asset_class,
# MAGIC     SUM(market_value) AS actual_market_value,
# MAGIC     COUNT(*)          AS positions_count
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id, asset_class
# MAGIC ),
# MAGIC
# MAGIC -- ── Every (account × IPS asset class) cell ───────────────────────────────────
# MAGIC -- Ensures zero-held asset classes still show up as rows.
# MAGIC account_class_grid AS (
# MAGIC   SELECT
# MAGIC     a.account_id,
# MAGIC     it.asset_class
# MAGIC   FROM (SELECT DISTINCT account_id FROM holdings) a
# MAGIC   CROSS JOIN (SELECT DISTINCT asset_class FROM ips_targets) it
# MAGIC ),
# MAGIC
# MAGIC SELECT
# MAGIC   -- ── Identity ──────────────────────────────────────────────────────────────
# MAGIC   c.advisor_id,
# MAGIC   c.client_id,
# MAGIC   c.client_name,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   ac.account_id,
# MAGIC   ac.account_name,
# MAGIC   ac.account_type,
# MAGIC   g.asset_class,
# MAGIC
# MAGIC   -- ── Actuals ───────────────────────────────────────────────────────────────
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0), 2)              AS actual_market_value,
# MAGIC   ROUND(at.total_account_value, 2)                           AS total_account_value,
# MAGIC   COALESCE(ab.positions_count, 0)                            AS positions_count,
# MAGIC   ROUND(
# MAGIC     COALESCE(ab.actual_market_value, 0)
# MAGIC     / NULLIF(at.total_account_value, 0) * 100, 4)            AS actual_allocation_pct,
# MAGIC
# MAGIC   -- ── IPS targets ───────────────────────────────────────────────────────────
# MAGIC   it.target_allocation_pct,
# MAGIC   it.min_allocation_pct,
# MAGIC   it.max_allocation_pct,
# MAGIC   it.rebalance_trigger_pct,
# MAGIC   ROUND(it.target_allocation_pct / 100 * at.total_account_value, 2) AS target_market_value,
# MAGIC   ROUND(it.min_allocation_pct    / 100 * at.total_account_value, 2) AS min_market_value,
# MAGIC   ROUND(it.max_allocation_pct    / 100 * at.total_account_value, 2) AS max_market_value,
# MAGIC
# MAGIC   -- ── Drift ─────────────────────────────────────────────────────────────────
# MAGIC   ROUND(
# MAGIC     COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC     - it.target_allocation_pct, 4)                           AS drift_from_target_pct,
# MAGIC
# MAGIC   ROUND(GREATEST(0,
# MAGIC     COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC     - it.max_allocation_pct,
# MAGIC     it.min_allocation_pct
# MAGIC     - COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC   ), 4)                                                      AS out_of_bounds_pct,
# MAGIC
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            > it.max_allocation_pct
# MAGIC       THEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            - it.max_allocation_pct
# MAGIC       WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            < it.min_allocation_pct
# MAGIC       THEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            - it.min_allocation_pct
# MAGIC       ELSE -LEAST(
# MAGIC              it.max_allocation_pct
# MAGIC              - COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100,
# MAGIC              COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC              - it.min_allocation_pct)
# MAGIC     END, 4)                                                  AS band_distance_pct,
# MAGIC
# MAGIC   -- ── Breach flags ──────────────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct THEN 'Over Band'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct THEN 'Under Band'
# MAGIC     ELSE 'Within Band'
# MAGIC   END                                                        AS drift_status,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct + it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct - it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct                             THEN 'Warning'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct                             THEN 'Warning'
# MAGIC     ELSE 'OK'
# MAGIC   END                                                        AS drift_severity,
# MAGIC
# MAGIC   -- ── Rebalance amounts ─────────────────────────────────────────────────────
# MAGIC   ROUND(
# MAGIC     it.target_allocation_pct / 100 * at.total_account_value
# MAGIC     - COALESCE(ab.actual_market_value, 0), 2)                AS rebalance_to_target,
# MAGIC
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            > it.max_allocation_pct
# MAGIC       THEN it.max_allocation_pct / 100 * at.total_account_value
# MAGIC            - COALESCE(ab.actual_market_value, 0)
# MAGIC       WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC            < it.min_allocation_pct
# MAGIC       THEN it.min_allocation_pct / 100 * at.total_account_value
# MAGIC            - COALESCE(ab.actual_market_value, 0)
# MAGIC       ELSE 0
# MAGIC     END, 2)                                                  AS rebalance_to_band
# MAGIC
# MAGIC FROM account_class_grid g
# MAGIC JOIN accounts    ac ON g.account_id  = ac.account_id
# MAGIC JOIN clients     c  ON ac.client_id  = c.client_id
# MAGIC JOIN account_totals at ON g.account_id = at.account_id
# MAGIC JOIN ips_targets it ON c.risk_profile = it.risk_profile
# MAGIC                     AND g.asset_class  = it.asset_class
# MAGIC LEFT JOIN actual_by_class ab
# MAGIC   ON g.account_id = ab.account_id AND g.asset_class = ab.asset_class

# COMMAND ----------

# DBTITLE 1,─── SECTION B: GENIE CONTEXT QUERIES ─────────────────────────────────────────
# Static SQL designed for Genie space context.
# Paste each block into Genie alongside its suggested question.

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which clients have the worst IPS drift?"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   client_id,
# MAGIC   client_name,
# MAGIC   advisor_id,
# MAGIC   tier,
# MAGIC   risk_profile,
# MAGIC   ROUND(SUM(actual_market_value) / 1e6, 3)                   AS client_aum_m,
# MAGIC   ROUND(AVG(ABS(drift_from_target_pct)), 4)                  AS drift_score,
# MAGIC   SUM(CASE WHEN drift_status != 'Within Band' THEN 1 ELSE 0 END) AS breach_count,
# MAGIC   ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 3)                AS total_rebalance_abs_m
# MAGIC FROM gold_ips_drift
# MAGIC GROUP BY client_id, client_name, advisor_id, tier, risk_profile
# MAGIC ORDER BY drift_score DESC
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which accounts are overweight in Private Credit?"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   advisor_id,
# MAGIC   client_id,
# MAGIC   client_name,
# MAGIC   account_id,
# MAGIC   account_name,
# MAGIC   account_type,
# MAGIC   tier,
# MAGIC   risk_profile,
# MAGIC   ROUND(actual_allocation_pct, 2)   AS actual_pct,
# MAGIC   target_allocation_pct             AS target_pct,
# MAGIC   max_allocation_pct                AS max_pct,
# MAGIC   ROUND(out_of_bounds_pct, 4)       AS over_band_by_pct,
# MAGIC   ROUND(actual_market_value / 1e6, 3) AS actual_mv_m,
# MAGIC   ROUND(total_account_value / 1e6, 3) AS account_aum_m,
# MAGIC   ROUND(rebalance_to_band / 1e6, 3)   AS rebalance_to_band_m
# MAGIC FROM gold_ips_drift
# MAGIC WHERE asset_class = 'Private Credit'
# MAGIC   AND drift_status = 'Over Band'
# MAGIC ORDER BY over_band_by_pct DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show me all accounts outside their IPS bounds"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   client_name,
# MAGIC   advisor_id,
# MAGIC   account_id,
# MAGIC   account_name,
# MAGIC   account_type,
# MAGIC   asset_class,
# MAGIC   drift_status,
# MAGIC   drift_severity,
# MAGIC   ROUND(actual_allocation_pct, 2)     AS actual_pct,
# MAGIC   target_allocation_pct               AS target_pct,
# MAGIC   min_allocation_pct                  AS min_pct,
# MAGIC   max_allocation_pct                  AS max_pct,
# MAGIC   out_of_bounds_pct,
# MAGIC   ROUND(actual_market_value / 1e6, 3) AS actual_mv_m,
# MAGIC   ROUND(total_account_value / 1e6, 3) AS account_aum_m,
# MAGIC   ROUND(rebalance_to_band   / 1e6, 3) AS rebalance_to_band_m
# MAGIC FROM gold_ips_drift
# MAGIC WHERE drift_status != 'Within Band'
# MAGIC ORDER BY out_of_bounds_pct DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "What is the total rebalance amount needed across all accounts?"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   asset_class,
# MAGIC   drift_status,
# MAGIC   COUNT(DISTINCT account_id)                        AS accounts_impacted,
# MAGIC   COUNT(DISTINCT client_id)                         AS clients_impacted,
# MAGIC   ROUND(SUM(actual_market_value)  / 1e9, 3)         AS total_actual_b,
# MAGIC   ROUND(SUM(target_market_value)  / 1e9, 3)         AS total_target_b,
# MAGIC   ROUND(SUM(rebalance_to_band)    / 1e6, 2)         AS rebalance_to_band_m,
# MAGIC   ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 2)       AS rebalance_abs_m
# MAGIC FROM gold_ips_drift
# MAGIC WHERE drift_status != 'Within Band'
# MAGIC GROUP BY asset_class, drift_status
# MAGIC ORDER BY rebalance_abs_m DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which advisors have the most clients with IPS drift?"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   advisor_id,
# MAGIC   COUNT(DISTINCT client_id)                                        AS total_clients,
# MAGIC   COUNT(DISTINCT account_id)                                       AS total_accounts,
# MAGIC   ROUND(SUM(total_account_value) / COUNT(DISTINCT asset_class) / 1e9, 3) AS book_aum_b,
# MAGIC   COUNT(DISTINCT CASE WHEN drift_status != 'Within Band' THEN client_id END) AS clients_with_drift,
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN drift_status != 'Within Band' THEN client_id END)
# MAGIC     / NULLIF(COUNT(DISTINCT client_id), 0) * 100, 1)               AS pct_clients_drifted,
# MAGIC   ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 2)                      AS total_rebalance_abs_m
# MAGIC FROM gold_ips_drift
# MAGIC GROUP BY advisor_id
# MAGIC ORDER BY clients_with_drift DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "What is the average allocation vs target by asset class?"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   asset_class,
# MAGIC   ROUND(AVG(actual_allocation_pct),      4) AS avg_actual_pct,
# MAGIC   ROUND(AVG(target_allocation_pct),      4) AS avg_target_pct,
# MAGIC   ROUND(AVG(drift_from_target_pct),      4) AS avg_drift_pct,
# MAGIC   COUNT(DISTINCT account_id)                AS total_accounts,
# MAGIC   SUM(CASE WHEN drift_status = 'Over Band'  THEN 1 ELSE 0 END) AS over_band_count,
# MAGIC   SUM(CASE WHEN drift_status = 'Under Band' THEN 1 ELSE 0 END) AS under_band_count,
# MAGIC   ROUND(SUM(actual_market_value) / 1e9, 3)  AS total_actual_b,
# MAGIC   ROUND(SUM(target_market_value) / 1e9, 3)  AS total_target_b
# MAGIC FROM gold_ips_drift
# MAGIC GROUP BY asset_class
# MAGIC ORDER BY ABS(avg_drift_pct) DESC
