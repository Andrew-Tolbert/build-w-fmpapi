# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %sql
# MAGIC use catalog ahtsa; 
# MAGIC use schema awm; 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (account_id, asset_class). Query this view directly in Lakeview
# MAGIC -- and Genie — filter and aggregate on the front end.
# MAGIC -- holdings.asset_class is already the true economic class — ETFs reclassified at
# MAGIC -- write time via bronze_etf_info in 07_validate_and_rebuild_holdings.py.
# MAGIC -- Asset classes with zero holdings still appear via cross-join so every IPS cell is visible.
# MAGIC CREATE OR REPLACE table gold_account_ips_drift AS
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
# MAGIC )
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
