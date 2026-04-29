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
# allocations deviate from the target ranges defined in the client's IPS. This notebook:
#
#   SECTION A — Gold Table Creation
#     gold_account_ips_drift  — actual vs target per (account_id, asset_class)
#                               includes drift magnitude, breach flags, and rebalance $
#     gold_client_ips_drift   — client-level rollup: drift score, breach count, priority
#
#   SECTION B — Lakeview Dashboard Queries  (:param syntax)
#     1. IPS Drift Overview      — all accounts ranked by out-of-bounds severity
#     2. Allocation vs Band      — actual/target/min/max per account, asset class
#     3. Breach Heatmap          — all clients × asset classes, colored by drift status
#     4. Advisor Book Drift      — advisor-level aggregation: clients with drift
#     5. Rebalance Opportunity   — $ to trade to restore IPS compliance per account
#
#   SECTION C — Genie Context Queries  (paste into Genie as context)
#     1. Which clients have the worst IPS drift?
#     2. Which accounts are overweight in Private Credit?
#     3. Show all accounts outside their IPS bounds
#     4. What is the total rebalance amount needed?
#     5. Which advisors have the most clients with IPS drift?
#     6. What is the average actual allocation vs target by asset class?
#
# Drift Metrics:
#   drift_from_target_pct  — actual % - target % (signed; + = overweight, - = underweight)
#   out_of_bounds_pct      — distance outside the min/max band (0 when in band)
#   drift_status           — 'Over Band' | 'Under Band' | 'Within Band'
#   rebalance_to_target    — $ to move back to target (negative = sell, positive = buy)
#   rebalance_to_band      — $ to move just back inside the band (0 when in band)
#   drift_score            — account-level: AVG(|drift_from_target_pct|) across asset classes
#
# Lakeview parameters (all optional; NULL = no filter / include all):
#   :advisor_id   — advisor ID      (multi-select)
#   :account_type — account type    (multi-select)
#   :client_id    — client ID       (multi-select)
#   :asset_class  — asset class     (multi-select)
#   :drift_status — breach filter   (e.g. 'Over Band', 'Under Band', 'Within Band')

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# Set catalog and schema dynamically from ingest_config widgets
spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------
# DBTITLE 1,─── SECTION A: GOLD TABLE CREATION ─────────────────────────────────────────────

# COMMAND ----------
# DBTITLE 1,gold_account_ips_drift — Actual vs Target per Account × Asset Class
# MAGIC %sql
# MAGIC -- One row per (account_id, asset_class) — 6 rows per account.
# MAGIC -- Asset classes with zero holdings still appear so every IPS cell is visible.
# MAGIC -- out_of_bounds_pct: distance outside the min/max band (0 when in band).
# MAGIC -- rebalance_to_band: $ trade to get back inside band (0 when in band).
# MAGIC -- rebalance_to_target: $ trade to hit the exact target allocation.
# MAGIC CREATE OR REPLACE TABLE gold_account_ips_drift AS
# MAGIC WITH
# MAGIC -- ── Total account value (including cash) ─────────────────────────────────
# MAGIC account_totals AS (
# MAGIC   SELECT
# MAGIC     h.account_id,
# MAGIC     SUM(h.market_value) AS total_account_value
# MAGIC   FROM holdings h
# MAGIC   GROUP BY h.account_id
# MAGIC ),
# MAGIC
# MAGIC -- ── Actual allocation by (account_id, asset_class) ────────────────────────
# MAGIC actual_by_class AS (
# MAGIC   SELECT
# MAGIC     h.account_id,
# MAGIC     h.asset_class,
# MAGIC     SUM(h.market_value) AS actual_market_value,
# MAGIC     COUNT(*)            AS positions_count
# MAGIC   FROM holdings h
# MAGIC   GROUP BY h.account_id, h.asset_class
# MAGIC ),
# MAGIC
# MAGIC -- ── Cross-join accounts × all IPS asset classes ────────────────────────────
# MAGIC -- Ensures every asset class row exists for every account, even if $0 held.
# MAGIC account_class_grid AS (
# MAGIC   SELECT
# MAGIC     a.account_id,
# MAGIC     it.asset_class
# MAGIC   FROM (SELECT DISTINCT account_id FROM holdings) a
# MAGIC   CROSS JOIN (SELECT DISTINCT asset_class FROM ips_targets) it
# MAGIC ),
# MAGIC
# MAGIC -- ── ETF sub-class enrichment from bronze_etf_info ────────────────────────
# MAGIC -- For the ETF allocation bucket: what types of ETFs does each account hold?
# MAGIC -- assetClass from FMP is e.g. "Equity", "Fixed Income", "Commodity", "Real Estate".
# MAGIC etf_class_by_account AS (
# MAGIC   SELECT
# MAGIC     h.account_id,
# MAGIC     ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(ei.assetClass)), ' / ')  AS etf_asset_classes,
# MAGIC     COUNT(DISTINCT ei.assetClass)                              AS etf_class_count,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass = 'Equity'
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_equity_pct,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass IN ('Fixed Income', 'Bond')
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_fixed_income_pct,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass IN ('Commodity', 'Commodities',
# MAGIC                                            'Real Estate', 'Alternative',
# MAGIC                                            'Multi-Asset')
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_other_pct
# MAGIC   FROM holdings h
# MAGIC   JOIN bronze_etf_info ei ON h.ticker = ei.symbol
# MAGIC   WHERE h.asset_class = 'ETF'
# MAGIC   GROUP BY h.account_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   -- ── Identity ──────────────────────────────────────────────────────────────
# MAGIC   g.account_id,
# MAGIC   ac.account_name,
# MAGIC   ac.account_type,
# MAGIC   ac.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   g.asset_class,
# MAGIC
# MAGIC   -- ── Portfolio values ──────────────────────────────────────────────────────
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0), 2)        AS actual_market_value,
# MAGIC   ROUND(at.total_account_value, 2)                     AS total_account_value,
# MAGIC   COALESCE(ab.positions_count, 0)                      AS positions_count,
# MAGIC
# MAGIC   -- ── ETF sub-type enrichment (ETF rows only; NULL for other asset classes) ──
# MAGIC   -- Shows what types of ETFs are held within this account's ETF allocation bucket.
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ea.etf_asset_classes  ELSE NULL END AS etf_asset_classes,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ea.etf_class_count     ELSE NULL END AS etf_class_count,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ea.etf_equity_pct      ELSE NULL END AS etf_equity_pct,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ea.etf_fixed_income_pct ELSE NULL END AS etf_fixed_income_pct,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ea.etf_other_pct       ELSE NULL END AS etf_other_pct,
# MAGIC
# MAGIC   -- ── Actual allocation % ───────────────────────────────────────────────────
# MAGIC   ROUND(
# MAGIC     COALESCE(ab.actual_market_value, 0)
# MAGIC     / NULLIF(at.total_account_value, 0) * 100, 4)      AS actual_allocation_pct,
# MAGIC
# MAGIC   -- ── IPS targets ───────────────────────────────────────────────────────────
# MAGIC   it.target_allocation_pct,
# MAGIC   it.min_allocation_pct,
# MAGIC   it.max_allocation_pct,
# MAGIC   it.rebalance_trigger_pct,
# MAGIC
# MAGIC   -- ── Drift metrics ─────────────────────────────────────────────────────────
# MAGIC   -- Signed drift: positive = overweight, negative = underweight
# MAGIC   ROUND(
# MAGIC     COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC     - it.target_allocation_pct, 4)                     AS drift_from_target_pct,
# MAGIC
# MAGIC   -- Out-of-bounds distance: 0 when within band, positive when breached
# MAGIC   ROUND(
# MAGIC     GREATEST(0,
# MAGIC       COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC       - it.max_allocation_pct,
# MAGIC       it.min_allocation_pct
# MAGIC       - COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC     ), 4)                                              AS out_of_bounds_pct,
# MAGIC
# MAGIC   -- Distance to nearest band edge (negative = cushion inside band, positive = outside)
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
# MAGIC              - it.min_allocation_pct
# MAGIC            )
# MAGIC     END, 4)                                            AS band_distance_pct,
# MAGIC
# MAGIC   -- ── Breach flags ──────────────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct THEN 'Over Band'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct THEN 'Under Band'
# MAGIC     ELSE 'Within Band'
# MAGIC   END                                                  AS drift_status,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct + it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct - it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          > it.max_allocation_pct THEN 'Warning'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC          < it.min_allocation_pct THEN 'Warning'
# MAGIC     ELSE 'OK'
# MAGIC   END                                                  AS drift_severity,
# MAGIC
# MAGIC   -- ── Rebalance amounts ─────────────────────────────────────────────────────
# MAGIC   -- rebalance_to_target: $ to bring allocation exactly to target (neg = sell)
# MAGIC   ROUND(
# MAGIC     it.target_allocation_pct / 100 * at.total_account_value
# MAGIC     - COALESCE(ab.actual_market_value, 0), 2)          AS rebalance_to_target,
# MAGIC
# MAGIC   -- rebalance_to_band: $ to get just inside the band (0 when already in band)
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
# MAGIC     END, 2)                                            AS rebalance_to_band,
# MAGIC
# MAGIC   -- ── IPS target dollar value ───────────────────────────────────────────────
# MAGIC   ROUND(it.target_allocation_pct / 100 * at.total_account_value, 2)
# MAGIC                                                        AS target_market_value,
# MAGIC   ROUND(it.min_allocation_pct    / 100 * at.total_account_value, 2)
# MAGIC                                                        AS min_market_value,
# MAGIC   ROUND(it.max_allocation_pct    / 100 * at.total_account_value, 2)
# MAGIC                                                        AS max_market_value,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                                  AS updated_at
# MAGIC
# MAGIC FROM account_class_grid g
# MAGIC JOIN accounts ac         ON g.account_id = ac.account_id
# MAGIC JOIN clients  c          ON ac.client_id  = c.client_id
# MAGIC JOIN account_totals at   ON g.account_id  = at.account_id
# MAGIC JOIN ips_targets it      ON c.risk_profile = it.risk_profile
# MAGIC                        AND g.asset_class   = it.asset_class
# MAGIC LEFT JOIN actual_by_class ab
# MAGIC   ON g.account_id = ab.account_id AND g.asset_class = ab.asset_class
# MAGIC LEFT JOIN etf_class_by_account ea
# MAGIC   ON g.account_id = ea.account_id

# COMMAND ----------
# DBTITLE 1,gold_client_ips_drift — Client-Level Drift Rollup
# MAGIC %sql
# MAGIC -- One row per (client_id, asset_class) rolled up across all of the client's accounts.
# MAGIC -- Also provides a per-client summary row for book-level views.
# MAGIC -- drift_score: AVG(|drift_from_target_pct|) — lower is better; 0 = perfect alignment.
# MAGIC -- breach_count: number of (account × asset_class) cells outside min/max band.
# MAGIC CREATE OR REPLACE TABLE gold_client_ips_drift AS
# MAGIC WITH
# MAGIC -- ── Client-level actual allocation across all accounts ─────────────────────
# MAGIC client_class_actual AS (
# MAGIC   SELECT
# MAGIC     ac.client_id,
# MAGIC     h.asset_class,
# MAGIC     SUM(h.market_value)                                AS actual_market_value,
# MAGIC     SUM(SUM(h.market_value)) OVER (PARTITION BY ac.client_id)
# MAGIC                                                        AS total_client_value
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts ac ON h.account_id = ac.account_id
# MAGIC   GROUP BY ac.client_id, h.asset_class
# MAGIC ),
# MAGIC
# MAGIC -- ── Ensure all 6 asset classes per client ─────────────────────────────────
# MAGIC client_class_grid AS (
# MAGIC   SELECT
# MAGIC     c.client_id,
# MAGIC     it.asset_class
# MAGIC   FROM (SELECT DISTINCT client_id FROM accounts) c
# MAGIC   CROSS JOIN (SELECT DISTINCT asset_class FROM ips_targets) it
# MAGIC ),
# MAGIC
# MAGIC -- ── Per client total AUM ──────────────────────────────────────────────────
# MAGIC client_totals AS (
# MAGIC   SELECT ac.client_id, SUM(h.market_value) AS total_client_value
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts ac ON h.account_id = ac.account_id
# MAGIC   GROUP BY ac.client_id
# MAGIC ),
# MAGIC
# MAGIC -- ── ETF sub-class enrichment (client level) ──────────────────────────────
# MAGIC etf_class_by_client AS (
# MAGIC   SELECT
# MAGIC     ac.client_id,
# MAGIC     ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(ei.assetClass)), ' / ')  AS etf_asset_classes,
# MAGIC     COUNT(DISTINCT ei.assetClass)                              AS etf_class_count,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass = 'Equity'
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_equity_pct,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass IN ('Fixed Income', 'Bond')
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_fixed_income_pct,
# MAGIC     ROUND(SUM(CASE WHEN ei.assetClass IN ('Commodity', 'Commodities',
# MAGIC                                            'Real Estate', 'Alternative',
# MAGIC                                            'Multi-Asset')
# MAGIC                    THEN h.market_value ELSE 0 END)
# MAGIC           / NULLIF(SUM(h.market_value), 0) * 100, 2)          AS etf_other_pct
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts ac ON h.account_id = ac.account_id
# MAGIC   JOIN bronze_etf_info ei ON h.ticker = ei.symbol
# MAGIC   WHERE h.asset_class = 'ETF'
# MAGIC   GROUP BY ac.client_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   g.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   g.asset_class,
# MAGIC
# MAGIC   -- ── Actuals ───────────────────────────────────────────────────────────────
# MAGIC   ROUND(COALESCE(ca.actual_market_value, 0), 2)        AS actual_market_value,
# MAGIC   ROUND(ct.total_client_value, 2)                      AS total_client_value,
# MAGIC   ROUND(
# MAGIC     COALESCE(ca.actual_market_value, 0)
# MAGIC     / NULLIF(ct.total_client_value, 0) * 100, 4)       AS actual_allocation_pct,
# MAGIC
# MAGIC   -- ── IPS targets ───────────────────────────────────────────────────────────
# MAGIC   it.target_allocation_pct,
# MAGIC   it.min_allocation_pct,
# MAGIC   it.max_allocation_pct,
# MAGIC   it.rebalance_trigger_pct,
# MAGIC
# MAGIC   -- ── Drift ─────────────────────────────────────────────────────────────────
# MAGIC   ROUND(
# MAGIC     COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC     - it.target_allocation_pct, 4)                     AS drift_from_target_pct,
# MAGIC
# MAGIC   ROUND(
# MAGIC     GREATEST(0,
# MAGIC       COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC       - it.max_allocation_pct,
# MAGIC       it.min_allocation_pct
# MAGIC       - COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC     ), 4)                                              AS out_of_bounds_pct,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          > it.max_allocation_pct THEN 'Over Band'
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          < it.min_allocation_pct THEN 'Under Band'
# MAGIC     ELSE 'Within Band'
# MAGIC   END                                                  AS drift_status,
# MAGIC
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          > it.max_allocation_pct + it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          < it.min_allocation_pct - it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          > it.max_allocation_pct THEN 'Warning'
# MAGIC     WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC          < it.min_allocation_pct THEN 'Warning'
# MAGIC     ELSE 'OK'
# MAGIC   END                                                  AS drift_severity,
# MAGIC
# MAGIC   -- ── Rebalance ─────────────────────────────────────────────────────────────
# MAGIC   ROUND(
# MAGIC     it.target_allocation_pct / 100 * ct.total_client_value
# MAGIC     - COALESCE(ca.actual_market_value, 0), 2)          AS rebalance_to_target,
# MAGIC
# MAGIC   ROUND(
# MAGIC     CASE
# MAGIC       WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC            > it.max_allocation_pct
# MAGIC       THEN it.max_allocation_pct / 100 * ct.total_client_value
# MAGIC            - COALESCE(ca.actual_market_value, 0)
# MAGIC       WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC            < it.min_allocation_pct
# MAGIC       THEN it.min_allocation_pct / 100 * ct.total_client_value
# MAGIC            - COALESCE(ca.actual_market_value, 0)
# MAGIC       ELSE 0
# MAGIC     END, 2)                                            AS rebalance_to_band,
# MAGIC
# MAGIC   -- ── Client-level aggregate stats (same for all 6 rows of a client) ────────
# MAGIC   -- Use MAX() when aggregating in front-end to avoid double-counting.
# MAGIC   ROUND(SUM(
# MAGIC     ABS(
# MAGIC       COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC       - it.target_allocation_pct
# MAGIC     )
# MAGIC   ) OVER (PARTITION BY g.client_id) / 6, 4)           AS client_drift_score,
# MAGIC
# MAGIC   SUM(
# MAGIC     CASE
# MAGIC       WHEN COALESCE(ca.actual_market_value, 0) / NULLIF(ct.total_client_value, 0) * 100
# MAGIC            NOT BETWEEN it.min_allocation_pct AND it.max_allocation_pct
# MAGIC       THEN 1 ELSE 0
# MAGIC     END
# MAGIC   ) OVER (PARTITION BY g.client_id)                   AS client_breach_count,
# MAGIC
# MAGIC   ROUND(it.target_allocation_pct / 100 * ct.total_client_value, 2)
# MAGIC                                                        AS target_market_value,
# MAGIC   ROUND(it.min_allocation_pct    / 100 * ct.total_client_value, 2)
# MAGIC                                                        AS min_market_value,
# MAGIC   ROUND(it.max_allocation_pct    / 100 * ct.total_client_value, 2)
# MAGIC                                                        AS max_market_value,
# MAGIC
# MAGIC   -- ── ETF sub-type enrichment (ETF rows only) ──────────────────────────────
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ec.etf_asset_classes   ELSE NULL END AS etf_asset_classes,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ec.etf_class_count      ELSE NULL END AS etf_class_count,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ec.etf_equity_pct       ELSE NULL END AS etf_equity_pct,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ec.etf_fixed_income_pct ELSE NULL END AS etf_fixed_income_pct,
# MAGIC   CASE WHEN g.asset_class = 'ETF' THEN ec.etf_other_pct        ELSE NULL END AS etf_other_pct,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                                  AS updated_at
# MAGIC
# MAGIC FROM client_class_grid g
# MAGIC JOIN clients c        ON g.client_id   = c.client_id
# MAGIC JOIN client_totals ct ON g.client_id   = ct.client_id
# MAGIC JOIN ips_targets it   ON c.risk_profile = it.risk_profile
# MAGIC                      AND g.asset_class  = it.asset_class
# MAGIC LEFT JOIN client_class_actual ca
# MAGIC   ON g.client_id = ca.client_id AND g.asset_class = ca.asset_class
# MAGIC LEFT JOIN etf_class_by_client ec
# MAGIC   ON g.client_id = ec.client_id

# COMMAND ----------
# DBTITLE 1,─── SECTION B: LAKEVIEW DASHBOARD QUERIES ─────────────────────────────────────
# Lakeview named parameter syntax: :param_name
# Filter pattern: (array_contains(:param, column) OR :param IS NULL)
# All filters are optional — omit or leave NULL to include all.

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 1 — IPS Drift Overview (Ranked by Breach Severity)
# MAGIC %sql
# MAGIC -- All accounts ranked by out-of-bounds severity.
# MAGIC -- Each row is one (account × asset_class) cell; breached cells rise to the top.
# MAGIC -- Supports: advisor_id, account_type, client_id, asset_class, drift_status filters.
# MAGIC SELECT
# MAGIC   d.client_name,
# MAGIC   d.client_id,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   d.account_id,
# MAGIC   d.account_name,
# MAGIC   d.account_type,
# MAGIC   d.asset_class,
# MAGIC   d.drift_status,
# MAGIC   d.drift_severity,
# MAGIC
# MAGIC   -- Allocation values
# MAGIC   ROUND(d.actual_allocation_pct,  2)              AS actual_pct,
# MAGIC   d.target_allocation_pct                         AS target_pct,
# MAGIC   d.min_allocation_pct                            AS min_pct,
# MAGIC   d.max_allocation_pct                            AS max_pct,
# MAGIC   d.drift_from_target_pct,
# MAGIC   d.out_of_bounds_pct,
# MAGIC   d.band_distance_pct,
# MAGIC
# MAGIC   -- Dollar context
# MAGIC   ROUND(d.actual_market_value    / 1e6, 3)        AS actual_mv_m,
# MAGIC   ROUND(d.target_market_value    / 1e6, 3)        AS target_mv_m,
# MAGIC   ROUND(d.total_account_value    / 1e6, 3)        AS account_aum_m,
# MAGIC   ROUND(d.rebalance_to_target    / 1e6, 3)        AS rebalance_to_target_m,
# MAGIC   ROUND(d.rebalance_to_band      / 1e6, 3)        AS rebalance_to_band_m,
# MAGIC   d.positions_count
# MAGIC
# MAGIC FROM gold_account_ips_drift d
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   d.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, d.account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    d.client_id)    OR :client_id    IS NULL)
# MAGIC   AND (array_contains(:asset_class,  d.asset_class)  OR :asset_class  IS NULL)
# MAGIC   AND (d.drift_status = :drift_status               OR :drift_status  IS NULL)
# MAGIC ORDER BY
# MAGIC   d.out_of_bounds_pct DESC,
# MAGIC   ABS(d.drift_from_target_pct) DESC

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 2 — Allocation vs Band (Actual / Target / Min / Max per Account)
# MAGIC %sql
# MAGIC -- For each filtered account and asset class, shows the four key values:
# MAGIC -- actual, target, min, max. Ideal for a grouped bar or bullet chart in Lakeview.
# MAGIC -- Pivot by asset_class on the front end if desired.
# MAGIC SELECT
# MAGIC   d.account_id,
# MAGIC   d.account_name,
# MAGIC   d.account_type,
# MAGIC   d.client_id,
# MAGIC   d.client_name,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   d.asset_class,
# MAGIC   d.drift_status,
# MAGIC   d.drift_severity,
# MAGIC
# MAGIC   -- All four allocation points
# MAGIC   ROUND(d.actual_allocation_pct,   2) AS actual_pct,
# MAGIC   d.target_allocation_pct              AS target_pct,
# MAGIC   d.min_allocation_pct                 AS min_pct,
# MAGIC   d.max_allocation_pct                 AS max_pct,
# MAGIC
# MAGIC   -- Drift metrics
# MAGIC   d.drift_from_target_pct,
# MAGIC   d.out_of_bounds_pct,
# MAGIC   d.band_distance_pct,
# MAGIC
# MAGIC   -- Dollar values (for tooltip / detail)
# MAGIC   ROUND(d.actual_market_value / 1e6, 3) AS actual_mv_m,
# MAGIC   ROUND(d.target_market_value / 1e6, 3) AS target_mv_m,
# MAGIC   ROUND(d.min_market_value    / 1e6, 3) AS min_mv_m,
# MAGIC   ROUND(d.max_market_value    / 1e6, 3) AS max_mv_m,
# MAGIC   ROUND(d.total_account_value / 1e6, 3) AS account_aum_m,
# MAGIC   d.positions_count
# MAGIC
# MAGIC FROM gold_account_ips_drift d
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   d.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, d.account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    d.client_id)    OR :client_id    IS NULL)
# MAGIC   AND (array_contains(:asset_class,  d.asset_class)  OR :asset_class  IS NULL)
# MAGIC ORDER BY d.client_name, d.account_id, d.asset_class

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 3 — Drift Heatmap (All Clients × Asset Classes)
# MAGIC %sql
# MAGIC -- Client-level drift across all 6 asset classes.
# MAGIC -- One row per (client_id, asset_class) — ideal for a pivot/heatmap in Lakeview
# MAGIC -- where rows = clients, columns = asset classes, color = drift_severity.
# MAGIC SELECT
# MAGIC   d.client_id,
# MAGIC   d.client_name,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   d.asset_class,
# MAGIC   d.drift_status,
# MAGIC   d.drift_severity,
# MAGIC   ROUND(d.actual_allocation_pct,  2)               AS actual_pct,
# MAGIC   d.target_allocation_pct,
# MAGIC   d.min_allocation_pct,
# MAGIC   d.max_allocation_pct,
# MAGIC   d.drift_from_target_pct,
# MAGIC   d.out_of_bounds_pct,
# MAGIC   ROUND(d.actual_market_value / 1e6, 3)            AS actual_mv_m,
# MAGIC   ROUND(d.total_client_value  / 1e6, 3)            AS client_aum_m,
# MAGIC   ROUND(d.rebalance_to_band   / 1e6, 3)            AS rebalance_to_band_m,
# MAGIC   d.client_drift_score,
# MAGIC   d.client_breach_count
# MAGIC FROM gold_client_ips_drift d
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,  d.advisor_id)  OR :advisor_id  IS NULL)
# MAGIC   AND (array_contains(:client_id,   d.client_id)   OR :client_id   IS NULL)
# MAGIC   AND (array_contains(:asset_class, d.asset_class) OR :asset_class IS NULL)
# MAGIC   AND (d.drift_status = :drift_status              OR :drift_status IS NULL)
# MAGIC ORDER BY d.client_drift_score DESC, d.client_name, d.asset_class

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 4 — Advisor Book: Drift Summary per Advisor
# MAGIC %sql
# MAGIC -- Rolls up drift across each advisor's entire book of clients.
# MAGIC -- Shows how many clients / accounts have breaches and the total rebalance $$.
# MAGIC WITH
# MAGIC advisor_stats AS (
# MAGIC   SELECT
# MAGIC     d.advisor_id,
# MAGIC     COUNT(DISTINCT d.client_id)                           AS total_clients,
# MAGIC     COUNT(DISTINCT d.account_id)                          AS total_accounts,
# MAGIC     ROUND(SUM(d.total_account_value) / 6 / 1e9, 3)        AS total_aum_b,
# MAGIC     COUNT(DISTINCT CASE WHEN d.drift_status != 'Within Band'
# MAGIC                    THEN d.client_id END)                   AS clients_with_drift,
# MAGIC     COUNT(DISTINCT CASE WHEN d.drift_status != 'Within Band'
# MAGIC                    THEN d.account_id END)                  AS accounts_with_drift,
# MAGIC     SUM(CASE WHEN d.drift_status != 'Within Band'
# MAGIC             THEN 1 ELSE 0 END)                            AS total_breach_cells,
# MAGIC     SUM(CASE WHEN d.drift_severity = 'Critical'
# MAGIC             THEN 1 ELSE 0 END)                            AS critical_breach_cells,
# MAGIC     ROUND(SUM(ABS(d.rebalance_to_band)) / 1e6, 2)         AS total_rebalance_abs_m,
# MAGIC     ROUND(AVG(ABS(d.drift_from_target_pct)), 4)           AS avg_abs_drift_pct
# MAGIC   FROM gold_account_ips_drift d
# MAGIC   GROUP BY d.advisor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   advisor_id,
# MAGIC   total_clients,
# MAGIC   total_accounts,
# MAGIC   total_aum_b,
# MAGIC   clients_with_drift,
# MAGIC   accounts_with_drift,
# MAGIC   total_breach_cells,
# MAGIC   critical_breach_cells,
# MAGIC   ROUND(clients_with_drift / NULLIF(total_clients, 0) * 100, 1) AS pct_clients_with_drift,
# MAGIC   total_rebalance_abs_m,
# MAGIC   avg_abs_drift_pct
# MAGIC FROM advisor_stats
# MAGIC WHERE (array_contains(:advisor_id, advisor_id) OR :advisor_id IS NULL)
# MAGIC ORDER BY total_rebalance_abs_m DESC

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 5 — Rebalance Opportunity (Accounts Needing Action)
# MAGIC %sql
# MAGIC -- One row per account: total absolute rebalance $ and breach details.
# MAGIC -- Prioritized by total rebalance size — focus advisor attention first.
# MAGIC WITH
# MAGIC account_summary AS (
# MAGIC   SELECT
# MAGIC     d.account_id,
# MAGIC     d.account_name,
# MAGIC     d.account_type,
# MAGIC     d.client_id,
# MAGIC     d.client_name,
# MAGIC     d.advisor_id,
# MAGIC     d.tier,
# MAGIC     d.risk_profile,
# MAGIC     ROUND(MAX(d.total_account_value) / 1e6, 3)             AS account_aum_m,
# MAGIC     SUM(CASE WHEN d.drift_status != 'Within Band' THEN 1 ELSE 0 END)
# MAGIC                                                            AS breach_count,
# MAGIC     SUM(CASE WHEN d.drift_severity = 'Critical'  THEN 1 ELSE 0 END)
# MAGIC                                                            AS critical_count,
# MAGIC     ROUND(SUM(ABS(d.rebalance_to_band))   / 1e6, 3)        AS rebalance_to_band_abs_m,
# MAGIC     ROUND(SUM(ABS(d.rebalance_to_target)) / 1e6, 3)        AS rebalance_to_target_abs_m,
# MAGIC     ROUND(AVG(ABS(d.drift_from_target_pct)), 4)            AS avg_drift_pct,
# MAGIC     MAX(d.out_of_bounds_pct)                               AS max_out_of_bounds_pct,
# MAGIC     -- Which asset classes are breached
# MAGIC     ARRAY_JOIN(
# MAGIC       ARRAY_SORT(COLLECT_SET(
# MAGIC         CASE WHEN d.drift_status != 'Within Band'
# MAGIC         THEN CONCAT(d.asset_class, ' (', d.drift_status, ')')
# MAGIC         END
# MAGIC       )), ', ')                                            AS breached_classes
# MAGIC   FROM gold_account_ips_drift d
# MAGIC   GROUP BY d.account_id, d.account_name, d.account_type, d.client_id,
# MAGIC            d.client_name, d.advisor_id, d.tier, d.risk_profile
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM account_summary
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    client_id)    OR :client_id    IS NULL)
# MAGIC   AND breach_count > 0
# MAGIC ORDER BY rebalance_to_band_abs_m DESC

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 6 — ETF Bucket Breakdown by Asset Sub-Class
# MAGIC %sql
# MAGIC -- Drills into the ETF allocation bucket to show what types of ETFs are held.
# MAGIC -- Uses bronze_etf_info.assetClass to classify each ETF position (Equity, Fixed Income, etc.)
# MAGIC -- One row per (account_id, ETF ticker) — aggregate by etf_asset_class on front end.
# MAGIC WITH
# MAGIC filtered_accounts AS (
# MAGIC   SELECT DISTINCT a.account_id
# MAGIC   FROM accounts a
# MAGIC   JOIN clients c ON a.client_id = c.client_id
# MAGIC   WHERE
# MAGIC     (array_contains(:advisor_id,   c.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:client_id,    a.client_id)    OR :client_id    IS NULL)
# MAGIC )
# MAGIC SELECT
# MAGIC   h.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   a.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   h.ticker,
# MAGIC   ei.name                                          AS etf_name,
# MAGIC   ei.assetClass                                    AS etf_asset_class,
# MAGIC   ei.expenseRatio,
# MAGIC   ei.assetsUnderManagement                         AS etf_aum,
# MAGIC   ROUND(h.market_value, 2)                         AS position_market_value,
# MAGIC   ROUND(h.market_value / NULLIF(
# MAGIC     SUM(h.market_value) OVER (PARTITION BY h.account_id), 0) * 100, 4)
# MAGIC                                                    AS pct_of_account,
# MAGIC   -- IPS context for the ETF bucket
# MAGIC   d.actual_allocation_pct                          AS etf_bucket_actual_pct,
# MAGIC   d.target_allocation_pct                          AS etf_bucket_target_pct,
# MAGIC   d.min_allocation_pct                             AS etf_bucket_min_pct,
# MAGIC   d.max_allocation_pct                             AS etf_bucket_max_pct,
# MAGIC   d.drift_status                                   AS etf_bucket_drift_status,
# MAGIC   d.out_of_bounds_pct                              AS etf_bucket_out_of_bounds_pct,
# MAGIC   -- ETF sub-type mix for this account
# MAGIC   d.etf_asset_classes,
# MAGIC   d.etf_equity_pct,
# MAGIC   d.etf_fixed_income_pct,
# MAGIC   d.etf_other_pct
# MAGIC FROM holdings h
# MAGIC JOIN accounts a    ON h.account_id = a.account_id
# MAGIC JOIN clients  c    ON a.client_id  = c.client_id
# MAGIC JOIN bronze_etf_info ei ON h.ticker = ei.symbol
# MAGIC JOIN filtered_accounts fa ON h.account_id = fa.account_id
# MAGIC -- Bring in the IPS drift context for the ETF bucket
# MAGIC LEFT JOIN gold_account_ips_drift d
# MAGIC   ON h.account_id = d.account_id AND d.asset_class = 'ETF'
# MAGIC WHERE h.asset_class = 'ETF'
# MAGIC   AND (array_contains(:ticker, h.ticker) OR :ticker IS NULL)
# MAGIC ORDER BY h.account_id, h.market_value DESC

# COMMAND ----------
# DBTITLE 1,─── SECTION C: GENIE CONTEXT QUERIES ─────────────────────────────────────────
# Static SQL designed for Genie space context.
# Each query answers a specific natural language question an advisor might ask.
# Paste the SQL block into Genie alongside its suggested question.

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which clients have the worst IPS drift?"
# MAGIC %sql
# MAGIC -- Ranks all clients by drift score (avg |drift from target| across 6 asset classes).
# MAGIC -- Higher score = portfolio has drifted further from IPS targets overall.
# MAGIC SELECT
# MAGIC   d.client_id,
# MAGIC   d.client_name,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   ROUND(MAX(d.total_client_value) / 1e6, 3)   AS client_aum_m,
# MAGIC   MAX(d.client_drift_score)                    AS drift_score,
# MAGIC   MAX(d.client_breach_count)                   AS breach_count,
# MAGIC   -- List of breached asset classes for at-a-glance review
# MAGIC   ARRAY_JOIN(
# MAGIC     ARRAY_SORT(COLLECT_SET(
# MAGIC       CASE WHEN d.drift_status != 'Within Band'
# MAGIC       THEN CONCAT(d.asset_class, ': ', d.drift_status,
# MAGIC                   ' (', ROUND(d.actual_allocation_pct, 1), '% vs ',
# MAGIC                   d.min_allocation_pct, '-', d.max_allocation_pct, '%)')
# MAGIC       END
# MAGIC     )), ' | ')                                 AS breach_detail,
# MAGIC   ROUND(SUM(ABS(d.rebalance_to_band)) / 1e6, 3) AS total_rebalance_abs_m
# MAGIC FROM gold_client_ips_drift d
# MAGIC GROUP BY d.client_id, d.client_name, d.advisor_id, d.tier, d.risk_profile
# MAGIC ORDER BY drift_score DESC
# MAGIC LIMIT 30

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which accounts are overweight in Private Credit?"
# MAGIC %sql
# MAGIC -- Finds every account where Private Credit allocation exceeds the IPS maximum.
# MAGIC -- Key for the demo: UHNW clients with BDC positions above their 10-15% cap.
# MAGIC SELECT
# MAGIC   d.account_id,
# MAGIC   d.account_name,
# MAGIC   d.account_type,
# MAGIC   d.client_id,
# MAGIC   d.client_name,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   ROUND(d.actual_allocation_pct,  2) AS actual_pct,
# MAGIC   d.target_allocation_pct            AS target_pct,
# MAGIC   d.max_allocation_pct               AS max_pct,
# MAGIC   d.drift_from_target_pct,
# MAGIC   d.out_of_bounds_pct,
# MAGIC   d.drift_severity,
# MAGIC   ROUND(d.actual_market_value  / 1e6, 3) AS actual_mv_m,
# MAGIC   ROUND(d.total_account_value  / 1e6, 3) AS account_aum_m,
# MAGIC   ROUND(d.rebalance_to_band    / 1e6, 3) AS rebalance_to_band_m,
# MAGIC   ROUND(d.rebalance_to_target  / 1e6, 3) AS rebalance_to_target_m,
# MAGIC   d.positions_count
# MAGIC FROM gold_account_ips_drift d
# MAGIC WHERE d.asset_class = 'Private Credit'
# MAGIC   AND d.drift_status = 'Over Band'
# MAGIC ORDER BY d.out_of_bounds_pct DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "Show me all accounts outside their IPS bounds"
# MAGIC %sql
# MAGIC -- All (account × asset_class) cells currently breaching the IPS min/max band.
# MAGIC -- Sorted by out-of-bounds distance so the worst breaches appear first.
# MAGIC SELECT
# MAGIC   d.client_name,
# MAGIC   d.advisor_id,
# MAGIC   d.tier,
# MAGIC   d.risk_profile,
# MAGIC   d.account_id,
# MAGIC   d.account_name,
# MAGIC   d.account_type,
# MAGIC   d.asset_class,
# MAGIC   d.drift_status,
# MAGIC   d.drift_severity,
# MAGIC   ROUND(d.actual_allocation_pct, 2)          AS actual_pct,
# MAGIC   d.target_allocation_pct                    AS target_pct,
# MAGIC   d.min_allocation_pct                       AS min_pct,
# MAGIC   d.max_allocation_pct                       AS max_pct,
# MAGIC   d.out_of_bounds_pct,
# MAGIC   ROUND(d.actual_market_value / 1e6, 3)      AS actual_mv_m,
# MAGIC   ROUND(d.total_account_value / 1e6, 3)      AS account_aum_m,
# MAGIC   ROUND(d.rebalance_to_band   / 1e6, 3)      AS rebalance_to_band_m
# MAGIC FROM gold_account_ips_drift d
# MAGIC WHERE d.drift_status != 'Within Band'
# MAGIC ORDER BY d.out_of_bounds_pct DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "What is the total rebalance amount needed across all accounts?"
# MAGIC %sql
# MAGIC -- Aggregates rebalance dollars by asset class and drift direction.
# MAGIC -- Shows where the largest buy/sell flows would be required to restore IPS compliance.
# MAGIC SELECT
# MAGIC   d.asset_class,
# MAGIC   d.drift_status,
# MAGIC   COUNT(DISTINCT d.account_id)                               AS accounts_impacted,
# MAGIC   COUNT(DISTINCT d.client_id)                                AS clients_impacted,
# MAGIC   ROUND(SUM(d.actual_market_value)    / 1e9, 3)              AS total_actual_b,
# MAGIC   ROUND(SUM(d.target_market_value)    / 1e9, 3)              AS total_target_b,
# MAGIC   ROUND(SUM(d.rebalance_to_band)      / 1e6, 2)              AS rebalance_to_band_m,
# MAGIC   ROUND(SUM(d.rebalance_to_target)    / 1e6, 2)              AS rebalance_to_target_m,
# MAGIC   ROUND(SUM(ABS(d.rebalance_to_band)) / 1e6, 2)              AS rebalance_abs_m,
# MAGIC   ROUND(AVG(d.out_of_bounds_pct), 4)                         AS avg_out_of_bounds_pct
# MAGIC FROM gold_account_ips_drift d
# MAGIC WHERE d.drift_status != 'Within Band'
# MAGIC GROUP BY d.asset_class, d.drift_status
# MAGIC ORDER BY rebalance_abs_m DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which advisors have the most clients with IPS drift?"
# MAGIC %sql
# MAGIC -- Advisor-level view: clients affected, breach count, and rebalance urgency.
# MAGIC SELECT
# MAGIC   d.advisor_id,
# MAGIC   COUNT(DISTINCT d.client_id)                                AS total_clients,
# MAGIC   COUNT(DISTINCT d.account_id)                               AS total_accounts,
# MAGIC   ROUND(SUM(d.total_account_value) / 6 / 1e9, 3)            AS book_aum_b,
# MAGIC   COUNT(DISTINCT CASE WHEN d.drift_status != 'Within Band'
# MAGIC                  THEN d.client_id END)                       AS clients_with_drift,
# MAGIC   SUM(CASE WHEN d.drift_severity = 'Critical' THEN 1 ELSE 0 END)
# MAGIC                                                              AS critical_cells,
# MAGIC   ROUND(
# MAGIC     COUNT(DISTINCT CASE WHEN d.drift_status != 'Within Band' THEN d.client_id END)
# MAGIC     / NULLIF(COUNT(DISTINCT d.client_id), 0) * 100, 1)      AS pct_clients_drifted,
# MAGIC   ROUND(SUM(ABS(d.rebalance_to_band)) / 1e6, 2)             AS total_rebalance_abs_m
# MAGIC FROM gold_account_ips_drift d
# MAGIC GROUP BY d.advisor_id
# MAGIC ORDER BY total_rebalance_abs_m DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "What is the average allocation vs target by asset class across all clients?"
# MAGIC %sql
# MAGIC -- Book-wide view: for each asset class, how does the average actual allocation
# MAGIC -- compare to the expected target? Reveals systematic tilts across the portfolio.
# MAGIC SELECT
# MAGIC   d.asset_class,
# MAGIC   ROUND(AVG(d.actual_allocation_pct),   4) AS avg_actual_pct,
# MAGIC   ROUND(AVG(d.target_allocation_pct),   4) AS avg_target_pct,
# MAGIC   ROUND(AVG(d.drift_from_target_pct),   4) AS avg_drift_pct,
# MAGIC   ROUND(AVG(d.out_of_bounds_pct),       4) AS avg_out_of_bounds_pct,
# MAGIC   COUNT(DISTINCT d.account_id)              AS total_accounts,
# MAGIC   SUM(CASE WHEN d.drift_status = 'Over Band'  THEN 1 ELSE 0 END) AS over_band_count,
# MAGIC   SUM(CASE WHEN d.drift_status = 'Under Band' THEN 1 ELSE 0 END) AS under_band_count,
# MAGIC   SUM(CASE WHEN d.drift_status = 'Within Band' THEN 1 ELSE 0 END) AS in_band_count,
# MAGIC   ROUND(SUM(d.actual_market_value)  / 1e9, 3) AS total_actual_b,
# MAGIC   ROUND(SUM(d.target_market_value)  / 1e9, 3) AS total_target_b,
# MAGIC   ROUND(SUM(ABS(d.rebalance_to_band)) / 1e6, 2) AS total_rebalance_abs_m
# MAGIC FROM gold_account_ips_drift d
# MAGIC GROUP BY d.asset_class
# MAGIC ORDER BY ABS(avg_drift_pct) DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "What types of ETFs are in the portfolio and how are they allocated?"
# MAGIC %sql
# MAGIC -- Shows the ETF bucket composition across all accounts, segmented by ETF asset class.
# MAGIC -- Uses bronze_etf_info.assetClass to reveal whether the ETF bucket is equity-heavy,
# MAGIC -- fixed-income-heavy, or mixed — critical for understanding true economic exposure.
# MAGIC SELECT
# MAGIC   ei.assetClass                                      AS etf_asset_class,
# MAGIC   COUNT(DISTINCT h.ticker)                           AS distinct_etfs,
# MAGIC   COUNT(DISTINCT a.client_id)                        AS clients_holding,
# MAGIC   ROUND(SUM(h.market_value) / 1e9, 3)               AS total_mv_b,
# MAGIC   ROUND(SUM(h.market_value) / NULLIF(
# MAGIC     SUM(SUM(h.market_value)) OVER (), 0) * 100, 2)   AS pct_of_etf_bucket,
# MAGIC   ROUND(AVG(ei.expenseRatio), 4)                    AS avg_expense_ratio,
# MAGIC   ARRAY_JOIN(ARRAY_SORT(COLLECT_SET(h.ticker)), ', ') AS tickers
# MAGIC FROM holdings h
# MAGIC JOIN accounts a    ON h.account_id = a.account_id
# MAGIC JOIN bronze_etf_info ei ON h.ticker = ei.symbol
# MAGIC WHERE h.asset_class = 'ETF'
# MAGIC GROUP BY ei.assetClass
# MAGIC ORDER BY total_mv_b DESC
