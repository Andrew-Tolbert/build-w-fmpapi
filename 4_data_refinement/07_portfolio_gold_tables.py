# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///

# COMMAND ----------

# MAGIC %md
# MAGIC # AWM — Portfolio Gold Tables
# MAGIC
# MAGIC Builds `silver_advisor_daily_returns` and all `gold_app_*` tables consumed by the
# MAGIC Advisor Intelligence App and the Lakeview return-series dashboard.
# MAGIC
# MAGIC Run order: after `06_dashboard_tables.py` (requires `gold_ips_drift` and
# MAGIC `gold_unified_signals`).
# MAGIC
# MAGIC Run this notebook to refresh after any of the following change:
# MAGIC   - holdings / transactions (synthetic rebuild or new data)
# MAGIC   - bronze_historical_prices (daily price ingest)
# MAGIC   - gold_unified_signals (daily/monthly refinement)
# MAGIC
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `silver_advisor_daily_returns` | Daily portfolio vs S&P 500 return timeseries per advisor, trailing 365 days |
# MAGIC | `gold_account_ips_drift` | Materialized IPS drift table — one row per (account × asset class) |
# MAGIC | `gold_app_portfolio_summary` | One row per advisor_id — KPI stat cards |
# MAGIC | `gold_app_asset_allocation` | Advisor book weighted by asset class |
# MAGIC | `gold_app_performance_timeseries` | Daily cumulative returns for area chart |
# MAGIC | `gold_app_top_holdings` | Top holdings per advisor with risk flags |
# MAGIC | `gold_app_concentration_risk` | IPS drift heatmap for top 5 clients per advisor |
# MAGIC | `gold_app_holdings_list` | Distinct advisor × ticker list with alert flag |
# MAGIC | `gold_app_management_tone` | Latest management tone scores from earnings transcripts |
# MAGIC
# MAGIC Lakeview reference queries (no CREATE — Lakeview dataset queries only):
# MAGIC   - Holdings by date range with ETF look-through and alpha contributions
# MAGIC   - Signal feed and signal exposure by account
# MAGIC   - Portfolio return series (parameterized by date range, benchmark, advisor)

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## silver_advisor_daily_returns
# MAGIC One row per (date, advisor_id). Covers the trailing 365 calendar days.
# MAGIC Returns are indexed to 0% at the first trading day in the window:
# MAGIC   - portfolio_baseline = actual advisor AUM on day 1 (not a computed cost basis)
# MAGIC   - benchmark_baseline = GSPC close on day 1
# MAGIC
# MAGIC This guarantees both series start at exactly 0.0 on the first row.
# MAGIC
# MAGIC **Key table:** pre-computed cumulative portfolio return, benchmark return (GSPC),
# MAGIC and alpha per advisor per trading day. Required by all `gold_app_*` performance tables.

# COMMAND ----------

# DBTITLE 1,Silver_Advisor_Daily_Returns
# MAGIC %sql
# MAGIC -- One row per (date, advisor_id). Covers the trailing 365 calendar days.
# MAGIC -- Returns are indexed to 0% at the first trading day in the window:
# MAGIC --   portfolio_baseline = actual advisor AUM on day 1 (not a computed cost basis)
# MAGIC --   benchmark_baseline = GSPC close on day 1
# MAGIC -- This guarantees both series start at exactly 0.0 on the first row.
# MAGIC CREATE OR REPLACE TABLE silver_advisor_daily_returns
# MAGIC   COMMENT 'Daily portfolio vs S&P 500 (GSPC) return timeseries per advisor, trailing 365 days.' AS
# MAGIC WITH -- ── Window: trailing 365 calendar days ───────────────────────────────────────
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC     DATE_SUB(CURRENT_DATE(), 365) AS start_dt,
# MAGIC     CURRENT_DATE() AS end_dt
# MAGIC ),
# MAGIC -- ── Nearest available trading day on or before each bound ────────────────────
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           date
# MAGIC             <= (
# MAGIC               SELECT
# MAGIC                 end_dt
# MAGIC               FROM
# MAGIC                 params
# MAGIC             )
# MAGIC         THEN
# MAGIC           date
# MAGIC       END
# MAGIC     ) AS end_price_dt,
# MAGIC     MAX(
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           date
# MAGIC             <= (
# MAGIC               SELECT
# MAGIC                 start_dt
# MAGIC               FROM
# MAGIC                 params
# MAGIC             )
# MAGIC         THEN
# MAGIC           date
# MAGIC       END
# MAGIC     ) AS start_price_dt
# MAGIC   FROM
# MAGIC     bronze_historical_prices
# MAGIC ),
# MAGIC -- ── All trading days in the window ───────────────────────────────────────────
# MAGIC trading_days AS (
# MAGIC   SELECT DISTINCT
# MAGIC     date
# MAGIC   FROM
# MAGIC     bronze_historical_prices
# MAGIC   WHERE
# MAGIC     date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC     AND date
# MAGIC       <= (
# MAGIC         SELECT
# MAGIC           end_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Equity positions as of today, carrying advisor_id ────────────────────────
# MAGIC filtered_positions AS (
# MAGIC   SELECT
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id,
# MAGIC     SUM(t.quantity) AS quantity,
# MAGIC     SUM(t.gross_amount) AS total_cost
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN accounts a
# MAGIC         ON t.account_id = a.account_id
# MAGIC       JOIN clients c
# MAGIC         ON a.client_id = c.client_id
# MAGIC   WHERE
# MAGIC     t.action IN ('BUY', 'DRIP')
# MAGIC     AND t.ticker != 'CASH'
# MAGIC     AND t.date
# MAGIC       <= (
# MAGIC         SELECT
# MAGIC           end_dt
# MAGIC         FROM
# MAGIC           params
# MAGIC       )
# MAGIC   GROUP BY
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id
# MAGIC ),
# MAGIC -- ── Daily portfolio value per advisor ─────────────────────────────────────────
# MAGIC daily_portfolio AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     fp.advisor_id,
# MAGIC     SUM(fp.quantity * hp.adjClose) AS portfolio_value
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN filtered_positions fp
# MAGIC       JOIN bronze_historical_prices hp
# MAGIC         ON hp.symbol = fp.ticker
# MAGIC         AND hp.date = td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     fp.advisor_id
# MAGIC ),
# MAGIC -- ── Portfolio baseline = actual AUM on day 1 ─────────────────────────────────
# MAGIC portfolio_baseline AS (
# MAGIC   SELECT
# MAGIC     advisor_id,
# MAGIC     portfolio_value AS base
# MAGIC   FROM
# MAGIC     daily_portfolio
# MAGIC   WHERE
# MAGIC     date
# MAGIC       = (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Daily S&P 500 series (symbol = 'GSPC') ───────────────────────────────────
# MAGIC daily_benchmark AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     MAX_BY(v.close, v.date) AS benchmark_value
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       LEFT JOIN bronze_indexes_and_vix v
# MAGIC         ON v.symbol = 'GSPC'
# MAGIC         AND v.date = td.date
# MAGIC   GROUP BY
# MAGIC     td.date
# MAGIC ),
# MAGIC -- ── Benchmark baseline = GSPC close on day 1 ─────────────────────────────────
# MAGIC benchmark_baseline AS (
# MAGIC   SELECT
# MAGIC     benchmark_value AS base
# MAGIC   FROM
# MAGIC     daily_benchmark
# MAGIC   WHERE
# MAGIC     date
# MAGIC       = (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Fee transactions per advisor, pre-filtered to the window ─────────────────
# MAGIC advisor_fees AS (
# MAGIC   SELECT
# MAGIC     f.date,
# MAGIC     f.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions f
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON f.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     f.action = 'FEE'
# MAGIC     AND f.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative fees per advisor as a step function ────────────────────────────
# MAGIC fees_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ABS(af.net_amount)), 0) AS cumulative_fees
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_fees af
# MAGIC         ON af.advisor_id = adv.advisor_id
# MAGIC         AND af.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC -- ── Inflow transactions per advisor (cash deposits = BUY where ticker = CASH) ─
# MAGIC -- Mirrors the cash_positions logic in holdings_by_date_range: initial capital
# MAGIC -- coming into an account is recorded as BUY / CASH. No window filter — we want
# MAGIC -- the full cumulative inflow from the start of the return series.
# MAGIC advisor_inflows AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.quantity AS amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON t.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     t.action = 'BUY'
# MAGIC     AND t.ticker = 'CASH'
# MAGIC     AND t.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative inflows per advisor as a step function ─────────────────────────
# MAGIC inflows_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ai.amount), 0) AS cumulative_inflows
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_inflows ai
# MAGIC         ON ai.advisor_id = adv.advisor_id
# MAGIC         AND ai.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC -- ── Dividend transactions per advisor, pre-filtered to the window ─────────────
# MAGIC -- net_amount on DIVIDEND rows is positive cash received (matches cash_positions logic).
# MAGIC advisor_dividends AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON t.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     t.action = 'DIVIDEND'
# MAGIC     AND t.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative dividends per advisor as a step function ───────────────────────
# MAGIC dividends_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ad.net_amount), 0) AS cumulative_dividends
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_dividends ad
# MAGIC         ON ad.advisor_id = adv.advisor_id
# MAGIC         AND ad.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC )
# MAGIC -- ── Final output — one row per (date, advisor_id) ─────────────────────────────
# MAGIC SELECT
# MAGIC   dp.date,
# MAGIC   dp.advisor_id,
# MAGIC   ROUND(dp.portfolio_value / NULLIF(pb.base, 0) - 1, 6) AS portfolio_return_before_fees,
# MAGIC   ROUND(
# MAGIC     (dp.portfolio_value - fd.cumulative_fees) / NULLIF(pb.base, 0) - 1,
# MAGIC     6
# MAGIC   ) AS portfolio_return_after_fees,
# MAGIC   ROUND(
# MAGIC     (dp.portfolio_value - fd.cumulative_fees) / NULLIF(pb.base, 0)
# MAGIC       - db.benchmark_value / NULLIF(bb.base, 0),
# MAGIC     6
# MAGIC   ) AS portfolio_alpha,
# MAGIC   ROUND(fd.cumulative_fees, 2) AS cumulative_fees,
# MAGIC   ROUND(id.cumulative_inflows, 2) AS cumulative_inflows,
# MAGIC   ROUND(dd.cumulative_dividends, 2) AS cumulative_dividends,
# MAGIC   ROUND(db.benchmark_value / NULLIF(bb.base, 0) - 1, 6) AS benchmark_return,
# MAGIC   'GSPC' AS benchmark
# MAGIC FROM
# MAGIC   daily_portfolio dp
# MAGIC     LEFT JOIN daily_benchmark db
# MAGIC       ON dp.date = db.date
# MAGIC     LEFT JOIN fees_by_day fd
# MAGIC       ON dp.date = fd.date
# MAGIC       AND dp.advisor_id = fd.advisor_id
# MAGIC     LEFT JOIN inflows_by_day id
# MAGIC       ON dp.date = id.date
# MAGIC       AND dp.advisor_id = id.advisor_id
# MAGIC     LEFT JOIN dividends_by_day dd
# MAGIC       ON dp.date = dd.date
# MAGIC       AND dp.advisor_id = dd.advisor_id
# MAGIC     JOIN portfolio_baseline pb
# MAGIC       ON dp.advisor_id = pb.advisor_id
# MAGIC     CROSS JOIN benchmark_baseline bb
# MAGIC ORDER BY
# MAGIC   dp.advisor_id,
# MAGIC   dp.date

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## `gold_account_ips_drift`
# MAGIC Materialized table version of `gold_ips_drift`. One row per (account_id × asset_class).
# MAGIC Queried directly by the advisor app — rebuilt here after every synthetic data refresh.

# COMMAND ----------

# DBTITLE 1,gold_account_ips_drift
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_account_ips_drift AS
# MAGIC WITH
# MAGIC account_totals AS (
# MAGIC   SELECT account_id, SUM(market_value) AS total_account_value
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC actual_by_class AS (
# MAGIC   SELECT account_id, asset_class,
# MAGIC     SUM(market_value) AS actual_market_value,
# MAGIC     COUNT(*)          AS positions_count
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id, asset_class
# MAGIC ),
# MAGIC account_class_grid AS (
# MAGIC   SELECT a.account_id, it.asset_class
# MAGIC   FROM (SELECT DISTINCT account_id FROM holdings) a
# MAGIC   CROSS JOIN (SELECT DISTINCT asset_class FROM ips_targets) it
# MAGIC )
# MAGIC SELECT
# MAGIC   c.advisor_id, c.client_id, c.client_name, c.tier, c.risk_profile,
# MAGIC   ac.account_id, ac.account_name, ac.account_type,
# MAGIC   g.asset_class,
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0), 2)                            AS actual_market_value,
# MAGIC   ROUND(at.total_account_value, 2)                                         AS total_account_value,
# MAGIC   COALESCE(ab.positions_count, 0)                                          AS positions_count,
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100, 4) AS actual_allocation_pct,
# MAGIC   it.target_allocation_pct, it.min_allocation_pct, it.max_allocation_pct, it.rebalance_trigger_pct,
# MAGIC   ROUND(it.target_allocation_pct / 100 * at.total_account_value, 2) AS target_market_value,
# MAGIC   ROUND(it.min_allocation_pct    / 100 * at.total_account_value, 2) AS min_market_value,
# MAGIC   ROUND(it.max_allocation_pct    / 100 * at.total_account_value, 2) AS max_market_value,
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 - it.target_allocation_pct, 4) AS drift_from_target_pct,
# MAGIC   ROUND(GREATEST(0,
# MAGIC     COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 - it.max_allocation_pct,
# MAGIC     it.min_allocation_pct - COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100
# MAGIC   ), 4) AS out_of_bounds_pct,
# MAGIC   ROUND(CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 > it.max_allocation_pct
# MAGIC       THEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 - it.max_allocation_pct
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 < it.min_allocation_pct
# MAGIC       THEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 - it.min_allocation_pct
# MAGIC     ELSE -LEAST(
# MAGIC       it.max_allocation_pct - COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100,
# MAGIC       COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 - it.min_allocation_pct)
# MAGIC   END, 4) AS band_distance_pct,
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 > it.max_allocation_pct THEN 'Over Band'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 < it.min_allocation_pct THEN 'Under Band'
# MAGIC     ELSE 'Within Band'
# MAGIC   END AS drift_status,
# MAGIC   CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 > it.max_allocation_pct + it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 < it.min_allocation_pct - it.rebalance_trigger_pct THEN 'Critical'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 > it.max_allocation_pct THEN 'Warning'
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 < it.min_allocation_pct THEN 'Warning'
# MAGIC     ELSE 'OK'
# MAGIC   END AS drift_severity,
# MAGIC   ROUND(it.target_allocation_pct / 100 * at.total_account_value - COALESCE(ab.actual_market_value, 0), 2) AS rebalance_to_target,
# MAGIC   ROUND(CASE
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 > it.max_allocation_pct
# MAGIC       THEN it.max_allocation_pct / 100 * at.total_account_value - COALESCE(ab.actual_market_value, 0)
# MAGIC     WHEN COALESCE(ab.actual_market_value, 0) / NULLIF(at.total_account_value, 0) * 100 < it.min_allocation_pct
# MAGIC       THEN it.min_allocation_pct / 100 * at.total_account_value - COALESCE(ab.actual_market_value, 0)
# MAGIC     ELSE 0
# MAGIC   END, 2) AS rebalance_to_band
# MAGIC FROM account_class_grid g
# MAGIC JOIN accounts       ac ON g.account_id   = ac.account_id
# MAGIC JOIN clients        c  ON ac.client_id   = c.client_id
# MAGIC JOIN account_totals at ON g.account_id   = at.account_id
# MAGIC JOIN ips_targets    it ON c.risk_profile = it.risk_profile AND g.asset_class = it.asset_class
# MAGIC LEFT JOIN actual_by_class ab ON g.account_id = ab.account_id AND g.asset_class = ab.asset_class

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `gold_app_portfolio_summary`
# MAGIC **Schema:** `advisor_id`, `total_aum`, `perf_vs_bench_pct`, `drift_count`, `clients_at_risk`, `qtd_aum_change`
# MAGIC
# MAGIC - **total_aum** — SUM of `clients.total_aum` in raw dollars; frontend formats to $B/$M
# MAGIC - **perf_vs_bench_pct** — `portfolio_alpha` from the latest row of `silver_advisor_daily_returns`
# MAGIC - **drift_count** — count of account × asset_class slots with a Critical IPS breach
# MAGIC - **clients_at_risk** — distinct clients with at least one Critical severity IPS breach
# MAGIC - **qtd_aum_change** — raw dollar change (total_aum × QTD return); frontend formats to $B/$M

# COMMAND ----------

# DBTITLE 1,gold_app_portfolio_summary
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_portfolio_summary AS
# MAGIC WITH
# MAGIC aum AS (
# MAGIC   SELECT advisor_id, SUM(total_aum) AS total_aum
# MAGIC   FROM ahtsa.awm.clients
# MAGIC   GROUP BY advisor_id
# MAGIC ),
# MAGIC perf AS (
# MAGIC   SELECT advisor_id, ROUND(portfolio_alpha * 100, 1) AS perf_vs_bench_pct
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC qtd_start AS (
# MAGIC   SELECT advisor_id, portfolio_return_before_fees AS qtd_start_return
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   WHERE date >= DATE_TRUNC('quarter', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC qtd_end AS (
# MAGIC   SELECT advisor_id, portfolio_return_before_fees AS qtd_end_return
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC qtd AS (
# MAGIC   SELECT
# MAGIC     s.advisor_id,
# MAGIC     ROUND((e.qtd_end_return - s.qtd_start_return) / (1 + s.qtd_start_return), 4) AS qtd_return
# MAGIC   FROM qtd_start s JOIN qtd_end e ON s.advisor_id = e.advisor_id
# MAGIC ),
# MAGIC drift AS (
# MAGIC   SELECT advisor_id, COUNT(*) AS drift_count
# MAGIC   FROM ahtsa.awm.gold_ips_drift
# MAGIC   WHERE drift_status != 'Within Band' AND drift_severity = 'Critical'
# MAGIC   GROUP BY advisor_id
# MAGIC ),
# MAGIC at_risk AS (
# MAGIC   SELECT d.advisor_id, COUNT(DISTINCT a.client_id) AS clients_at_risk
# MAGIC   FROM ahtsa.awm.gold_ips_drift d
# MAGIC   JOIN ahtsa.awm.accounts a ON d.account_id = a.account_id
# MAGIC   WHERE d.drift_severity = 'Critical'
# MAGIC   GROUP BY d.advisor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   a.advisor_id,
# MAGIC   a.total_aum,
# MAGIC   p.perf_vs_bench_pct,
# MAGIC   COALESCE(d.drift_count, 0)                              AS drift_count,
# MAGIC   COALESCE(ar.clients_at_risk, 0)                         AS clients_at_risk,
# MAGIC   ROUND(a.total_aum * COALESCE(q.qtd_return, 0), 0)      AS qtd_aum_change
# MAGIC FROM aum a
# MAGIC LEFT JOIN perf    p  ON a.advisor_id = p.advisor_id
# MAGIC LEFT JOIN qtd     q  ON a.advisor_id = q.advisor_id
# MAGIC LEFT JOIN drift   d  ON a.advisor_id = d.advisor_id
# MAGIC LEFT JOIN at_risk ar ON a.advisor_id = ar.advisor_id

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `gold_app_asset_allocation`
# MAGIC **Schema:** `advisor_id`, `asset_class`, `pct_of_portfolio`
# MAGIC
# MAGIC Weighted allocation of each advisor's book by asset class.
# MAGIC Window `SUM` is partitioned by `advisor_id` so percentages sum to 100% per advisor.

# COMMAND ----------

# DBTITLE 1,gold_app_asset_allocation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_asset_allocation AS
# MAGIC SELECT
# MAGIC   c.advisor_id,
# MAGIC   h.asset_class,
# MAGIC   ROUND(
# MAGIC     SUM(h.market_value) / SUM(SUM(h.market_value)) OVER (PARTITION BY c.advisor_id) * 100,
# MAGIC     1
# MAGIC   ) AS pct_of_portfolio
# MAGIC FROM ahtsa.awm.holdings h
# MAGIC JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC JOIN ahtsa.awm.clients  c ON a.client_id  = c.client_id
# MAGIC GROUP BY c.advisor_id, h.asset_class
# MAGIC ORDER BY c.advisor_id, pct_of_portfolio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `gold_app_performance_timeseries`
# MAGIC **Schema:** `advisor_id`, `date`, `portfolio_return`, `benchmark_return`
# MAGIC
# MAGIC Daily cumulative returns from `silver_advisor_daily_returns` — one row per advisor per trading day.
# MAGIC Both series are anchored to 0% on 2025-05-05. Returns are multiplied by 100 for display as percentages.
# MAGIC No month-end aggregation — the app renders the full daily series.

# COMMAND ----------

# DBTITLE 1,gold_app_performance_timeseries
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_performance_timeseries AS
# MAGIC SELECT
# MAGIC   advisor_id,
# MAGIC   date,
# MAGIC   ROUND(portfolio_return_before_fees * 100, 1) AS portfolio_return,
# MAGIC   ROUND(benchmark_return * 100, 1)             AS benchmark_return
# MAGIC FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC ORDER BY advisor_id, date

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. `gold_app_top_holdings`
# MAGIC **Schema:** `advisor_id`, `holding_id`, `name`, `asset_class`, `aum_millions`, `pct_of_portfolio`, `ytd_return`, `risk_flag`
# MAGIC
# MAGIC - Top 10 holdings **per advisor** by market value (CASH excluded)
# MAGIC - `pct_of_portfolio` — window partitioned by `advisor_id`
# MAGIC - `risk_flag` driven by `gold_unified_signals` in the last 30 days:
# MAGIC   `alert` ≥ 0.8 severity · `watch` ≥ 0.5 · `none` otherwise
# MAGIC - `ytd_return` — price return from first trading day of the current calendar year

# COMMAND ----------

# DBTITLE 1,gold_app_top_holdings
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_top_holdings AS
# MAGIC WITH adv_holdings AS (
# MAGIC   SELECT
# MAGIC     c.advisor_id,
# MAGIC     h.ticker,
# MAGIC     ANY_VALUE(h.asset_class)                                                        AS asset_class,
# MAGIC     SUM(h.market_value)                                                             AS total_mv,
# MAGIC     ROUND(SUM(h.market_value) / SUM(SUM(h.market_value)) OVER (PARTITION BY c.advisor_id) * 100, 1) AS pct_of_portfolio
# MAGIC   FROM ahtsa.awm.holdings  h
# MAGIC   JOIN ahtsa.awm.accounts  a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients   c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC   GROUP BY c.advisor_id, h.ticker
# MAGIC ),
# MAGIC ytd_start AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC     AND date >= DATE_TRUNC('year', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC ytd_end AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC ytd_returns AS (
# MAGIC   SELECT s.symbol, ROUND((e.end_price / s.start_price - 1) * 100, 1) AS ytd_return
# MAGIC   FROM ytd_start s JOIN ytd_end e ON s.symbol = e.symbol
# MAGIC ),
# MAGIC signals AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     CASE
# MAGIC       WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.8 THEN 'alert'
# MAGIC       WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.5 THEN 'watch'
# MAGIC       ELSE 'none'
# MAGIC     END AS risk_flag
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE signal_date >= DATE_SUB(CURRENT_DATE, 30)
# MAGIC     AND symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC     AND source_type != 'news'
# MAGIC   GROUP BY symbol
# MAGIC )
# MAGIC SELECT
# MAGIC   t.advisor_id,
# MAGIC   t.ticker                            AS holding_id,
# MAGIC   COALESCE(cp.companyName, t.ticker)  AS name,
# MAGIC   t.asset_class,
# MAGIC   ROUND(t.total_mv / 1e6, 1)         AS aum_millions,
# MAGIC   t.pct_of_portfolio,
# MAGIC   COALESCE(yr.ytd_return, 0.0)        AS ytd_return,
# MAGIC   COALESCE(s.risk_flag, 'none')       AS risk_flag
# MAGIC FROM adv_holdings t
# MAGIC LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON t.ticker = cp.symbol
# MAGIC LEFT JOIN ytd_returns yr                        ON t.ticker = yr.symbol
# MAGIC LEFT JOIN signals s                             ON t.ticker = s.symbol
# MAGIC ORDER BY t.advisor_id, t.total_mv DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. `gold_app_concentration_risk`
# MAGIC **Schema:** `advisor_id`, `asset_class`, `client_name`, `delta_pct`
# MAGIC
# MAGIC IPS drift (actual − target) for the **top 5 clients by AUM per advisor**.
# MAGIC Positive `delta_pct` = overweight vs IPS target; negative = underweight.
# MAGIC Averaged across all accounts per client using `gold_ips_drift.drift_from_target_pct`.

# COMMAND ----------

# DBTITLE 1,gold_app_concentration_risk
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_concentration_risk AS
# MAGIC WITH ranked_clients AS (
# MAGIC   SELECT
# MAGIC     client_id,
# MAGIC     advisor_id,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY total_aum DESC) AS rn
# MAGIC   FROM ahtsa.awm.clients
# MAGIC ),
# MAGIC top_clients AS (
# MAGIC   SELECT client_id, advisor_id FROM ranked_clients WHERE rn <= 5
# MAGIC )
# MAGIC SELECT
# MAGIC   tc.advisor_id,
# MAGIC   d.asset_class,
# MAGIC   d.client_name,
# MAGIC   ROUND(AVG(d.drift_from_target_pct), 1) AS delta_pct
# MAGIC FROM ahtsa.awm.gold_ips_drift d
# MAGIC JOIN top_clients tc ON d.client_id = tc.client_id
# MAGIC GROUP BY tc.advisor_id, d.asset_class, d.client_name
# MAGIC ORDER BY tc.advisor_id, d.asset_class, delta_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. `gold_app_holdings_list`
# MAGIC One row per advisor × ticker. `has_alert` = true when the ticker has any signal with
# MAGIC `advisor_action_needed = true` AND `sentiment = 'Negative'` in the last two quarters (~6 months).

# COMMAND ----------

# DBTITLE 1,gold_app_holdings_list
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_holdings_list AS
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(date) AS max_date FROM ahtsa.awm.holdings
# MAGIC ),
# MAGIC advisor_holdings AS (
# MAGIC   -- Distinct ticker per advisor from the most recent holdings snapshot, excluding cash
# MAGIC   SELECT DISTINCT
# MAGIC     c.advisor_id,
# MAGIC     h.ticker,
# MAGIC     h.asset_class
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts  a  ON a.account_id = h.account_id
# MAGIC   JOIN ahtsa.awm.clients   c  ON c.client_id  = a.client_id
# MAGIC   JOIN latest_date         ld ON h.date        = ld.max_date
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC ),
# MAGIC alert_tickers AS (
# MAGIC   -- Tickers flagged: advisor_action_needed + Negative sentiment within last 2 quarters
# MAGIC   SELECT DISTINCT symbol
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE advisor_action_needed = true
# MAGIC     AND LOWER(sentiment) = 'negative'
# MAGIC     AND source_type != 'news'
# MAGIC     AND signal_date >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC )
# MAGIC SELECT
# MAGIC   ah.advisor_id,
# MAGIC   ah.ticker                                                        AS holding_id,
# MAGIC   COALESCE(p.companyName, ah.ticker)                              AS name,
# MAGIC   ah.asset_class,
# MAGIC   COALESCE(NULLIF(p.sector, ''), NULLIF(p.industry, ''), ah.asset_class) AS strategy,
# MAGIC   CASE WHEN al.symbol IS NOT NULL THEN true ELSE false END        AS has_alert
# MAGIC FROM advisor_holdings ah
# MAGIC LEFT JOIN ahtsa.awm.bronze_company_profiles p  ON p.symbol = ah.ticker
# MAGIC LEFT JOIN alert_tickers                     al ON al.symbol = ah.ticker
# MAGIC ORDER BY ah.advisor_id, has_alert DESC, ah.asset_class, ah.ticker

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. `gold_app_management_tone`
# MAGIC Synthetic management tone data for all advisor holdings.
# MAGIC Sections: Prepared Remarks, Q&A, Overall, Delta (NULL numerics — commentary only).
# MAGIC Re-run this cell to rebuild after data or sentiment updates.
# MAGIC
# MAGIC Reads the latest quarter per symbol from `gold_unified_signals` and reshapes it to match
# MAGIC the `gold_app_management_tone` schema (3 sections: Prepared Remarks, Q&A, Overall).
# MAGIC Delta (section_order=4) is excluded until QoQ string-parsing logic is added.
# MAGIC
# MAGIC `signal_value` layout: `[neg_frac, neu_frac, pos_frac]` — multiplied × 100 and cast to INT.

# COMMAND ----------

# DBTITLE 1,gold_app_management_tone
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_management_tone AS
# MAGIC WITH latest_signals AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     signal_type,
# MAGIC     signal_date,
# MAGIC     source_description,
# MAGIC     signal_value,
# MAGIC     sentiment,
# MAGIC     rationale,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY symbol, signal_type ORDER BY signal_date DESC)       AS rn,
# MAGIC     LEAD(source_description, 1) OVER (PARTITION BY symbol, signal_type ORDER BY signal_date DESC) AS prior_source_description
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE signal_type LIKE '%Management Tone%'
# MAGIC ),
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     signal_type,
# MAGIC     signal_date,
# MAGIC     source_description,
# MAGIC     prior_source_description,
# MAGIC     from_json(signal_value, 'ARRAY<DOUBLE>') AS tone_array,
# MAGIC     sentiment,
# MAGIC     rationale
# MAGIC   FROM latest_signals
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   symbol                                                              AS holding_id,
# MAGIC   CASE signal_type
# MAGIC     WHEN 'Management Tone - prepared_remarks' THEN 'Prepared Remarks'
# MAGIC     WHEN 'Management Tone - qa'               THEN 'Q&A'
# MAGIC     WHEN 'Management Tone - Overall'          THEN 'Overall'
# MAGIC   END                                                                AS section,
# MAGIC   CASE signal_type
# MAGIC     WHEN 'Management Tone - prepared_remarks' THEN 1
# MAGIC     WHEN 'Management Tone - qa'               THEN 2
# MAGIC     WHEN 'Management Tone - Overall'          THEN 3
# MAGIC   END                                                                AS section_order,
# MAGIC   CAST(ROUND(tone_array[2] * 100) AS INT)                           AS positive_pct,
# MAGIC   CAST(ROUND(tone_array[1] * 100) AS INT)                           AS neutral_pct,
# MAGIC   CAST(ROUND(tone_array[0] * 100) AS INT)                           AS negative_pct,
# MAGIC   LOWER(sentiment)                                                   AS sentiment,
# MAGIC   rationale                                                          AS section_note,
# MAGIC   signal_date                                                        AS earnings_date,
# MAGIC   YEAR(signal_date)                                                  AS year,
# MAGIC   QUARTER(signal_date)                                               AS quarter,
# MAGIC   regexp_extract(source_description,       'Q[1-4] \\d{4}', 0)     AS quarter_label,
# MAGIC   regexp_extract(prior_source_description, 'Q[1-4] \\d{4}', 0)     AS prior_quarter_label,
# MAGIC   source_description
# MAGIC FROM parsed
# MAGIC ORDER BY holding_id, section_order

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lakeview Reference Queries
# MAGIC The following cells contain parameterized SELECT queries used directly as Lakeview
# MAGIC dataset queries. They do not CREATE any tables — paste them into Lakeview dataset
# MAGIC definitions as-is.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Holdings by Date Range
# MAGIC Dynamic Holdings Query — returns holdings as of end_date, reconstructed live from
# MAGIC the transactions ledger and priced against bronze_historical_prices.
# MAGIC
# MAGIC Parameters:
# MAGIC   `:date.min` — period start; drives period_pl and period_return_pct
# MAGIC   `:date.max` — positions and valuations are computed as of this date
# MAGIC   `:benchmark` — index symbol from bronze_indexes_and_vix (e.g. GSPC)

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Holdings by Date Range
# MAGIC %sql
# MAGIC WITH
# MAGIC
# MAGIC -- ── Parameters ──────────────────────────────────────────────────────────────
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC      :date.min AS start_dt,
# MAGIC      :date.max AS end_dt
# MAGIC
# MAGIC ),
# MAGIC
# MAGIC -- ── Nearest available trading day on or before each bound ────────────────────
# MAGIC -- Handles weekends / holidays so the query never returns NULL prices.
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN date <= (SELECT end_dt   FROM params) THEN date END) AS end_price_dt,
# MAGIC     MAX(CASE WHEN date <= (SELECT start_dt FROM params) THEN date END) AS start_price_dt
# MAGIC   FROM bronze_historical_prices
# MAGIC ),
# MAGIC
# MAGIC -- ── Equity positions as of end_date ─────────────────────────────────────────
# MAGIC -- Quantity and weighted-average cost basis derived from BUY and DRIP transactions.
# MAGIC equity_positions AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     ticker,
# MAGIC     SUM(quantity)     AS quantity,
# MAGIC     SUM(gross_amount) AS total_cost
# MAGIC   FROM transactions
# MAGIC   WHERE action IN ('BUY', 'DRIP')
# MAGIC     AND ticker != 'CASH'
# MAGIC     AND date <= (SELECT end_dt FROM params)
# MAGIC   GROUP BY account_id, ticker
# MAGIC ),
# MAGIC
# MAGIC -- ── Cash balance as of end_date ──────────────────────────────────────────────
# MAGIC -- initial deposit + dividends received + drip reinvestment (negative) + fees (negative)
# MAGIC cash_positions AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     GREATEST(0.0,
# MAGIC       SUM(CASE WHEN action = 'BUY'      AND ticker = 'CASH' THEN quantity   ELSE 0.0 END)
# MAGIC     + SUM(CASE WHEN action = 'DIVIDEND'                      THEN net_amount ELSE 0.0 END)
# MAGIC     + SUM(CASE WHEN action = 'DRIP'                          THEN net_amount ELSE 0.0 END)
# MAGIC     + SUM(CASE WHEN action = 'FEE'                           THEN net_amount ELSE 0.0 END)
# MAGIC     ) AS cash_balance
# MAGIC   FROM transactions
# MAGIC   WHERE date <= (SELECT end_dt FROM params)
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC
# MAGIC -- ── End-of-period prices ─────────────────────────────────────────────────────
# MAGIC end_prices AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date = (SELECT end_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Start-of-period prices (for period return pct) ───────────────────────────
# MAGIC start_prices AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Asset class reference ────────────────────────────────────────────────────
# MAGIC -- Asset class is not in transactions; carry it from the holdings snapshot.
# MAGIC asset_class_ref AS (
# MAGIC   SELECT DISTINCT account_id, ticker, asset_class
# MAGIC   FROM holdings
# MAGIC   WHERE ticker != 'CASH'
# MAGIC ),
# MAGIC
# MAGIC -- ── Positions that existed before the period start ──────────────────────────
# MAGIC -- Used to distinguish: pre-existing → period cost basis = start_price
# MAGIC --                      new (opened mid-period) → period cost basis = actual avg cost paid
# MAGIC pre_existing_positions AS (
# MAGIC   SELECT DISTINCT account_id, ticker
# MAGIC   FROM transactions
# MAGIC   WHERE action IN ('BUY', 'DRIP')
# MAGIC     AND ticker != 'CASH'
# MAGIC     AND date < (SELECT start_dt FROM params)
# MAGIC ),
# MAGIC
# MAGIC -- ── Benchmark return for the period ─────────────────────────────────────────
# MAGIC -- Uses MAX(CASE...) to find the nearest close on or before each boundary date,
# MAGIC -- keeping alignment with price_dates even if the index has sparse trading days.
# MAGIC -- benchmark_return is a raw decimal (e.g. 0.0823); format as % on the front end.
# MAGIC -- Alpha at any grain = SUM(contribution_to_*_return) - benchmark_return
# MAGIC benchmark AS (
# MAGIC   SELECT
# MAGIC     v.symbol                                                                          as benchmark_symbol,
# MAGIC     :benchmark                                                                        AS benchmark,
# MAGIC     MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END)             AS benchmark_start,
# MAGIC     MAX_BY(v.close, CASE WHEN v.date <= pd.end_price_dt   THEN v.date END)             AS benchmark_end,
# MAGIC     (MAX_BY(v.close, CASE WHEN v.date <= pd.end_price_dt   THEN v.date END)
# MAGIC      - MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END))
# MAGIC     / NULLIF(MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END), 0)
# MAGIC                                                                                          AS benchmark_return
# MAGIC   FROM bronze_indexes_and_vix v
# MAGIC   CROSS JOIN price_dates pd
# MAGIC   WHERE v.index = :benchmark
# MAGIC   GROUP BY ALL
# MAGIC ),
# MAGIC
# MAGIC -- ── Equity holding rows ──────────────────────────────────────────────────────
# MAGIC equity_rows AS (
# MAGIC   SELECT
# MAGIC     ep.account_id,
# MAGIC     ep.ticker,
# MAGIC     COALESCE(ac.asset_class, 'Equity')                               AS asset_class,
# MAGIC     ep.quantity,
# MAGIC     epr.end_price                                                     AS price,
# MAGIC     ep.quantity * epr.end_price                                       AS market_value,
# MAGIC     ep.total_cost / ep.quantity                                       AS cost_basis_per_share,
# MAGIC     ep.total_cost                                                     AS total_cost_basis,
# MAGIC     ep.quantity * epr.end_price - ep.total_cost                       AS unrealized_gl,
# MAGIC     ROUND(
# MAGIC       (ep.quantity * epr.end_price - ep.total_cost)
# MAGIC       / NULLIF(ep.total_cost, 0) * 100, 2)                           AS unrealized_gl_pct,
# MAGIC     spr.start_price,
# MAGIC     ROUND(
# MAGIC       (epr.end_price - spr.start_price)
# MAGIC       / NULLIF(spr.start_price, 0) * 100, 2)                         AS period_return_pct,
# MAGIC     CASE
# MAGIC       WHEN pp.account_id IS NOT NULL THEN spr.start_price
# MAGIC       ELSE ep.total_cost / NULLIF(ep.quantity, 0)
# MAGIC     END                                                                AS period_cost_basis_per_share
# MAGIC   FROM equity_positions ep
# MAGIC   JOIN end_prices    epr ON ep.ticker = epr.symbol
# MAGIC   LEFT JOIN start_prices spr ON ep.ticker = spr.symbol
# MAGIC   LEFT JOIN asset_class_ref ac
# MAGIC     ON ep.account_id = ac.account_id AND ep.ticker = ac.ticker
# MAGIC   LEFT JOIN pre_existing_positions pp
# MAGIC     ON ep.account_id = pp.account_id AND ep.ticker = pp.ticker
# MAGIC ),
# MAGIC
# MAGIC -- ── Cash holding rows ────────────────────────────────────────────────────────
# MAGIC cash_rows AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     'CASH'       AS ticker,
# MAGIC     'Cash'       AS asset_class,
# MAGIC     cash_balance AS quantity,
# MAGIC     1.0          AS price,
# MAGIC     cash_balance AS market_value,
# MAGIC     1.0          AS cost_basis_per_share,
# MAGIC     cash_balance AS total_cost_basis,
# MAGIC     0.0          AS unrealized_gl,
# MAGIC     0.0          AS unrealized_gl_pct,
# MAGIC     1.0          AS start_price,
# MAGIC     0.0          AS period_return_pct,
# MAGIC     1.0          AS period_cost_basis_per_share
# MAGIC   FROM cash_positions
# MAGIC   WHERE cash_balance > 0
# MAGIC ),
# MAGIC
# MAGIC -- ── Advisory fees for the selected period ────────────────────────────────────
# MAGIC -- Account-level quarterly advisory fee (ticker = 'ADVISORY_FEE').
# MAGIC -- Prorated across positions by weight in position_level so SUM(fees_attributed)
# MAGIC -- is correct at any grain (account, client, asset class, sector).
# MAGIC period_fees AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     ABS(SUM(net_amount)) AS fees_paid
# MAGIC   FROM transactions
# MAGIC   WHERE action = 'FEE'
# MAGIC     AND date BETWEEN (SELECT start_dt FROM params) AND (SELECT end_dt FROM params)
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC
# MAGIC -- ── Union ────────────────────────────────────────────────────────────────────
# MAGIC all_holdings AS (
# MAGIC   SELECT * FROM equity_rows
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM cash_rows
# MAGIC ),
# MAGIC
# MAGIC -- ── Position-level metrics (pre sector expansion) ────────────────────────────
# MAGIC position_level AS (
# MAGIC SELECT
# MAGIC   -- Period context
# MAGIC   (SELECT start_price_dt FROM price_dates) AS period_start_price_date,
# MAGIC   (SELECT end_price_dt   FROM price_dates) AS as_of_date,
# MAGIC   bm.benchmark_symbol,
# MAGIC   ROUND(bm.benchmark_return,       6) AS benchmark_return,
# MAGIC   ROUND(bm.benchmark_return * 100, 4) AS benchmark_return_pct,
# MAGIC
# MAGIC   -- Client / account
# MAGIC   c.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   ah.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC
# MAGIC   -- Position
# MAGIC   ah.ticker,
# MAGIC   ah.asset_class,
# MAGIC   ROUND(ah.quantity,             4) AS quantity,
# MAGIC   ROUND(ah.price,                4) AS price,
# MAGIC   ROUND(ah.market_value,         2) AS market_value,
# MAGIC   ROUND(ah.cost_basis_per_share, 4) AS cost_basis_per_share,
# MAGIC   ROUND(ah.total_cost_basis,     2) AS total_cost_basis,
# MAGIC   ROUND(ah.unrealized_gl,        2) AS unrealized_gl,
# MAGIC   ah.unrealized_gl_pct,
# MAGIC   -- Period metrics — these change when start_date changes
# MAGIC   ROUND(ah.quantity * ah.start_price,                              2) AS market_value_at_period_start,
# MAGIC   ROUND(ah.period_cost_basis_per_share,                            4) AS period_cost_basis_per_share,
# MAGIC   ROUND(ah.quantity * ah.period_cost_basis_per_share,              2) AS period_total_cost_basis,
# MAGIC   ROUND(ah.market_value - ah.quantity * ah.period_cost_basis_per_share, 2) AS period_pl,
# MAGIC   ah.period_return_pct,
# MAGIC
# MAGIC   -- Portfolio weights (window functions — no GROUP BY needed)
# MAGIC   ROUND(
# MAGIC     ah.market_value
# MAGIC     / NULLIF(SUM(ah.market_value) OVER (PARTITION BY ah.account_id), 0) * 100, 2
# MAGIC   ) AS pct_of_account,
# MAGIC   ROUND(
# MAGIC     ah.market_value
# MAGIC     / NULLIF(SUM(ah.market_value) OVER (PARTITION BY c.client_id), 0) * 100, 2
# MAGIC   ) AS pct_of_client_portfolio,
# MAGIC   ROUND(
# MAGIC     ah.market_value
# MAGIC     / NULLIF(SUM(ah.market_value) OVER (), 0) * 100, 4
# MAGIC   ) AS pct_of_total_aum,
# MAGIC
# MAGIC   -- Return contributions — SUM at any grain on the front end to get total return %
# MAGIC   -- e.g. SUM(contribution_to_account_return) WHERE account_id = X  →  account period return
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price)
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (PARTITION BY ah.account_id), 0), 6
# MAGIC   ) AS contribution_to_account_return,
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price)
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (PARTITION BY c.client_id), 0), 6
# MAGIC   ) AS contribution_to_client_return,
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price)
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (), 0), 8
# MAGIC   ) AS contribution_to_aum_return,
# MAGIC
# MAGIC   -- Alpha contributions — SUM at any grain to get total alpha vs benchmark
# MAGIC   -- Formula: position return contribution minus position's benchmark drag
# MAGIC   -- (starting weight × benchmark return), so the sum stays additive.
# MAGIC   -- SUM(account_alpha_contribution)  →  account_total_return  - benchmark_return
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price * (1 + bm.benchmark_return))
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (PARTITION BY ah.account_id), 0), 6
# MAGIC   ) AS account_alpha_contribution,
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price * (1 + bm.benchmark_return))
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (PARTITION BY c.client_id), 0), 6
# MAGIC   ) AS client_alpha_contribution,
# MAGIC   ROUND(
# MAGIC     (ah.market_value - ah.quantity * ah.start_price * (1 + bm.benchmark_return))
# MAGIC     / NULLIF(SUM(ah.quantity * ah.start_price) OVER (), 0), 8
# MAGIC   ) AS aum_alpha_contribution,
# MAGIC
# MAGIC   -- ── Fee attribution (prorated by position weight within account) ──────────
# MAGIC   -- fees_attributed sums correctly at any grain — asset class, sector, client, etc.
# MAGIC   -- fees_attributed / market_value = effective fee rate for that slice
# MAGIC   ROUND(
# MAGIC     COALESCE(pf.fees_paid, 0)
# MAGIC     * ah.market_value
# MAGIC     / NULLIF(SUM(ah.market_value) OVER (PARTITION BY ah.account_id), 0), 2
# MAGIC   ) AS fees_attributed
# MAGIC
# MAGIC FROM all_holdings ah
# MAGIC JOIN accounts a      ON ah.account_id = a.account_id
# MAGIC JOIN clients  c      ON a.client_id   = c.client_id
# MAGIC CROSS JOIN benchmark bm
# MAGIC LEFT JOIN period_fees pf ON ah.account_id = pf.account_id
# MAGIC )
# MAGIC
# MAGIC -- ── Final output — sector look-through applied ───────────────────────────────
# MAGIC -- ETF positions fan out into N sector rows via bronze_etf_sectors.
# MAGIC -- All dollar metrics are multiplied by weight_in_source so SUM() remains correct
# MAGIC -- at any grain. Rate columns (period_return_pct, unrealized_gl_pct) are unchanged.
# MAGIC -- Sector weights reflect the latest bronze_etf_sectors fetch, not point-in-time.
# MAGIC SELECT
# MAGIC   -- ── Period context ────────────────────────────────────────────────────────
# MAGIC   pl.period_start_price_date,
# MAGIC   pl.as_of_date,
# MAGIC   pl.benchmark_symbol,
# MAGIC   pl.benchmark_return,
# MAGIC   pl.benchmark_return_pct,
# MAGIC
# MAGIC   -- ── Client / account ─────────────────────────────────────────────────────
# MAGIC   pl.client_id,
# MAGIC   pl.client_name,
# MAGIC   pl.advisor_id,
# MAGIC   pl.tier,
# MAGIC   pl.risk_profile,
# MAGIC   pl.account_id,
# MAGIC   pl.account_name,
# MAGIC   pl.account_type,
# MAGIC
# MAGIC   -- ── Position ─────────────────────────────────────────────────────────────
# MAGIC   pl.ticker,
# MAGIC   pl.asset_class,
# MAGIC
# MAGIC   -- ── Sector look-through ──────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN pl.ticker = 'CASH'         THEN 'Cash'
# MAGIC     WHEN es.etf_symbol IS NOT NULL  THEN COALESCE(es.sector, 'Unknown')
# MAGIC     ELSE COALESCE(cp.sector, 'Unknown')
# MAGIC   END                                                    AS sector,
# MAGIC   CASE
# MAGIC     WHEN es.etf_symbol IS NULL AND pl.ticker != 'CASH'
# MAGIC     THEN COALESCE(cp.industry, 'Unknown')
# MAGIC   END                                                    AS industry,
# MAGIC   CASE WHEN es.etf_symbol IS NOT NULL THEN 'ETF' ELSE 'Direct' END
# MAGIC                                                          AS source_type,
# MAGIC   COALESCE(es.weightPercentage / 100, 1.0)               AS weight_in_source,
# MAGIC
# MAGIC   -- ── Per-share / rate columns (not scaled — position-level rates) ─────────
# MAGIC   pl.price,
# MAGIC   pl.cost_basis_per_share,
# MAGIC   pl.period_cost_basis_per_share,
# MAGIC   pl.period_return_pct,
# MAGIC   pl.unrealized_gl_pct,
# MAGIC
# MAGIC   -- ── Dollar columns scaled by weight (additive at any grain) ─────────────
# MAGIC   ROUND(pl.quantity                      * COALESCE(es.weightPercentage/100, 1.0), 4)
# MAGIC                                                          AS quantity,
# MAGIC   ROUND(pl.market_value                  * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS market_value,
# MAGIC   ROUND(pl.total_cost_basis              * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS total_cost_basis,
# MAGIC   ROUND(pl.unrealized_gl                 * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS unrealized_gl,
# MAGIC   ROUND(pl.market_value_at_period_start  * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS market_value_at_period_start,
# MAGIC   ROUND(pl.period_total_cost_basis       * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS period_total_cost_basis,
# MAGIC   ROUND(pl.period_pl                     * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS period_pl,
# MAGIC
# MAGIC   -- ── Portfolio weight columns scaled by weight ─────────────────────────────
# MAGIC   -- SUM(pct_of_account) for a sector = that sector's share of the account
# MAGIC   ROUND(pl.pct_of_account                * COALESCE(es.weightPercentage/100, 1.0), 4)
# MAGIC                                                          AS pct_of_account,
# MAGIC   ROUND(pl.pct_of_client_portfolio       * COALESCE(es.weightPercentage/100, 1.0), 4)
# MAGIC                                                          AS pct_of_client_portfolio,
# MAGIC   ROUND(pl.pct_of_total_aum              * COALESCE(es.weightPercentage/100, 1.0), 6)
# MAGIC                                                          AS pct_of_total_aum,
# MAGIC
# MAGIC   -- ── Return / alpha contributions scaled by weight ─────────────────────────
# MAGIC   -- SUM(contribution_to_account_return) by sector = sector's contribution to account return
# MAGIC   ROUND(pl.contribution_to_account_return * COALESCE(es.weightPercentage/100, 1.0), 6)
# MAGIC                                                          AS contribution_to_account_return,
# MAGIC   ROUND(pl.contribution_to_client_return  * COALESCE(es.weightPercentage/100, 1.0), 6)
# MAGIC                                                          AS contribution_to_client_return,
# MAGIC   ROUND(pl.contribution_to_aum_return     * COALESCE(es.weightPercentage/100, 1.0), 8)
# MAGIC                                                          AS contribution_to_aum_return,
# MAGIC   ROUND(pl.account_alpha_contribution     * COALESCE(es.weightPercentage/100, 1.0), 6)
# MAGIC                                                          AS account_alpha_contribution,
# MAGIC   ROUND(pl.client_alpha_contribution      * COALESCE(es.weightPercentage/100, 1.0), 6)
# MAGIC                                                          AS client_alpha_contribution,
# MAGIC   ROUND(pl.aum_alpha_contribution         * COALESCE(es.weightPercentage/100, 1.0), 8)
# MAGIC                                                          AS aum_alpha_contribution,
# MAGIC
# MAGIC   -- ── Fees (prorated by position weight, then scaled by ETF sector weight) ──
# MAGIC   -- SUM(fees_attributed) at any grain = total fees for that slice
# MAGIC   -- SUM(fees_attributed) / SUM(market_value) = fees as % of AUM for that slice
# MAGIC   ROUND(pl.fees_attributed               * COALESCE(es.weightPercentage/100, 1.0), 2)
# MAGIC                                                          AS fees_attributed
# MAGIC
# MAGIC FROM position_level pl
# MAGIC LEFT JOIN bronze_etf_sectors     es ON pl.ticker = es.etf_symbol
# MAGIC LEFT JOIN bronze_company_profiles cp ON pl.ticker = cp.symbol
# MAGIC ORDER BY pl.client_name, pl.account_id, pl.asset_class, pl.ticker, sector

# COMMAND ----------

# MAGIC %md
# MAGIC ### Signal Feed and Signal Exposure
# MAGIC Unified Signal Queries for Lakeview.
# MAGIC
# MAGIC Query 1 — Signal Feed: one row per signal, with company metadata and numeric sentiment score.
# MAGIC Query 2 — Exposure: one row per (account × ticker × source_type × signal_type × advisor_action_needed).
# MAGIC
# MAGIC Lakeview parameters (Exposure query):
# MAGIC   `:date.min`              — signal window start
# MAGIC   `:date.max`              — signal window end
# MAGIC   `:source_type`           — limit rollup to these source types  (multi-select)
# MAGIC   `:signal_type`           — limit rollup to these signal types  (multi-select)
# MAGIC   `:advisor_action_needed` — limit rollup to action-needed signals only (true/false)

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
# MAGIC   c.advisor_id,
# MAGIC   c.client_id,
# MAGIC   c.client_name,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   h.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   h.ticker,
# MAGIC   cp.companyName,
# MAGIC   cp.sector,
# MAGIC   s.signal_count,
# MAGIC   s.net_sentiment_score,
# MAGIC   s.high_severity_count,
# MAGIC   s.advisor_action_count,
# MAGIC   CASE WHEN s.advisor_action_count > 0 THEN '⚠️ Action Needed' ELSE '🟩 No Action' END AS advisor_action_display,
# MAGIC   s.latest_signal_date
# MAGIC FROM holdings h
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC LEFT JOIN bronze_company_profiles cp ON h.ticker = cp.symbol
# MAGIC JOIN (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     COUNT(*)                                                           AS signal_count,
# MAGIC     ROUND(
# MAGIC       (SUM(CASE WHEN sentiment = 'Positive' THEN  1.0 ELSE 0 END)
# MAGIC        + SUM(CASE WHEN sentiment = 'Mixed'  THEN -0.5 ELSE 0 END)
# MAGIC        + SUM(CASE WHEN sentiment = 'Negative' THEN -1.0 ELSE 0 END))
# MAGIC       / NULLIF(COUNT(*), 0), 3)                                       AS net_sentiment_score,
# MAGIC     SUM(CASE WHEN signal_value = 'High' THEN 1 ELSE 0 END)           AS high_severity_count,
# MAGIC     SUM(CASE WHEN advisor_action_needed THEN 1 ELSE 0 END)            AS advisor_action_count,
# MAGIC     MAX(signal_date)                                                   AS latest_signal_date
# MAGIC   FROM gold_unified_signals
# MAGIC   WHERE
# MAGIC     (signal_date >= :date.min             OR :date.min             IS NULL)
# MAGIC     AND (signal_date <= :date.max         OR :date.max             IS NULL)
# MAGIC     AND (array_contains(:source_type, source_type) OR :source_type IS NULL)
# MAGIC     AND (array_contains(:signal_type, signal_type) OR :signal_type IS NULL)
# MAGIC     AND (:advisor_action_needed IS NULL OR advisor_action_needed = :advisor_action_needed)
# MAGIC   GROUP BY symbol
# MAGIC ) s ON h.ticker = s.symbol
# MAGIC ORDER BY s.advisor_action_count DESC, s.net_sentiment_score ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Portfolio Return Series
# MAGIC Returns a daily time series of portfolio return % vs a benchmark index,
# MAGIC both baselined to 0% at the period start date.
# MAGIC
# MAGIC Parameters:
# MAGIC   `:date.min`    — period start (date range widget)
# MAGIC   `:date.max`    — period end   (date range widget)
# MAGIC   `:benchmark`   — index symbol from bronze_indexes_and_vix (e.g. GSPC, DJI, IXIC)
# MAGIC   `:advisor_id`   — optional; leave blank to include all advisors
# MAGIC   `:account_type` — optional; leave blank to include all account types
# MAGIC   `:ticker`       — optional; leave blank to include all tickers

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Portfolio Return Series
# MAGIC %sql
# MAGIC WITH
# MAGIC
# MAGIC -- ── Parameters ──────────────────────────────────────────────────────────────
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC     :date.min AS start_dt,
# MAGIC     :date.max AS end_dt
# MAGIC ),
# MAGIC
# MAGIC -- ── Nearest available trading day on or before each bound ────────────────────
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN date <= (SELECT end_dt   FROM params) THEN date END) AS end_price_dt,
# MAGIC     MAX(CASE WHEN date <= (SELECT start_dt FROM params) THEN date END) AS start_price_dt
# MAGIC   FROM bronze_historical_prices
# MAGIC ),
# MAGIC
# MAGIC -- ── All trading days in the window ───────────────────────────────────────────
# MAGIC trading_days AS (
# MAGIC   SELECT DISTINCT date
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date >= (SELECT start_price_dt FROM price_dates)
# MAGIC     AND date <= (SELECT end_price_dt   FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Filtered positions as of end_date ────────────────────────────────────────
# MAGIC -- Carries total_cost so the baseline can use actual cost for new positions.
# MAGIC filtered_positions AS (
# MAGIC   SELECT
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     SUM(t.quantity)     AS quantity,
# MAGIC     SUM(t.gross_amount) AS total_cost
# MAGIC   FROM transactions t
# MAGIC   JOIN accounts a ON t.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE t.action IN ('BUY', 'DRIP')
# MAGIC     AND t.ticker != 'CASH'
# MAGIC     AND t.date   <= (SELECT end_dt FROM params)
# MAGIC     AND (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:ticker, t.ticker) OR :ticker IS NULL)
# MAGIC   GROUP BY t.account_id, t.ticker
# MAGIC ),
# MAGIC
# MAGIC -- ── Positions that existed before the period start ──────────────────────────
# MAGIC -- Mirrors the same logic in holdings_by_date_range so baselines are consistent.
# MAGIC pre_existing_positions AS (
# MAGIC   SELECT DISTINCT t.account_id, t.ticker
# MAGIC   FROM transactions t
# MAGIC   JOIN accounts a ON t.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE t.action IN ('BUY', 'DRIP')
# MAGIC     AND t.ticker != 'CASH'
# MAGIC     AND t.date   <  (SELECT start_dt FROM params)
# MAGIC     AND (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC ),
# MAGIC
# MAGIC -- ── Start-of-period prices ────────────────────────────────────────────────────
# MAGIC series_start_prices AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Daily portfolio value ─────────────────────────────────────────────────────
# MAGIC -- For each trading day, mark filtered positions to market.
# MAGIC daily_portfolio AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     SUM(fp.quantity * hp.adjClose) AS portfolio_value
# MAGIC   FROM trading_days td
# MAGIC   CROSS JOIN filtered_positions fp
# MAGIC   JOIN bronze_historical_prices hp
# MAGIC     ON hp.symbol = fp.ticker AND hp.date = td.date
# MAGIC   GROUP BY td.date
# MAGIC ),
# MAGIC
# MAGIC -- ── Portfolio baseline using period_cost_basis ────────────────────────────────
# MAGIC -- Pre-existing positions: quantity × start_price  (same as holdings query)
# MAGIC -- New positions:          actual total_cost paid   (same as holdings query)
# MAGIC -- This makes the series reconcile with SUM(period_pl)/SUM(period_total_cost_basis).
# MAGIC portfolio_baseline AS (
# MAGIC   SELECT SUM(
# MAGIC     CASE WHEN pp.account_id IS NOT NULL
# MAGIC          THEN fp.quantity * sp.start_price
# MAGIC          ELSE fp.total_cost
# MAGIC     END
# MAGIC   ) AS base
# MAGIC   FROM filtered_positions fp
# MAGIC   LEFT JOIN series_start_prices       sp ON fp.ticker     = sp.symbol
# MAGIC   LEFT JOIN pre_existing_positions    pp ON fp.account_id = pp.account_id
# MAGIC                                         AND fp.ticker     = pp.ticker
# MAGIC ),
# MAGIC
# MAGIC -- ── Daily benchmark series ────────────────────────────────────────────────────
# MAGIC -- Uses MAX_BY to pick the close on the exact trading day; LEFT JOIN so missing
# MAGIC -- index dates don't drop portfolio rows.
# MAGIC daily_benchmark AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     MAX_BY(v.close, v.date) AS benchmark_value
# MAGIC   FROM trading_days td
# MAGIC   LEFT JOIN bronze_indexes_and_vix v
# MAGIC     ON v.index = :benchmark AND v.date = td.date
# MAGIC   GROUP BY td.date
# MAGIC ),
# MAGIC
# MAGIC -- ── Benchmark value at period start (baseline = 0%) ──────────────────────────
# MAGIC benchmark_baseline AS (
# MAGIC   SELECT benchmark_value AS base
# MAGIC   FROM daily_benchmark
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Cumulative fees paid since period start ───────────────────────────────────
# MAGIC -- Applies the same advisor / account_type filters as filtered_positions so the
# MAGIC -- fee drag matches the portfolio slice being measured.
# MAGIC -- Fees accumulate as a step function — flat between quarters, then jump on fee dates.
# MAGIC fees_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     COALESCE(SUM(ABS(f.net_amount)), 0) AS cumulative_fees
# MAGIC   FROM trading_days td
# MAGIC   LEFT JOIN transactions f
# MAGIC     ON f.action = 'FEE'
# MAGIC     AND f.date >= (SELECT start_price_dt FROM price_dates)
# MAGIC     AND f.date <= td.date
# MAGIC     AND f.account_id IN (SELECT DISTINCT account_id FROM filtered_positions)
# MAGIC   GROUP BY td.date
# MAGIC )
# MAGIC
# MAGIC -- ── Final output ─────────────────────────────────────────────────────────────
# MAGIC SELECT
# MAGIC   dp.date,
# MAGIC   ROUND(dp.portfolio_value / NULLIF(pb.base, 0) - 1, 6)                          AS portfolio_return_before_fees,
# MAGIC   ROUND((dp.portfolio_value - fd.cumulative_fees) / NULLIF(pb.base, 0) - 1, 6)   AS portfolio_return_after_fees,
# MAGIC   ROUND(fd.cumulative_fees, 2)                                                    AS cumulative_fees,
# MAGIC   ROUND(db.benchmark_value / NULLIF(bb.base, 0) - 1, 6)                          AS benchmark_return,
# MAGIC   :benchmark                                                                      AS benchmark
# MAGIC FROM daily_portfolio dp
# MAGIC LEFT JOIN daily_benchmark  db ON dp.date = db.date
# MAGIC LEFT JOIN fees_by_day      fd ON dp.date = fd.date
# MAGIC CROSS JOIN portfolio_baseline pb
# MAGIC CROSS JOIN benchmark_baseline bb
# MAGIC ORDER BY dp.date

# COMMAND ----------

print("Portfolio gold tables complete:")
print(f"  silver_advisor_daily_returns      — daily returns vs GSPC, trailing 365 days")
print(f"  gold_app_portfolio_summary        — KPI stat cards per advisor")
print(f"  gold_app_asset_allocation         — asset class weights per advisor")
print(f"  gold_app_performance_timeseries   — daily cumulative return series")
print(f"  gold_app_top_holdings             — top holdings with risk flags")
print(f"  gold_app_concentration_risk       — IPS drift heatmap, top 5 clients")
print(f"  gold_app_holdings_list            — advisor x ticker list with alert flag")
print(f"  gold_app_management_tone          — latest management tone scores")
