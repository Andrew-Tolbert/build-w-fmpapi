# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Portfolio Return Series — Dashboard Use
#
# Returns a daily time series of portfolio return % vs a benchmark index,
# both baselined to 0% at the period start date.
#
# Parameters (Lakeview named params):
#   :date.min   — period start (date range widget)
#   :date.max   — period end   (date range widget)
#   :benchmark  — index symbol from bronze_indexes_and_vix (e.g. GSPC, DJI, IXIC)
#   :advisor_id   — optional; leave blank to include all advisors
#   :account_type — optional; leave blank to include all account types
#   :ticker       — optional; leave blank to include all tickers
#
# Any single filter, any combination, or none at all is valid.
# Empty string = no filter / include all.
#
# Output grain: one row per trading day in [start_date, end_date]
# Output columns:
#   date                          — trading day
#   portfolio_return_before_fees  — decimal; pure price return, no fee drag
#   portfolio_return_after_fees   — decimal; net of cumulative mgmt fees since start_date
#   cumulative_fees               — dollar fees paid from start_date through each day
#   benchmark_return              — same scale for the chosen index
#   benchmark                     — label for the front end
#
# Positions are held fixed as of end_date (buy-and-hold perspective).
# Cash is excluded from price return; fees are subtracted from the portfolio value
# in the after-fees series so the drag accumulates correctly through the period.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ahtsa;
# MAGIC use schema awm;

# COMMAND ----------

# DBTITLE 1,PORTFOLIO RETURN SERIES
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
