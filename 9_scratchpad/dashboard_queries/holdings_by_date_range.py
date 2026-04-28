# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Dynamic Holdings Query — Dashboard Use
#
# Returns holdings as of the end_date, reconstructed live from the transactions ledger
# and priced against bronze_historical_prices. The holdings table is a static snapshot;
# this query is the authoritative, date-aware version for the front end.
#
# Parameters (Databricks widgets — set below; Lakeview uses {{ start_date }} / {{ end_date }}):
#   start_date  — period start; drives period_pl and period_return_pct
#   end_date    — positions and valuations are computed as of this date
#
# Position logic (mirrors 07_validate_and_rebuild_holdings.py):
#   Equity quantity   = SUM(quantity) for BUY + DRIP txns <= end_date
#   Equity cost basis = SUM(gross_amount) / SUM(quantity)  for same
#   Cash balance      = initial CASH BUY quantity + DIVIDEND net + DRIP net + FEE net
#
# Pricing:
#   end_price   = adjClose on the nearest trading day on or before end_date
#   start_price = adjClose on the nearest trading day on or before start_date
#
# Period metrics (the columns that change when start_date changes):
#   market_value_at_period_start = quantity × start_price  (what this position was worth then)
#   period_pl                    = market_value - market_value_at_period_start  ($ gain/loss)
#   period_return_pct            = (end_price - start_price) / start_price × 100
#
# Inception metrics (independent of start_date):
#   unrealized_gl / unrealized_gl_pct  — gain/loss since original cost basis

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ahtsa;
# MAGIC use schema awm;

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,FINAL QUERY
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
# MAGIC     :benchmark                                                        AS benchmark_symbol,
# MAGIC     MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END)             AS benchmark_start,
# MAGIC     MAX_BY(v.close, CASE WHEN v.date <= pd.end_price_dt   THEN v.date END)             AS benchmark_end,
# MAGIC     (MAX_BY(v.close, CASE WHEN v.date <= pd.end_price_dt   THEN v.date END)
# MAGIC      - MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END))
# MAGIC     / NULLIF(MAX_BY(v.close, CASE WHEN v.date <= pd.start_price_dt THEN v.date END), 0)
# MAGIC                                                                                          AS benchmark_return
# MAGIC   FROM bronze_indexes_and_vix v
# MAGIC   CROSS JOIN price_dates pd
# MAGIC   WHERE v.symbol = :benchmark
# MAGIC ),
# MAGIC
# MAGIC -- ── Management fees paid in the period ──────────────────────────────────────
# MAGIC -- One row per account; LEFT JOIN onto every holding row so the front end can
# MAGIC -- sum fees at any grain using MAX(fees_paid_in_period) per account (not SUM,
# MAGIC -- to avoid multiplying by the number of positions).
# MAGIC fees_in_period AS (
# MAGIC   SELECT
# MAGIC     account_id,
# MAGIC     ABS(SUM(net_amount)) AS fees_paid
# MAGIC   FROM transactions
# MAGIC   WHERE action = 'FEE'
# MAGIC     AND date >= (SELECT start_dt FROM params)
# MAGIC     AND date <= (SELECT end_dt   FROM params)
# MAGIC   GROUP BY account_id
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
# MAGIC -- ── Union ────────────────────────────────────────────────────────────────────
# MAGIC all_holdings AS (
# MAGIC   SELECT * FROM equity_rows
# MAGIC   UNION ALL
# MAGIC   SELECT * FROM cash_rows
# MAGIC )
# MAGIC
# MAGIC -- ── Final output — enriched with account and client metadata ─────────────────
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
# MAGIC   -- Fees paid in period — use MAX() when aggregating to avoid double-counting across positions
# MAGIC   ROUND(COALESCE(f.fees_paid, 0.0), 2) AS fees_paid_in_period
# MAGIC
# MAGIC FROM all_holdings ah
# MAGIC JOIN accounts a      ON ah.account_id = a.account_id
# MAGIC JOIN clients  c      ON a.client_id   = c.client_id
# MAGIC CROSS JOIN benchmark bm
# MAGIC LEFT JOIN fees_in_period f ON ah.account_id = f.account_id
# MAGIC ORDER BY c.client_name, ah.account_id, ah.asset_class, ah.ticker

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH 
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC     DATE('2026-02-27') AS start_dt,
# MAGIC     DATE('2026-03-31')   AS end_dt
# MAGIC ),
# MAGIC
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN date <= (SELECT end_dt   FROM params) THEN date END) AS end_price_dt,
# MAGIC     MAX(CASE WHEN date <= (SELECT start_dt FROM params) THEN date END) AS start_price_dt
# MAGIC   FROM bronze_historical_prices
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC    *
# MAGIC FROM bronze_indexes_and_vix v
# MAGIC   CROSS JOIN price_dates pd
# MAGIC   WHERE v.symbol = 'DJI'
# MAGIC   AND date >= start_price_dt
# MAGIC   AND date <= end_price_dt
