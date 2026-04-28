# Databricks notebook source
# Dynamic Holdings Query — Dashboard Use
#
# Returns holdings as of the end_date, reconstructed live from the transactions ledger
# and priced against bronze_historical_prices. The holdings table is a static snapshot;
# this query is the authoritative, date-aware version for the front end.
#
# Parameters (Lakeview dashboard widgets):
#   start_date  — period start for return calculation (e.g. 2025-01-01)
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
#     (used only for period_return_pct — the price appreciation of each position
#      over the selected window, independent of cost basis)

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Test defaults — override via Lakeview date-range widget
# MAGIC -- In a Lakeview dashboard replace the literal dates with {{ start_date }} / {{ end_date }}
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC -- ── Parameters ──────────────────────────────────────────────────────────────
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC     DATE('2025-01-01') AS start_dt,   -- {{ start_date }}
# MAGIC     DATE('2025-12-31') AS end_dt       -- {{ end_date }}
# MAGIC ),
# MAGIC
# MAGIC -- ── Nearest available trading day on or before each bound ────────────────────
# MAGIC -- Handles weekends / holidays so the query never returns NULL prices.
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(CASE WHEN date <= (SELECT end_dt   FROM params) THEN date END) AS end_price_dt,
# MAGIC     MAX(CASE WHEN date <= (SELECT start_dt FROM params) THEN date END) AS start_price_dt
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
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
# MAGIC   FROM ${uc_catalog}.${uc_schema}.transactions
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
# MAGIC   FROM ${uc_catalog}.${uc_schema}.transactions
# MAGIC   WHERE date <= (SELECT end_dt FROM params)
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC
# MAGIC -- ── End-of-period prices ─────────────────────────────────────────────────────
# MAGIC end_prices AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC   WHERE date = (SELECT end_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Start-of-period prices (for period return pct) ───────────────────────────
# MAGIC start_prices AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC
# MAGIC -- ── Asset class reference ────────────────────────────────────────────────────
# MAGIC -- Asset class is not in transactions; carry it from the holdings snapshot.
# MAGIC asset_class_ref AS (
# MAGIC   SELECT DISTINCT account_id, ticker, asset_class
# MAGIC   FROM ${uc_catalog}.${uc_schema}.holdings
# MAGIC   WHERE ticker != 'CASH'
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
# MAGIC       / NULLIF(spr.start_price, 0) * 100, 2)                         AS period_return_pct
# MAGIC   FROM equity_positions ep
# MAGIC   JOIN end_prices    epr ON ep.ticker = epr.symbol
# MAGIC   LEFT JOIN start_prices spr ON ep.ticker = spr.symbol
# MAGIC   LEFT JOIN asset_class_ref ac
# MAGIC     ON ep.account_id = ac.account_id AND ep.ticker = ac.ticker
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
# MAGIC     0.0          AS period_return_pct
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
# MAGIC
# MAGIC   -- Client / account
# MAGIC   c.client_id,
# MAGIC   c.client_name,
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
# MAGIC   ) AS pct_of_total_aum
# MAGIC
# MAGIC FROM all_holdings ah
# MAGIC JOIN ${uc_catalog}.${uc_schema}.accounts a ON ah.account_id = a.account_id
# MAGIC JOIN ${uc_catalog}.${uc_schema}.clients  c ON a.client_id   = c.client_id
# MAGIC ORDER BY c.client_name, ah.account_id, ah.asset_class, ah.ticker
