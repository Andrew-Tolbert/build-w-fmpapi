# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Financial Fundamentals — Dashboard & Genie Queries
#
# Creates three enriched tables and provides two sets of queries:
#
#   SECTION A — Silver / Gold Table Creation
#     silver_company_fundamentals  — one row per (symbol, fiscal period, date)
#                                    all structured financial metrics joined from bronze
#     gold_company_kpis            — latest fiscal period per symbol (current snapshot)
#     gold_portfolio_fundamentals  — current holdings joined to gold_company_kpis
#
#   SECTION B — Lakeview Dashboard Queries  (:param syntax)
#     1. Portfolio Fundamentals Snapshot   — key metrics per held position
#     2. Revenue & Earnings Trend          — quarterly time series for held tickers
#     3. Leverage & Credit Risk Trend      — net debt/EBITDA, coverage over time
#     4. Analyst Sentiment Snapshot        — consensus, price targets, upside %
#     5. Margin Trends                     — gross/EBITDA/net margin over time
#     6. Valuation vs Sector              — P/E, EV/EBITDA vs sector peers
#
#   SECTION C — Genie Context Queries  (static SQL — paste into Genie as context)
#     1. Most leveraged positions in the portfolio
#     2. Revenue growth leaders
#     3. Highest analyst upside by holding
#     4. Private credit / BDC risk metrics
#     5. Analyst consensus breakdown
#     6. Holdings trading above their price target
#
# Lakeview parameters (all optional; NULL = no filter / include all):
#   :date.min     — period start date
#   :date.max     — period end date
#   :advisor_id   — advisor ID      (multi-select)
#   :account_type — account type    (multi-select)
#   :client_id    — client ID       (multi-select)
#   :ticker       — ticker symbol   (multi-select)
#   :benchmark    — index symbol    (GSPC, DJI, IXIC)

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# Set catalog and schema dynamically from ingest_config widgets
spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------
# DBTITLE 1,─── SECTION A: SILVER / GOLD TABLE CREATION ────────────────────────────────────

# COMMAND ----------
# DBTITLE 1,silver_company_fundamentals — Dated Financial Data per Fiscal Period
# MAGIC %sql
# MAGIC -- One row per (symbol, period, date).
# MAGIC -- Joins all structured bronze sources on the fiscal period key (symbol, date, period).
# MAGIC -- Analyst ratings/targets are current snapshots (no period key); attached at symbol level.
# MAGIC -- Forward analyst estimates use the most recent estimate date per symbol.
# MAGIC CREATE OR REPLACE TABLE silver_company_fundamentals AS
# MAGIC WITH latest_analyst_est AS (
# MAGIC   SELECT *
# MAGIC   FROM (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
# MAGIC     FROM bronze_analyst_estimates
# MAGIC   )
# MAGIC   WHERE _rn = 1
# MAGIC ),
# MAGIC latest_price AS (
# MAGIC   SELECT symbol, adjClose AS current_price
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date = (SELECT MAX(date) FROM bronze_historical_prices)
# MAGIC )
# MAGIC SELECT
# MAGIC   -- ── Identity ──────────────────────────────────────────────────────────────
# MAGIC   i.symbol,
# MAGIC   i.date,
# MAGIC   i.fiscalYear,
# MAGIC   i.period,
# MAGIC
# MAGIC   -- ── Company Profile ───────────────────────────────────────────────────────
# MAGIC   cp.companyName                                AS company_name,
# MAGIC   cp.sector,
# MAGIC   cp.industry,
# MAGIC   cp.marketCap                                  AS market_cap,
# MAGIC   cp.beta,
# MAGIC   cp.isEtf                                      AS is_etf,
# MAGIC   cp.currency,
# MAGIC   cp.exchange,
# MAGIC
# MAGIC   -- ── Income Statement ──────────────────────────────────────────────────────
# MAGIC   i.revenue,
# MAGIC   i.costOfRevenue                               AS cost_of_revenue,
# MAGIC   i.grossProfit                                 AS gross_profit,
# MAGIC   i.ebitda,
# MAGIC   i.ebit,
# MAGIC   i.operatingIncome                             AS operating_income,
# MAGIC   i.netIncome                                   AS net_income,
# MAGIC   i.eps,
# MAGIC   i.epsDiluted                                  AS eps_diluted,
# MAGIC   i.interestExpense                             AS interest_expense,
# MAGIC   i.depreciationAndAmortization                 AS da,
# MAGIC   i.researchAndDevelopmentExpenses              AS rd_expense,
# MAGIC   i.operatingExpenses                           AS operating_expenses,
# MAGIC   i.weightedAverageShsOutDil                    AS shares_diluted,
# MAGIC
# MAGIC   -- ── Balance Sheet ─────────────────────────────────────────────────────────
# MAGIC   b.totalAssets                                 AS total_assets,
# MAGIC   b.totalLiabilities                            AS total_liabilities,
# MAGIC   b.totalStockholdersEquity                     AS total_equity,
# MAGIC   b.totalDebt                                   AS total_debt,
# MAGIC   b.netDebt                                     AS net_debt,
# MAGIC   b.cashAndCashEquivalents                      AS cash,
# MAGIC   b.longTermDebt                                AS long_term_debt,
# MAGIC   b.shortTermDebt                               AS short_term_debt,
# MAGIC   b.totalCurrentAssets                          AS current_assets,
# MAGIC   b.totalCurrentLiabilities                     AS current_liabilities,
# MAGIC   b.retainedEarnings                            AS retained_earnings,
# MAGIC   b.goodwill,
# MAGIC
# MAGIC   -- ── Cash Flow ─────────────────────────────────────────────────────────────
# MAGIC   cf.operatingCashFlow                          AS operating_cash_flow,
# MAGIC   cf.freeCashFlow                               AS free_cash_flow,
# MAGIC   cf.capitalExpenditure                         AS capex,
# MAGIC   cf.netDividendsPaid                           AS dividends_paid,
# MAGIC   cf.netCashProvidedByOperatingActivities       AS cfo,
# MAGIC
# MAGIC   -- ── Key Metrics (pre-calculated, period-matched) ──────────────────────────
# MAGIC   km.netDebtToEBITDA                            AS net_debt_to_ebitda,
# MAGIC   km.evToEBITDA                                 AS ev_to_ebitda,
# MAGIC   km.currentRatio                               AS current_ratio,
# MAGIC   km.returnOnEquity                             AS roe,
# MAGIC   km.returnOnAssets                             AS roa,
# MAGIC   km.returnOnInvestedCapital                    AS roic,
# MAGIC   km.enterpriseValue                            AS enterprise_value,
# MAGIC   km.earningsYield                              AS earnings_yield,
# MAGIC   km.freeCashFlowYield                          AS fcf_yield,
# MAGIC   km.workingCapital                             AS working_capital,
# MAGIC   km.investedCapital                            AS invested_capital,
# MAGIC   km.cashConversionCycle                        AS cash_conversion_cycle,
# MAGIC   km.capexToRevenue                             AS capex_to_revenue,
# MAGIC
# MAGIC   -- ── Financial Ratios (pre-calculated, period-matched) ─────────────────────
# MAGIC   fr.netProfitMargin                            AS net_profit_margin,
# MAGIC   fr.grossProfitMargin                          AS gross_profit_margin,
# MAGIC   fr.ebitdaMargin                               AS ebitda_margin,
# MAGIC   fr.operatingProfitMargin                      AS operating_margin,
# MAGIC   fr.debtToAssetsRatio                          AS debt_to_assets,
# MAGIC   fr.debtToEquityRatio                          AS debt_to_equity,
# MAGIC   fr.interestCoverageRatio                      AS interest_coverage,
# MAGIC   fr.debtServiceCoverageRatio                   AS dscr,
# MAGIC   fr.quickRatio                                 AS quick_ratio,
# MAGIC   fr.dividendYield                              AS dividend_yield,
# MAGIC   fr.priceToEarningsRatio                       AS pe_ratio,
# MAGIC   fr.priceToBookRatio                           AS pb_ratio,
# MAGIC   fr.priceToSalesRatio                          AS ps_ratio,
# MAGIC   fr.enterpriseValueMultiple                    AS ev_ebitda_ratio,
# MAGIC   fr.freeCashFlowPerShare                       AS fcf_per_share,
# MAGIC   fr.bookValuePerShare                          AS book_value_per_share,
# MAGIC   fr.dividendPerShare                           AS dividend_per_share,
# MAGIC   fr.revenuePerShare                            AS revenue_per_share,
# MAGIC
# MAGIC   -- ── Growth Rates YoY ──────────────────────────────────────────────────────
# MAGIC   ig.growthRevenue                              AS revenue_growth_yoy,
# MAGIC   ig.growthGrossProfit                          AS gross_profit_growth_yoy,
# MAGIC   ig.growthEBITDA                               AS ebitda_growth_yoy,
# MAGIC   ig.growthOperatingIncome                      AS operating_income_growth_yoy,
# MAGIC   ig.growthNetIncome                            AS net_income_growth_yoy,
# MAGIC   ig.growthEPS                                  AS eps_growth_yoy,
# MAGIC   ig.growthEPSDiluted                           AS eps_diluted_growth_yoy,
# MAGIC
# MAGIC   -- ── Forward Analyst Estimates (latest available date per symbol) ──────────
# MAGIC   ae.revenueAvg                                 AS est_revenue,
# MAGIC   ae.revenueHigh                                AS est_revenue_high,
# MAGIC   ae.revenueLow                                 AS est_revenue_low,
# MAGIC   ae.ebitdaAvg                                  AS est_ebitda,
# MAGIC   ae.netIncomeAvg                               AS est_net_income,
# MAGIC   ae.epsAvg                                     AS est_eps,
# MAGIC   ae.epsHigh                                    AS est_eps_high,
# MAGIC   ae.epsLow                                     AS est_eps_low,
# MAGIC   ae.numAnalystsRevenue                         AS num_analysts_revenue,
# MAGIC   ae.numAnalystsEps                             AS num_analysts_eps,
# MAGIC
# MAGIC   -- ── Analyst Ratings (current snapshot) ───────────────────────────────────
# MAGIC   ar.consensus                                  AS analyst_consensus,
# MAGIC   ar.strongBuy                                  AS ratings_strong_buy,
# MAGIC   ar.buy                                        AS ratings_buy,
# MAGIC   ar.hold                                       AS ratings_hold,
# MAGIC   ar.sell                                       AS ratings_sell,
# MAGIC   ar.strongSell                                 AS ratings_strong_sell,
# MAGIC
# MAGIC   -- ── Price Targets (current snapshot) ─────────────────────────────────────
# MAGIC   pt.targetConsensus                            AS price_target_consensus,
# MAGIC   pt.targetMedian                               AS price_target_median,
# MAGIC   pt.targetHigh                                 AS price_target_high,
# MAGIC   pt.targetLow                                  AS price_target_low,
# MAGIC
# MAGIC   -- ── Current Price ─────────────────────────────────────────────────────────
# MAGIC   lp.current_price,
# MAGIC
# MAGIC   -- ── Calculated KPIs ───────────────────────────────────────────────────────
# MAGIC   ROUND((pt.targetConsensus - lp.current_price) / NULLIF(lp.current_price, 0) * 100, 2)
# MAGIC                                                 AS analyst_upside_pct,
# MAGIC   ROUND(i.ebitda / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS ebitda_margin_calc,
# MAGIC   ROUND(i.grossProfit / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS gross_margin_calc,
# MAGIC   ROUND(i.netIncome / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS net_margin_calc,
# MAGIC   ROUND(b.totalDebt / NULLIF(i.ebitda, 0), 2)  AS total_debt_to_ebitda_calc,
# MAGIC   ROUND(i.ebit / NULLIF(i.interestExpense, 0), 2)
# MAGIC                                                 AS interest_coverage_calc,
# MAGIC   ROUND(cf.freeCashFlow / NULLIF(lp.current_price * i.weightedAverageShsOutDil, 0) * 100, 2)
# MAGIC                                                 AS fcf_yield_calc,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                           AS updated_at
# MAGIC
# MAGIC FROM bronze_income_statements i
# MAGIC LEFT JOIN bronze_company_profiles cp
# MAGIC   ON i.symbol = cp.symbol
# MAGIC LEFT JOIN bronze_balance_sheets b
# MAGIC   ON i.symbol = b.symbol AND i.date = b.date AND i.period = b.period
# MAGIC LEFT JOIN bronze_cash_flows cf
# MAGIC   ON i.symbol = cf.symbol AND i.date = cf.date AND i.period = cf.period
# MAGIC LEFT JOIN bronze_key_metrics km
# MAGIC   ON i.symbol = km.symbol AND i.date = km.date AND i.period = km.period
# MAGIC LEFT JOIN bronze_financial_ratios fr
# MAGIC   ON i.symbol = fr.symbol AND i.date = fr.date AND i.period = fr.period
# MAGIC LEFT JOIN bronze_income_growth ig
# MAGIC   ON i.symbol = ig.symbol AND i.date = ig.date AND i.period = ig.period
# MAGIC LEFT JOIN latest_analyst_est ae
# MAGIC   ON i.symbol = ae.symbol
# MAGIC LEFT JOIN bronze_analyst_ratings ar
# MAGIC   ON i.symbol = ar.symbol
# MAGIC LEFT JOIN bronze_price_targets pt
# MAGIC   ON i.symbol = pt.symbol
# MAGIC LEFT JOIN latest_price lp
# MAGIC   ON i.symbol = lp.symbol

# COMMAND ----------
# DBTITLE 1,gold_company_kpis — Current Snapshot (Latest Period per Symbol)
# MAGIC %sql
# MAGIC -- One row per symbol: latest fiscal period, with all KPIs from silver.
# MAGIC -- Use this for snapshot dashboards and Genie queries that ask about current state.
# MAGIC CREATE OR REPLACE TABLE gold_company_kpis AS
# MAGIC SELECT * EXCEPT (_rn)
# MAGIC FROM (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC, period DESC) AS _rn
# MAGIC   FROM silver_company_fundamentals
# MAGIC )
# MAGIC WHERE _rn = 1

# COMMAND ----------
# DBTITLE 1,gold_portfolio_fundamentals — Holdings + Latest Fundamentals + Client Context
# MAGIC %sql
# MAGIC -- One row per (account_id, ticker) for all non-cash held positions.
# MAGIC -- Joins the static holdings snapshot to gold_company_kpis and client/account metadata.
# MAGIC -- Use for snapshot dashboards filtered by advisor / account type / client.
# MAGIC CREATE OR REPLACE TABLE gold_portfolio_fundamentals AS
# MAGIC SELECT
# MAGIC   -- ── Portfolio context ─────────────────────────────────────────────────────
# MAGIC   h.account_id,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   a.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC
# MAGIC   -- ── Position ──────────────────────────────────────────────────────────────
# MAGIC   h.ticker,
# MAGIC   h.asset_class,
# MAGIC   h.quantity,
# MAGIC   h.price                                       AS current_price,
# MAGIC   h.market_value,
# MAGIC   h.cost_basis_per_share,
# MAGIC   h.total_cost_basis,
# MAGIC   h.unrealized_gl,
# MAGIC   ROUND(h.unrealized_gl / NULLIF(h.total_cost_basis, 0) * 100, 2)
# MAGIC                                                 AS unrealized_gl_pct,
# MAGIC   h.date                                        AS holdings_date,
# MAGIC
# MAGIC   -- ── Portfolio weight ──────────────────────────────────────────────────────
# MAGIC   ROUND(h.market_value / NULLIF(SUM(h.market_value) OVER (PARTITION BY h.account_id), 0) * 100, 2)
# MAGIC                                                 AS pct_of_account,
# MAGIC   ROUND(h.market_value / NULLIF(SUM(h.market_value) OVER (PARTITION BY a.client_id), 0) * 100, 2)
# MAGIC                                                 AS pct_of_client_portfolio,
# MAGIC   ROUND(h.market_value / NULLIF(SUM(h.market_value) OVER (), 0) * 100, 4)
# MAGIC                                                 AS pct_of_total_aum,
# MAGIC
# MAGIC   -- ── Company info ──────────────────────────────────────────────────────────
# MAGIC   f.company_name,
# MAGIC   f.sector,
# MAGIC   f.industry,
# MAGIC   f.market_cap,
# MAGIC   f.beta,
# MAGIC   f.is_etf,
# MAGIC   f.date                                        AS fundamentals_date,
# MAGIC   f.fiscalYear                                  AS fiscal_year,
# MAGIC   f.period                                      AS fiscal_period,
# MAGIC
# MAGIC   -- ── Income ────────────────────────────────────────────────────────────────
# MAGIC   f.revenue,
# MAGIC   f.ebitda,
# MAGIC   f.net_income,
# MAGIC   f.eps_diluted,
# MAGIC   f.operating_income,
# MAGIC   f.interest_expense,
# MAGIC
# MAGIC   -- ── Margins ───────────────────────────────────────────────────────────────
# MAGIC   f.gross_profit_margin,
# MAGIC   f.ebitda_margin,
# MAGIC   f.net_profit_margin,
# MAGIC   f.operating_margin,
# MAGIC
# MAGIC   -- ── Leverage ──────────────────────────────────────────────────────────────
# MAGIC   f.total_debt,
# MAGIC   f.net_debt,
# MAGIC   f.net_debt_to_ebitda,
# MAGIC   f.interest_coverage,
# MAGIC   f.dscr,
# MAGIC   f.debt_to_assets,
# MAGIC   f.debt_to_equity,
# MAGIC
# MAGIC   -- ── Valuation ─────────────────────────────────────────────────────────────
# MAGIC   f.pe_ratio,
# MAGIC   f.pb_ratio,
# MAGIC   f.ps_ratio,
# MAGIC   f.ev_to_ebitda,
# MAGIC   f.earnings_yield,
# MAGIC   f.dividend_yield,
# MAGIC   f.fcf_yield,
# MAGIC
# MAGIC   -- ── Returns ───────────────────────────────────────────────────────────────
# MAGIC   f.roe,
# MAGIC   f.roa,
# MAGIC   f.roic,
# MAGIC
# MAGIC   -- ── Cash Flow ─────────────────────────────────────────────────────────────
# MAGIC   f.free_cash_flow,
# MAGIC   f.operating_cash_flow,
# MAGIC
# MAGIC   -- ── Growth YoY ────────────────────────────────────────────────────────────
# MAGIC   f.revenue_growth_yoy,
# MAGIC   f.ebitda_growth_yoy,
# MAGIC   f.net_income_growth_yoy,
# MAGIC   f.eps_growth_yoy,
# MAGIC
# MAGIC   -- ── Analyst ───────────────────────────────────────────────────────────────
# MAGIC   f.analyst_consensus,
# MAGIC   f.ratings_strong_buy + f.ratings_buy          AS buy_ratings,
# MAGIC   f.ratings_hold,
# MAGIC   f.ratings_sell + f.ratings_strong_sell        AS sell_ratings,
# MAGIC   f.price_target_consensus,
# MAGIC   f.price_target_high,
# MAGIC   f.price_target_low,
# MAGIC   f.analyst_upside_pct,
# MAGIC   f.est_revenue,
# MAGIC   f.est_ebitda,
# MAGIC   f.est_eps,
# MAGIC   f.num_analysts_revenue,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                           AS updated_at
# MAGIC
# MAGIC FROM holdings h
# MAGIC JOIN accounts a   ON h.account_id = a.account_id
# MAGIC JOIN clients  c   ON a.client_id  = c.client_id
# MAGIC LEFT JOIN gold_company_kpis f ON h.ticker = f.symbol
# MAGIC WHERE h.ticker != 'CASH'

# COMMAND ----------
# DBTITLE 1,─── SECTION B: LAKEVIEW DASHBOARD QUERIES ─────────────────────────────────────
# NOTE: These queries use Lakeview named parameter syntax (:param_name).
# All filter parameters are optional — NULL or empty means "include all".
# Filter pattern: (array_contains(:param, column) OR :param IS NULL)
#
# To use in a Lakeview dashboard:
#   1. Copy the SQL from each %sql cell below into a dashboard dataset
#   2. Add widgets for advisor_id, account_type, client_id, ticker (multi-select)
#      and date.min / date.max (date range)
#   3. Wire widget values to the named parameters

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 1 — Portfolio Fundamentals Snapshot
# MAGIC %sql
# MAGIC -- Current fundamentals for each held position.
# MAGIC -- One row per (account_id, ticker); aggregate as needed on the front end.
# MAGIC -- Supports: advisor_id, account_type, client_id, ticker filters.
# MAGIC SELECT
# MAGIC   g.company_name,
# MAGIC   g.ticker,
# MAGIC   g.sector,
# MAGIC   g.industry,
# MAGIC   g.asset_class,
# MAGIC   g.client_id,
# MAGIC   g.client_name,
# MAGIC   g.advisor_id,
# MAGIC   g.tier,
# MAGIC   g.risk_profile,
# MAGIC   g.account_id,
# MAGIC   g.account_name,
# MAGIC   g.account_type,
# MAGIC
# MAGIC   -- Position
# MAGIC   ROUND(g.market_value,          2) AS market_value,
# MAGIC   ROUND(g.unrealized_gl,         2) AS unrealized_gl,
# MAGIC   g.unrealized_gl_pct,
# MAGIC   g.pct_of_account,
# MAGIC   g.pct_of_client_portfolio,
# MAGIC
# MAGIC   -- Fundamentals context
# MAGIC   g.fiscal_year,
# MAGIC   g.fiscal_period,
# MAGIC   g.fundamentals_date,
# MAGIC
# MAGIC   -- Valuation
# MAGIC   g.pe_ratio,
# MAGIC   g.pb_ratio,
# MAGIC   g.ev_to_ebitda,
# MAGIC   g.earnings_yield,
# MAGIC   g.dividend_yield,
# MAGIC   g.fcf_yield,
# MAGIC
# MAGIC   -- Leverage / Credit
# MAGIC   g.net_debt_to_ebitda,
# MAGIC   g.interest_coverage,
# MAGIC   g.dscr,
# MAGIC   g.debt_to_assets,
# MAGIC   CASE
# MAGIC     WHEN g.net_debt_to_ebitda > 5  THEN 'High'
# MAGIC     WHEN g.net_debt_to_ebitda > 3  THEN 'Elevated'
# MAGIC     WHEN g.net_debt_to_ebitda <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                           AS leverage_flag,
# MAGIC
# MAGIC   -- Profitability
# MAGIC   g.ebitda_margin,
# MAGIC   g.net_profit_margin,
# MAGIC   g.roe,
# MAGIC   g.roic,
# MAGIC   g.revenue_growth_yoy,
# MAGIC   g.ebitda_growth_yoy,
# MAGIC
# MAGIC   -- Revenue / Earnings (latest period)
# MAGIC   ROUND(g.revenue     / 1e6, 2)                AS revenue_m,
# MAGIC   ROUND(g.ebitda      / 1e6, 2)                AS ebitda_m,
# MAGIC   ROUND(g.net_income  / 1e6, 2)                AS net_income_m,
# MAGIC   g.eps_diluted,
# MAGIC
# MAGIC   -- Analyst
# MAGIC   g.analyst_consensus,
# MAGIC   g.analyst_upside_pct,
# MAGIC   g.price_target_consensus,
# MAGIC   g.current_price
# MAGIC
# MAGIC FROM gold_portfolio_fundamentals g
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   g.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, g.account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    g.client_id)    OR :client_id    IS NULL)
# MAGIC   AND (array_contains(:ticker,       g.ticker)       OR :ticker       IS NULL)
# MAGIC ORDER BY g.market_value DESC

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 2 — Revenue & Earnings Trend (Quarterly Time Series)
# MAGIC %sql
# MAGIC -- Quarterly revenue, EBITDA, and net income over time for tickers held in the
# MAGIC -- filtered portfolio. Use as a time series / bar chart in Lakeview.
# MAGIC -- Date range filters the fiscal period end date, not the holding date.
# MAGIC WITH
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC     AND (array_contains(:advisor_id,   c.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:client_id,    a.client_id)    OR :client_id    IS NULL)
# MAGIC     AND (array_contains(:ticker,       h.ticker)       OR :ticker       IS NULL)
# MAGIC )
# MAGIC SELECT
# MAGIC   f.symbol                                      AS ticker,
# MAGIC   f.company_name,
# MAGIC   f.sector,
# MAGIC   f.date,
# MAGIC   f.fiscalYear,
# MAGIC   f.period,
# MAGIC   ROUND(f.revenue       / 1e6, 2)               AS revenue_m,
# MAGIC   ROUND(f.gross_profit  / 1e6, 2)               AS gross_profit_m,
# MAGIC   ROUND(f.ebitda        / 1e6, 2)               AS ebitda_m,
# MAGIC   ROUND(f.net_income    / 1e6, 2)               AS net_income_m,
# MAGIC   ROUND(f.operating_income / 1e6, 2)            AS operating_income_m,
# MAGIC   f.eps_diluted,
# MAGIC   f.gross_profit_margin,
# MAGIC   f.ebitda_margin,
# MAGIC   f.net_profit_margin,
# MAGIC   f.revenue_growth_yoy,
# MAGIC   f.ebitda_growth_yoy,
# MAGIC   f.net_income_growth_yoy,
# MAGIC   f.eps_growth_yoy
# MAGIC FROM silver_company_fundamentals f
# MAGIC JOIN held_tickers ht ON f.symbol = ht.ticker
# MAGIC WHERE f.date >= :date.min
# MAGIC   AND f.date <= :date.max
# MAGIC ORDER BY f.symbol, f.date

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 3 — Leverage & Credit Risk Trend
# MAGIC %sql
# MAGIC -- Net debt/EBITDA, interest coverage, and DSCR over time for held tickers.
# MAGIC -- Critical for the private credit covenant monitoring narrative.
# MAGIC -- Threshold flags: ND/EBITDA > 5x = High, > 3x = Elevated; coverage < 2x = High risk.
# MAGIC WITH
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC     AND (array_contains(:advisor_id,   c.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:client_id,    a.client_id)    OR :client_id    IS NULL)
# MAGIC     AND (array_contains(:ticker,       h.ticker)       OR :ticker       IS NULL)
# MAGIC )
# MAGIC SELECT
# MAGIC   f.symbol                                      AS ticker,
# MAGIC   f.company_name,
# MAGIC   f.sector,
# MAGIC   f.date,
# MAGIC   f.fiscalYear,
# MAGIC   f.period,
# MAGIC
# MAGIC   -- Leverage
# MAGIC   ROUND(f.net_debt      / 1e6, 2)               AS net_debt_m,
# MAGIC   ROUND(f.total_debt    / 1e6, 2)               AS total_debt_m,
# MAGIC   ROUND(f.ebitda        / 1e6, 2)               AS ebitda_m,
# MAGIC   f.net_debt_to_ebitda,
# MAGIC   f.total_debt_to_ebitda_calc,
# MAGIC   f.debt_to_assets,
# MAGIC   f.debt_to_equity,
# MAGIC
# MAGIC   -- Coverage
# MAGIC   f.interest_coverage,
# MAGIC   f.dscr,
# MAGIC   ROUND(f.free_cash_flow / 1e6, 2)              AS free_cash_flow_m,
# MAGIC   ROUND(f.operating_cash_flow / 1e6, 2)         AS operating_cash_flow_m,
# MAGIC
# MAGIC   -- Risk flags
# MAGIC   CASE
# MAGIC     WHEN f.net_debt_to_ebitda > 5  THEN 'High'
# MAGIC     WHEN f.net_debt_to_ebitda > 3  THEN 'Elevated'
# MAGIC     WHEN f.net_debt_to_ebitda <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                           AS leverage_flag,
# MAGIC   CASE
# MAGIC     WHEN f.interest_coverage < 2 THEN 'High Risk'
# MAGIC     WHEN f.interest_coverage < 3 THEN 'Watch'
# MAGIC     ELSE 'Healthy'
# MAGIC   END                                           AS coverage_flag,
# MAGIC
# MAGIC   -- Forward estimate context
# MAGIC   ROUND(f.est_ebitda / 1e6, 2)                  AS est_ebitda_m,
# MAGIC   ROUND(f.net_debt / NULLIF(f.est_ebitda, 0), 2) AS forward_nd_ebitda
# MAGIC
# MAGIC FROM silver_company_fundamentals f
# MAGIC JOIN held_tickers ht ON f.symbol = ht.ticker
# MAGIC WHERE f.date >= :date.min
# MAGIC   AND f.date <= :date.max
# MAGIC ORDER BY f.symbol, f.date

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 4 — Analyst Sentiment Snapshot
# MAGIC %sql
# MAGIC -- Current analyst consensus, price targets, and upside potential for each position.
# MAGIC -- Supports advisor/account_type/client/ticker filters.
# MAGIC SELECT
# MAGIC   g.ticker,
# MAGIC   g.company_name,
# MAGIC   g.sector,
# MAGIC   g.asset_class,
# MAGIC   g.client_name,
# MAGIC   g.advisor_id,
# MAGIC   g.account_type,
# MAGIC   ROUND(g.market_value, 2)                      AS market_value,
# MAGIC   g.pct_of_account,
# MAGIC
# MAGIC   -- Current price vs targets
# MAGIC   ROUND(g.current_price,          2)            AS current_price,
# MAGIC   ROUND(g.price_target_consensus, 2)            AS target_consensus,
# MAGIC   ROUND(g.price_target_high,      2)            AS target_high,
# MAGIC   ROUND(g.price_target_low,       2)            AS target_low,
# MAGIC   g.analyst_upside_pct,
# MAGIC
# MAGIC   -- Consensus rating
# MAGIC   g.analyst_consensus,
# MAGIC   g.buy_ratings,
# MAGIC   g.ratings_hold,
# MAGIC   g.sell_ratings,
# MAGIC   g.buy_ratings + g.ratings_hold + g.sell_ratings AS total_analysts,
# MAGIC   ROUND(g.buy_ratings / NULLIF(g.buy_ratings + g.ratings_hold + g.sell_ratings, 0) * 100, 1)
# MAGIC                                                 AS buy_pct,
# MAGIC
# MAGIC   -- Forward estimates
# MAGIC   ROUND(g.est_revenue / 1e6, 2)                 AS est_revenue_m,
# MAGIC   ROUND(g.est_ebitda  / 1e6, 2)                 AS est_ebitda_m,
# MAGIC   g.est_eps,
# MAGIC   g.num_analysts_revenue,
# MAGIC
# MAGIC   -- Implied forward metrics (NTM)
# MAGIC   ROUND(g.current_price / NULLIF(g.est_eps, 0), 2) AS forward_pe
# MAGIC FROM gold_portfolio_fundamentals g
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   g.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, g.account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    g.client_id)    OR :client_id    IS NULL)
# MAGIC   AND (array_contains(:ticker,       g.ticker)       OR :ticker       IS NULL)
# MAGIC ORDER BY g.analyst_upside_pct DESC

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 5 — Margin Trends (Quarterly Time Series)
# MAGIC %sql
# MAGIC -- Gross, EBITDA, and net profit margins over time for held tickers.
# MAGIC -- Use as a time series chart to show operating efficiency trends.
# MAGIC WITH
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC     AND (array_contains(:advisor_id,   c.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:client_id,    a.client_id)    OR :client_id    IS NULL)
# MAGIC     AND (array_contains(:ticker,       h.ticker)       OR :ticker       IS NULL)
# MAGIC )
# MAGIC SELECT
# MAGIC   f.symbol                                      AS ticker,
# MAGIC   f.company_name,
# MAGIC   f.sector,
# MAGIC   f.date,
# MAGIC   f.fiscalYear,
# MAGIC   f.period,
# MAGIC   f.gross_profit_margin,
# MAGIC   f.ebitda_margin,
# MAGIC   f.net_profit_margin,
# MAGIC   f.operating_margin,
# MAGIC   f.gross_margin_calc,
# MAGIC   f.ebitda_margin_calc,
# MAGIC   f.net_margin_calc,
# MAGIC   -- R&D intensity (useful for tech/pharma holdings)
# MAGIC   ROUND(f.rd_expense / NULLIF(f.revenue, 0) * 100, 2) AS rd_as_pct_revenue,
# MAGIC   -- FCF conversion
# MAGIC   ROUND(f.free_cash_flow / NULLIF(f.net_income, 0) * 100, 2) AS fcf_conversion_pct,
# MAGIC   -- Return on capital
# MAGIC   f.roe,
# MAGIC   f.roa,
# MAGIC   f.roic
# MAGIC FROM silver_company_fundamentals f
# MAGIC JOIN held_tickers ht ON f.symbol = ht.ticker
# MAGIC WHERE f.date >= :date.min
# MAGIC   AND f.date <= :date.max
# MAGIC ORDER BY f.symbol, f.date

# COMMAND ----------
# DBTITLE 1,[LAKEVIEW] 6 — Valuation vs Sector Peers
# MAGIC %sql
# MAGIC -- Current valuation multiples for held tickers vs sector median.
# MAGIC -- Shows whether portfolio holdings trade at a premium or discount to sector.
# MAGIC WITH
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker, SUM(h.market_value) AS total_mv
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC     AND (array_contains(:advisor_id,   c.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC     AND (array_contains(:account_type, a.account_type) OR :account_type IS NULL)
# MAGIC     AND (array_contains(:client_id,    a.client_id)    OR :client_id    IS NULL)
# MAGIC     AND (array_contains(:ticker,       h.ticker)       OR :ticker       IS NULL)
# MAGIC   GROUP BY h.ticker
# MAGIC ),
# MAGIC sector_medians AS (
# MAGIC   SELECT
# MAGIC     sector,
# MAGIC     MEDIAN(CASE WHEN pe_ratio     BETWEEN 0  AND 100 THEN pe_ratio     END) AS median_pe,
# MAGIC     MEDIAN(CASE WHEN ev_to_ebitda BETWEEN 0  AND 50  THEN ev_to_ebitda END) AS median_ev_ebitda,
# MAGIC     MEDIAN(CASE WHEN pb_ratio     BETWEEN 0  AND 20  THEN pb_ratio     END) AS median_pb,
# MAGIC     MEDIAN(CASE WHEN dividend_yield > 0 THEN dividend_yield END)            AS median_div_yield
# MAGIC   FROM gold_company_kpis
# MAGIC   GROUP BY sector
# MAGIC )
# MAGIC SELECT
# MAGIC   k.symbol                                      AS ticker,
# MAGIC   k.company_name,
# MAGIC   k.sector,
# MAGIC   k.industry,
# MAGIC   k.fiscal_year,
# MAGIC   k.fiscal_period,
# MAGIC   ht.total_mv                                   AS portfolio_market_value,
# MAGIC   ROUND(k.current_price, 2)                     AS current_price,
# MAGIC   ROUND(k.market_cap / 1e9, 2)                  AS market_cap_b,
# MAGIC
# MAGIC   -- Valuation multiples
# MAGIC   k.pe_ratio,
# MAGIC   k.pb_ratio,
# MAGIC   k.ps_ratio,
# MAGIC   k.ev_to_ebitda,
# MAGIC   k.earnings_yield,
# MAGIC   k.dividend_yield,
# MAGIC   k.fcf_yield,
# MAGIC
# MAGIC   -- Sector benchmarks
# MAGIC   sm.median_pe,
# MAGIC   sm.median_ev_ebitda,
# MAGIC   sm.median_pb,
# MAGIC   sm.median_div_yield,
# MAGIC
# MAGIC   -- Premium/discount vs sector
# MAGIC   ROUND(k.pe_ratio     / NULLIF(sm.median_pe,       0) - 1, 4) AS pe_premium_vs_sector,
# MAGIC   ROUND(k.ev_to_ebitda / NULLIF(sm.median_ev_ebitda, 0) - 1, 4) AS ev_premium_vs_sector,
# MAGIC   ROUND(k.pb_ratio     / NULLIF(sm.median_pb,        0) - 1, 4) AS pb_premium_vs_sector,
# MAGIC
# MAGIC   -- Analyst target
# MAGIC   k.analyst_consensus,
# MAGIC   k.analyst_upside_pct
# MAGIC
# MAGIC FROM held_tickers ht
# MAGIC JOIN gold_company_kpis k ON ht.ticker = k.symbol
# MAGIC LEFT JOIN sector_medians sm ON k.sector = sm.sector
# MAGIC ORDER BY ht.total_mv DESC

# COMMAND ----------
# DBTITLE 1,─── SECTION C: GENIE CONTEXT QUERIES ─────────────────────────────────────────
# These are static SQL queries designed to be added as Genie context.
# They answer specific natural language questions a wealth advisor might ask.
# Paste each query block (the SQL inside the %sql cell) into the Genie space
# as a named example query with its suggested natural language question.

# COMMAND ----------
# DBTITLE 1,[GENIE] "What are the most leveraged positions in the portfolio?"
# MAGIC %sql
# MAGIC -- Ranks all held positions by net debt / EBITDA, highest first.
# MAGIC -- For BDC/private credit holdings, highlights those with the most covenant risk.
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   k.company_name,
# MAGIC   k.sector,
# MAGIC   h.asset_class,
# MAGIC   ROUND(SUM(h.market_value) / 1e6, 2)           AS portfolio_mv_m,
# MAGIC   ROUND(SUM(h.market_value) / NULLIF(SUM(SUM(h.market_value)) OVER (), 0) * 100, 3)
# MAGIC                                                 AS pct_of_total_aum,
# MAGIC   k.net_debt_to_ebitda,
# MAGIC   k.interest_coverage,
# MAGIC   k.dscr,
# MAGIC   k.debt_to_equity,
# MAGIC   ROUND(k.net_debt  / 1e6, 2)                   AS net_debt_m,
# MAGIC   ROUND(k.ebitda    / 1e6, 2)                   AS ebitda_m,
# MAGIC   k.analyst_consensus,
# MAGIC   CASE
# MAGIC     WHEN k.net_debt_to_ebitda > 5  THEN 'HIGH — covenant risk'
# MAGIC     WHEN k.net_debt_to_ebitda > 3  THEN 'ELEVATED — monitor'
# MAGIC     WHEN k.net_debt_to_ebitda <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                           AS leverage_flag
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC GROUP BY h.ticker, k.company_name, k.sector, h.asset_class,
# MAGIC          k.net_debt_to_ebitda, k.interest_coverage, k.dscr, k.debt_to_equity,
# MAGIC          k.net_debt, k.ebitda, k.analyst_consensus
# MAGIC ORDER BY k.net_debt_to_ebitda DESC NULLS LAST
# MAGIC LIMIT 25

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which holdings have the strongest revenue growth?"
# MAGIC %sql
# MAGIC -- Shows revenue growth YoY and 3-period trend for each held ticker.
# MAGIC -- Useful for identifying momentum names vs value plays in the portfolio.
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   k.company_name,
# MAGIC   k.sector,
# MAGIC   h.asset_class,
# MAGIC   ROUND(SUM(h.market_value) / 1e6, 2)           AS portfolio_mv_m,
# MAGIC   k.revenue_growth_yoy                          AS revenue_growth_yoy_latest,
# MAGIC   k.ebitda_growth_yoy                           AS ebitda_growth_yoy_latest,
# MAGIC   k.net_income_growth_yoy,
# MAGIC   k.eps_growth_yoy,
# MAGIC   ROUND(k.revenue    / 1e6, 2)                  AS revenue_latest_m,
# MAGIC   ROUND(k.ebitda     / 1e6, 2)                  AS ebitda_latest_m,
# MAGIC   k.ebitda_margin,
# MAGIC   k.net_profit_margin,
# MAGIC   k.analyst_consensus
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC GROUP BY h.ticker, k.company_name, k.sector, h.asset_class,
# MAGIC          k.revenue_growth_yoy, k.ebitda_growth_yoy, k.net_income_growth_yoy,
# MAGIC          k.eps_growth_yoy, k.revenue, k.ebitda, k.ebitda_margin,
# MAGIC          k.net_profit_margin, k.analyst_consensus
# MAGIC ORDER BY k.revenue_growth_yoy DESC NULLS LAST
# MAGIC LIMIT 25

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which positions have the most analyst upside?"
# MAGIC %sql
# MAGIC -- Ranks held positions by analyst price target upside (target consensus vs current price).
# MAGIC -- Also shows buy/hold/sell distribution and forward P/E.
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   k.company_name,
# MAGIC   k.sector,
# MAGIC   h.asset_class,
# MAGIC   ROUND(SUM(h.market_value) / 1e6, 2)           AS portfolio_mv_m,
# MAGIC   ROUND(k.current_price,           2)           AS current_price,
# MAGIC   ROUND(k.price_target_consensus,  2)           AS target_price,
# MAGIC   k.analyst_upside_pct,
# MAGIC   k.analyst_consensus,
# MAGIC   k.ratings_strong_buy + k.ratings_buy         AS buy_ratings,
# MAGIC   k.ratings_hold,
# MAGIC   k.ratings_sell + k.ratings_strong_sell       AS sell_ratings,
# MAGIC   k.pe_ratio,
# MAGIC   ROUND(k.current_price / NULLIF(k.est_eps, 0), 2) AS forward_pe,
# MAGIC   k.est_eps
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC GROUP BY h.ticker, k.company_name, k.sector, h.asset_class,
# MAGIC          k.current_price, k.price_target_consensus, k.analyst_upside_pct,
# MAGIC          k.analyst_consensus, k.ratings_strong_buy, k.ratings_buy,
# MAGIC          k.ratings_hold, k.ratings_sell, k.ratings_strong_sell,
# MAGIC          k.pe_ratio, k.est_eps
# MAGIC ORDER BY k.analyst_upside_pct DESC NULLS LAST

# COMMAND ----------
# DBTITLE 1,[GENIE] "Show me the risk metrics for private credit and BDC holdings"
# MAGIC %sql
# MAGIC -- Focused view on BDC / private credit positions (asset_class = 'Private Credit').
# MAGIC -- Key metrics: NAV proxy (book value), coverage ratios, leverage, dividend yield.
# MAGIC -- These are the holdings most relevant to the covenant breach scenario.
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   k.company_name,
# MAGIC   h.asset_class,
# MAGIC   ROUND(SUM(h.market_value)     / 1e6, 2)       AS portfolio_mv_m,
# MAGIC   ROUND(SUM(h.quantity), 0)                     AS shares_held,
# MAGIC   ROUND(k.current_price,              2)        AS current_price,
# MAGIC   ROUND(k.book_value_per_share,       2)        AS nav_per_share,
# MAGIC   ROUND(k.current_price / NULLIF(k.book_value_per_share, 0), 2) AS price_to_nav,
# MAGIC   k.dividend_yield,
# MAGIC   ROUND(k.dividend_per_share,         2)        AS annual_dividend,
# MAGIC   k.net_debt_to_ebitda,
# MAGIC   k.interest_coverage,
# MAGIC   k.dscr,
# MAGIC   k.debt_to_equity,
# MAGIC   k.debt_to_assets,
# MAGIC   ROUND(k.net_debt   / 1e6, 2)                  AS net_debt_m,
# MAGIC   ROUND(k.ebitda     / 1e6, 2)                  AS ebitda_m,
# MAGIC   ROUND(k.free_cash_flow / 1e6, 2)              AS fcf_m,
# MAGIC   k.analyst_consensus,
# MAGIC   k.analyst_upside_pct,
# MAGIC   CASE
# MAGIC     WHEN k.net_debt_to_ebitda > 5  THEN 'HIGH — covenant risk'
# MAGIC     WHEN k.net_debt_to_ebitda > 3  THEN 'ELEVATED — monitor'
# MAGIC     WHEN k.net_debt_to_ebitda <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                           AS leverage_flag
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.asset_class = 'Private Credit'
# MAGIC GROUP BY h.ticker, k.company_name, h.asset_class,
# MAGIC          k.current_price, k.book_value_per_share, k.dividend_yield,
# MAGIC          k.dividend_per_share, k.net_debt_to_ebitda, k.interest_coverage,
# MAGIC          k.dscr, k.debt_to_equity, k.debt_to_assets,
# MAGIC          k.net_debt, k.ebitda, k.free_cash_flow, k.analyst_consensus,
# MAGIC          k.analyst_upside_pct
# MAGIC ORDER BY k.net_debt_to_ebitda DESC NULLS LAST

# COMMAND ----------
# DBTITLE 1,[GENIE] "What is the analyst consensus breakdown across the portfolio?"
# MAGIC %sql
# MAGIC -- Summary of analyst consensus ratings across all held tickers.
# MAGIC -- Shows how much of the portfolio by market value has Buy / Hold / Sell ratings.
# MAGIC SELECT
# MAGIC   k.analyst_consensus,
# MAGIC   COUNT(DISTINCT h.ticker)                      AS num_tickers,
# MAGIC   ROUND(SUM(h.market_value) / 1e9, 3)           AS total_mv_b,
# MAGIC   ROUND(SUM(h.market_value) / NULLIF(SUM(SUM(h.market_value)) OVER (), 0) * 100, 2)
# MAGIC                                                 AS pct_of_aum,
# MAGIC   ROUND(AVG(k.analyst_upside_pct), 2)           AS avg_upside_pct,
# MAGIC   ROUND(AVG(k.pe_ratio), 2)                     AS avg_pe,
# MAGIC   ROUND(AVG(k.ebitda_margin), 4)                AS avg_ebitda_margin
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC   AND k.analyst_consensus IS NOT NULL
# MAGIC GROUP BY k.analyst_consensus
# MAGIC ORDER BY total_mv_b DESC

# COMMAND ----------
# DBTITLE 1,[GENIE] "Which holdings are trading above their analyst price target?"
# MAGIC %sql
# MAGIC -- Identifies positions where the current price exceeds the analyst consensus target.
# MAGIC -- These are potentially overvalued relative to analyst expectations.
# MAGIC SELECT
# MAGIC   h.ticker,
# MAGIC   k.company_name,
# MAGIC   k.sector,
# MAGIC   h.asset_class,
# MAGIC   ROUND(SUM(h.market_value) / 1e6, 2)           AS portfolio_mv_m,
# MAGIC   ROUND(k.current_price,           2)           AS current_price,
# MAGIC   ROUND(k.price_target_consensus,  2)           AS target_price,
# MAGIC   k.analyst_upside_pct,
# MAGIC   k.analyst_consensus,
# MAGIC   k.pe_ratio,
# MAGIC   k.ev_to_ebitda,
# MAGIC   k.net_profit_margin,
# MAGIC   k.revenue_growth_yoy
# MAGIC FROM holdings h
# MAGIC JOIN gold_company_kpis k ON h.ticker = k.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC   AND k.current_price > k.price_target_consensus
# MAGIC   AND k.price_target_consensus IS NOT NULL
# MAGIC GROUP BY h.ticker, k.company_name, k.sector, h.asset_class,
# MAGIC          k.current_price, k.price_target_consensus, k.analyst_upside_pct,
# MAGIC          k.analyst_consensus, k.pe_ratio, k.ev_to_ebitda,
# MAGIC          k.net_profit_margin, k.revenue_growth_yoy
# MAGIC ORDER BY k.analyst_upside_pct ASC
