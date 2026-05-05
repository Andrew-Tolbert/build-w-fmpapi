# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Build all gold tables and views consumed by the Lakeview dashboards.
#
# Run order: after all bronze ingest and refinement signals are complete.
# Run this notebook to refresh dashboard data after any of the following change:
#   • holdings / transactions (synthetic rebuild or new data)
#   • bronze financials (monthly ingest)
#   • gold_unified_signals (daily/monthly refinement)
#
# Tables created:
#   gold_financial_fundamentals    — one row per (symbol, date, period); all financials joined
#   gold_financials_vs_estimates   — one row per (symbol, period); actuals vs consensus
#   gold_portfolio_sector_exposure — one row per (account, ticker, sector); ETF look-through
#
# Views created:
#   gold_ips_drift                 — one row per (account, asset_class); drift vs IPS targets
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.*

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_financial_fundamentals
# MAGIC One row per (symbol × fiscal_period × date). Joins every structured bronze source
# MAGIC into a single analyst-ready table covering all equities, ETFs, and BDCs.
# MAGIC
# MAGIC Use this table for:
# MAGIC   - Lakeview dashboards — filter by ticker / sector / date, aggregate on the front end
# MAGIC   - Genie queries    — natural language → SQL over a single wide table
# MAGIC   - Agent queries    — covenant monitoring, leverage scans, earnings trend extraction
# MAGIC
# MAGIC SECTION A — Table Creation
# MAGIC   gold_financial_fundamentals  — one row per (symbol, date, period)
# MAGIC
# MAGIC SECTION B — Lakeview Dataset Query  (:param syntax)
# MAGIC   Single parameterized SELECT for all dashboard visualizations.
# MAGIC   Lakeview handles aggregation — no pre-grouped views needed.
# MAGIC
# MAGIC SECTION C — Genie Context Queries  (static SQL — paste into Genie as examples)
# MAGIC   Analyst-style questions with tested SQL answers.

# COMMAND ----------

# DBTITLE 1,─── SECTION A: TABLE CREATION ──────────────────────────────────────────────────

# COMMAND ----------

# # Uncomment to drop and fully rebuild
# spark.sql("DROP TABLE IF EXISTS gold_financial_fundamentals")

# COMMAND ----------

# DBTITLE 1,gold_financial_fundamentals — One Row per (Symbol, Date, Period)
# MAGIC %sql
# MAGIC -- One row per (symbol, fiscal period, date).
# MAGIC -- Joins all structured bronze financial sources on (symbol, date, period).
# MAGIC -- Analyst ratings, price targets, and forward estimates are current snapshots
# MAGIC -- (no period key) and repeat on every period row for the same symbol.
# MAGIC CREATE OR REPLACE TABLE gold_financial_fundamentals AS
# MAGIC WITH latest_analyst_est AS (
# MAGIC   SELECT * EXCEPT (_rn)
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
# MAGIC   i.fiscalYear                                  AS fiscal_year,
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
# MAGIC   i.grossProfit                                 AS gross_profit,
# MAGIC   i.ebitda,
# MAGIC   i.ebit,
# MAGIC   i.operatingIncome                             AS operating_income,
# MAGIC   i.netIncome                                   AS net_income,
# MAGIC   i.eps,
# MAGIC   i.epsDiluted                                  AS eps_diluted,
# MAGIC   i.interestExpense                             AS interest_expense,
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
# MAGIC   b.retainedEarnings                            AS retained_earnings,
# MAGIC   b.goodwillAndIntangibleAssets                 AS goodwill_and_intangibles,
# MAGIC
# MAGIC   -- ── Cash Flow ─────────────────────────────────────────────────────────────
# MAGIC   cf.operatingCashFlow                          AS operating_cash_flow,
# MAGIC   cf.freeCashFlow                               AS free_cash_flow,
# MAGIC   cf.capitalExpenditure                         AS capex,
# MAGIC   cf.depreciationAndAmortization                AS da,
# MAGIC   cf.netDividendsPaid                           AS dividends_paid,
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
# MAGIC   km.capexToRevenue                             AS capex_to_revenue,
# MAGIC
# MAGIC   -- ── Financial Ratios (pre-calculated, period-matched) ─────────────────────
# MAGIC   fr.netProfitMargin                            AS net_profit_margin,
# MAGIC   fr.grossProfitMargin                          AS gross_profit_margin,
# MAGIC   fr.ebitdaMargin                               AS ebitda_margin,
# MAGIC   fr.operatingProfitMargin                      AS operating_margin,
# MAGIC   fr.debtToAssetsRatio                          AS debt_to_assets,
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
# MAGIC   -- ── Growth Rates YoY (from bronze_income_growth) ──────────────────────────
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
# MAGIC   -- ── Analyst Ratings (current snapshot — no period key) ────────────────────
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
# MAGIC   ROUND(
# MAGIC     (pt.targetConsensus - lp.current_price) / NULLIF(lp.current_price, 0) * 100,
# MAGIC     2)                                          AS analyst_upside_pct,
# MAGIC   ROUND(i.ebitda   / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS ebitda_margin_calc,
# MAGIC   ROUND(i.grossProfit / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS gross_margin_calc,
# MAGIC   ROUND(i.netIncome / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                 AS net_margin_calc,
# MAGIC   ROUND(b.totalDebt / NULLIF(i.ebitda, 0), 2)  AS total_debt_to_ebitda_calc,
# MAGIC   ROUND(i.ebit / NULLIF(i.interestExpense, 0), 2)
# MAGIC                                                 AS interest_coverage_calc,
# MAGIC   ROUND(b.netDebt / NULLIF(ae.ebitdaAvg, 0), 2)
# MAGIC                                                 AS forward_nd_ebitda,
# MAGIC
# MAGIC   -- ── Leverage flag ─────────────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN km.netDebtToEBITDA > 5  THEN 'High'
# MAGIC     WHEN km.netDebtToEBITDA > 3  THEN 'Elevated'
# MAGIC     WHEN km.netDebtToEBITDA <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                           AS leverage_flag,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                           AS updated_at
# MAGIC
# MAGIC FROM bronze_income_statements i
# MAGIC LEFT JOIN bronze_company_profiles  cp ON i.symbol = cp.symbol
# MAGIC LEFT JOIN bronze_balance_sheets    b  ON i.symbol = b.symbol
# MAGIC                                       AND i.date   = b.date
# MAGIC                                       AND i.period = b.period
# MAGIC LEFT JOIN bronze_cash_flows        cf ON i.symbol = cf.symbol
# MAGIC                                       AND i.date   = cf.date
# MAGIC                                       AND i.period = cf.period
# MAGIC LEFT JOIN bronze_key_metrics       km ON i.symbol = km.symbol
# MAGIC                                       AND i.date   = km.date
# MAGIC                                       AND i.period = km.period
# MAGIC LEFT JOIN bronze_financial_ratios  fr ON i.symbol = fr.symbol
# MAGIC                                       AND i.date   = fr.date
# MAGIC                                       AND i.period = fr.period
# MAGIC LEFT JOIN bronze_income_growth     ig ON i.symbol = ig.symbol
# MAGIC                                       AND i.date   = ig.date
# MAGIC                                       AND i.period = ig.period
# MAGIC LEFT JOIN latest_analyst_est       ae ON i.symbol = ae.symbol
# MAGIC LEFT JOIN bronze_analyst_ratings   ar ON i.symbol = ar.symbol
# MAGIC LEFT JOIN bronze_price_targets     pt ON i.symbol = pt.symbol
# MAGIC LEFT JOIN latest_price             lp ON i.symbol = lp.symbol

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count,
# MAGIC        COUNT(DISTINCT symbol) AS symbols,
# MAGIC        MIN(date) AS earliest_period,
# MAGIC        MAX(date) AS latest_period
# MAGIC FROM gold_financial_fundamentals

# COMMAND ----------

# DBTITLE 1,─── SECTION B: LAKEVIEW DATASET QUERY ──────────────────────────────────────────

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Financial Fundamentals — Time Series Dataset
# MAGIC %sql
# MAGIC -- Single dataset for all financial dashboard visualizations.
# MAGIC -- One row per (symbol, fiscal period, date). Lakeview aggregates on the front end:
# MAGIC --   • Trend chart:      filter by :ticker, plot any metric vs date
# MAGIC --   • Peer comparison:  filter by :sector, group by symbol, latest period only
# MAGIC --   • Leverage scan:    no filters, group by leverage_flag
# MAGIC --   • Margin analysis:  filter by :ticker or :sector, plot margin columns vs date
# MAGIC --
# MAGIC -- Dollar amounts scaled to $M. Raw values available in gold_financial_fundamentals.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   date,
# MAGIC   fiscal_year,
# MAGIC   period,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   industry,
# MAGIC   is_etf,
# MAGIC   leverage_flag,
# MAGIC
# MAGIC   -- Income ($M)
# MAGIC   ROUND(revenue          / 1e6, 2) AS revenue_m,
# MAGIC   ROUND(gross_profit     / 1e6, 2) AS gross_profit_m,
# MAGIC   ROUND(ebitda           / 1e6, 2) AS ebitda_m,
# MAGIC   ROUND(net_income       / 1e6, 2) AS net_income_m,
# MAGIC   ROUND(interest_expense / 1e6, 2) AS interest_expense_m,
# MAGIC   eps_diluted,
# MAGIC
# MAGIC   -- Margins (%)
# MAGIC   gross_profit_margin,
# MAGIC   ebitda_margin,
# MAGIC   net_profit_margin,
# MAGIC   operating_margin,
# MAGIC
# MAGIC   -- Leverage / Credit
# MAGIC   net_debt_to_ebitda,
# MAGIC   interest_coverage,
# MAGIC   dscr,
# MAGIC   debt_to_assets,
# MAGIC   ROUND(net_debt   / 1e6, 2) AS net_debt_m,
# MAGIC   ROUND(total_debt / 1e6, 2) AS total_debt_m,
# MAGIC   forward_nd_ebitda,
# MAGIC
# MAGIC   -- Valuation
# MAGIC   pe_ratio,
# MAGIC   pb_ratio,
# MAGIC   ps_ratio,
# MAGIC   ev_to_ebitda,
# MAGIC   earnings_yield,
# MAGIC   dividend_yield,
# MAGIC   fcf_yield,
# MAGIC   current_price,
# MAGIC   price_target_consensus,
# MAGIC   analyst_upside_pct,
# MAGIC   analyst_consensus,
# MAGIC
# MAGIC   -- Growth YoY (%)
# MAGIC   revenue_growth_yoy,
# MAGIC   ebitda_growth_yoy,
# MAGIC   net_income_growth_yoy,
# MAGIC   eps_growth_yoy,
# MAGIC   eps_diluted_growth_yoy,
# MAGIC
# MAGIC   -- Returns
# MAGIC   roe,
# MAGIC   roa,
# MAGIC   roic,
# MAGIC
# MAGIC   -- Cash Flow ($M)
# MAGIC   ROUND(free_cash_flow      / 1e6, 2) AS free_cash_flow_m,
# MAGIC   ROUND(operating_cash_flow / 1e6, 2) AS operating_cash_flow_m,
# MAGIC   ROUND(capex               / 1e6, 2) AS capex_m,
# MAGIC
# MAGIC   -- Balance Sheet ($M)
# MAGIC   ROUND(total_assets  / 1e6, 2) AS total_assets_m,
# MAGIC   ROUND(total_equity  / 1e6, 2) AS total_equity_m,
# MAGIC   ROUND(cash          / 1e6, 2) AS cash_m,
# MAGIC
# MAGIC   -- Market context
# MAGIC   ROUND(market_cap / 1e9, 2) AS market_cap_b,
# MAGIC   beta,
# MAGIC
# MAGIC   -- Analyst context
# MAGIC   ROUND(est_ebitda / 1e6, 2) AS est_ebitda_m,
# MAGIC   est_eps,
# MAGIC   num_analysts_revenue
# MAGIC
# MAGIC FROM gold_financial_fundamentals
# MAGIC WHERE
# MAGIC   (:ticker  IS NULL OR array_contains(:ticker,  symbol))
# MAGIC   AND (:sector  IS NULL OR array_contains(:sector,  sector))
# MAGIC   AND (date >= :date.min OR :date.min IS NULL)
# MAGIC   AND (date <= :date.max OR :date.max IS NULL)
# MAGIC ORDER BY symbol, date DESC

# COMMAND ----------

# DBTITLE 1,─── SECTION C: GENIE CONTEXT QUERIES ─────────────────────────────────────────
# Static SQL designed for Genie space context.
# Paste each block into the Genie space alongside the quoted question.

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show me the leverage trend for ARCC over the last 8 quarters"
# MAGIC %sql
# MAGIC -- Quarterly leverage metrics for a single ticker, most recent first.
# MAGIC -- Covenant concern threshold: net_debt_to_ebitda > 5x.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   date,
# MAGIC   fiscal_year,
# MAGIC   period,
# MAGIC   ROUND(net_debt         / 1e6, 2) AS net_debt_m,
# MAGIC   ROUND(ebitda           / 1e6, 2) AS ebitda_m,
# MAGIC   net_debt_to_ebitda,
# MAGIC   interest_coverage,
# MAGIC   dscr,
# MAGIC   leverage_flag,
# MAGIC   ROUND(forward_nd_ebitda, 2)      AS forward_nd_ebitda
# MAGIC FROM gold_financial_fundamentals
# MAGIC WHERE symbol = 'ARCC'
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which tickers have the highest leverage right now?"
# MAGIC %sql
# MAGIC -- Latest period per symbol, ranked by net debt / EBITDA. Excludes ETFs.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   date               AS latest_period,
# MAGIC   period,
# MAGIC   net_debt_to_ebitda,
# MAGIC   interest_coverage,
# MAGIC   leverage_flag,
# MAGIC   ROUND(net_debt  / 1e6, 2) AS net_debt_m,
# MAGIC   ROUND(ebitda    / 1e6, 2) AS ebitda_m,
# MAGIC   analyst_consensus
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
# MAGIC   FROM gold_financial_fundamentals
# MAGIC   WHERE is_etf = false OR is_etf IS NULL
# MAGIC )
# MAGIC WHERE _rn = 1
# MAGIC   AND net_debt_to_ebitda IS NOT NULL
# MAGIC ORDER BY net_debt_to_ebitda DESC
# MAGIC LIMIT 25

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show me revenue and EPS growth for AAPL over the last 2 years"
# MAGIC %sql
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   date,
# MAGIC   fiscal_year,
# MAGIC   period,
# MAGIC   ROUND(revenue    / 1e6, 2) AS revenue_m,
# MAGIC   ROUND(net_income / 1e6, 2) AS net_income_m,
# MAGIC   eps_diluted,
# MAGIC   revenue_growth_yoy,
# MAGIC   net_income_growth_yoy,
# MAGIC   eps_diluted_growth_yoy,
# MAGIC   ebitda_growth_yoy
# MAGIC FROM gold_financial_fundamentals
# MAGIC WHERE symbol = 'AAPL'
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,[GENIE] "Compare margins across Financial Services companies"
# MAGIC %sql
# MAGIC -- Latest period margins for all Financial Services tickers, sorted by market cap.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   date             AS latest_period,
# MAGIC   gross_profit_margin,
# MAGIC   ebitda_margin,
# MAGIC   net_profit_margin,
# MAGIC   operating_margin,
# MAGIC   roe,
# MAGIC   roic,
# MAGIC   ROUND(market_cap / 1e9, 2) AS market_cap_b
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
# MAGIC   FROM gold_financial_fundamentals
# MAGIC   WHERE sector = 'Financial Services'
# MAGIC     AND (is_etf = false OR is_etf IS NULL)
# MAGIC )
# MAGIC WHERE _rn = 1
# MAGIC ORDER BY market_cap_b DESC NULLS LAST
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which tickers have had 3+ consecutive quarters of sequential revenue growth?"
# MAGIC %sql
# MAGIC -- Identifies momentum: revenue grew quarter-over-quarter for 3 consecutive periods.
# MAGIC WITH quarterly AS (
# MAGIC   SELECT
# MAGIC     symbol, company_name, sector, date, period, revenue,
# MAGIC     LAG(revenue, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_q,
# MAGIC     LAG(revenue, 2) OVER (PARTITION BY symbol ORDER BY date) AS prev_2q,
# MAGIC     LAG(revenue, 3) OVER (PARTITION BY symbol ORDER BY date) AS prev_3q
# MAGIC   FROM gold_financial_fundamentals
# MAGIC   WHERE is_etf = false OR is_etf IS NULL
# MAGIC ),
# MAGIC latest AS (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
# MAGIC   FROM quarterly
# MAGIC )
# MAGIC SELECT
# MAGIC   symbol, company_name, sector,
# MAGIC   date                                         AS latest_period,
# MAGIC   ROUND(revenue   / 1e6, 2)                    AS revenue_m,
# MAGIC   ROUND(prev_q    / 1e6, 2)                    AS prev_q_m,
# MAGIC   ROUND((revenue - prev_3q) / NULLIF(prev_3q, 0) * 100, 1) AS growth_over_3q_pct
# MAGIC FROM latest
# MAGIC WHERE _rn = 1
# MAGIC   AND revenue > prev_q
# MAGIC   AND prev_q  > prev_2q
# MAGIC   AND prev_2q > prev_3q
# MAGIC ORDER BY growth_over_3q_pct DESC NULLS LAST

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show credit quality metrics for BDC and Financial Services holdings"
# MAGIC %sql
# MAGIC -- Credit-quality snapshot: coverage ratios, FCF, leverage, dividend yield.
# MAGIC -- Key for private credit / covenant monitoring.
# MAGIC SELECT
# MAGIC   f.symbol,
# MAGIC   f.company_name,
# MAGIC   f.date               AS latest_period,
# MAGIC   f.period,
# MAGIC   f.net_debt_to_ebitda,
# MAGIC   f.interest_coverage,
# MAGIC   f.dscr,
# MAGIC   f.fcf_yield,
# MAGIC   ROUND(f.free_cash_flow / 1e6, 2)  AS fcf_m,
# MAGIC   ROUND(f.net_debt       / 1e6, 2)  AS net_debt_m,
# MAGIC   f.dividend_yield,
# MAGIC   f.book_value_per_share,
# MAGIC   ROUND(f.current_price / NULLIF(f.book_value_per_share, 0), 2) AS price_to_book,
# MAGIC   f.leverage_flag,
# MAGIC   f.analyst_consensus
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
# MAGIC   FROM gold_financial_fundamentals
# MAGIC   WHERE sector = 'Financial Services'
# MAGIC     AND (is_etf = false OR is_etf IS NULL)
# MAGIC )  f
# MAGIC WHERE _rn = 1
# MAGIC ORDER BY f.net_debt_to_ebitda DESC NULLS LAST

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show the interest coverage trend for MAIN over 6 quarters"
# MAGIC %sql
# MAGIC -- Covenant early-warning: declining coverage is the key signal before breach.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   date,
# MAGIC   period,
# MAGIC   ROUND(ebitda           / 1e6, 2) AS ebitda_m,
# MAGIC   ROUND(interest_expense / 1e6, 2) AS interest_expense_m,
# MAGIC   interest_coverage,
# MAGIC   interest_coverage_calc,
# MAGIC   dscr,
# MAGIC   net_debt_to_ebitda,
# MAGIC   leverage_flag,
# MAGIC   ROUND(free_cash_flow   / 1e6, 2) AS fcf_m
# MAGIC FROM gold_financial_fundamentals
# MAGIC WHERE symbol = 'MAIN'
# MAGIC ORDER BY date DESC
# MAGIC LIMIT 6

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which companies have both declining margins AND rising leverage vs a year ago?"
# MAGIC %sql
# MAGIC -- Dual deterioration screen: simultaneous margin compression + leverage increase.
# MAGIC -- Compares latest period to 4 periods ago (approximate YoY).
# MAGIC WITH ranked AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
# MAGIC   FROM gold_financial_fundamentals
# MAGIC   WHERE is_etf = false OR is_etf IS NULL
# MAGIC ),
# MAGIC latest   AS (SELECT * FROM ranked WHERE rn = 1),
# MAGIC prior_yr AS (SELECT * FROM ranked WHERE rn = 5)
# MAGIC SELECT
# MAGIC   l.symbol,
# MAGIC   l.company_name,
# MAGIC   l.sector,
# MAGIC   l.date                                               AS latest_period,
# MAGIC   ROUND(l.ebitda_margin    * 100, 2)                   AS ebitda_margin_pct_now,
# MAGIC   ROUND(p.ebitda_margin    * 100, 2)                   AS ebitda_margin_pct_1yr_ago,
# MAGIC   ROUND((l.ebitda_margin - p.ebitda_margin) * 100, 2) AS margin_change_pp,
# MAGIC   l.net_debt_to_ebitda                                 AS nd_ebitda_now,
# MAGIC   p.net_debt_to_ebitda                                 AS nd_ebitda_1yr_ago,
# MAGIC   ROUND(l.net_debt_to_ebitda - p.net_debt_to_ebitda, 2) AS leverage_change,
# MAGIC   l.leverage_flag,
# MAGIC   l.analyst_consensus
# MAGIC FROM latest l
# MAGIC JOIN prior_yr p ON l.symbol = p.symbol
# MAGIC WHERE l.ebitda_margin < p.ebitda_margin
# MAGIC   AND l.net_debt_to_ebitda > p.net_debt_to_ebitda
# MAGIC   AND l.net_debt_to_ebitda IS NOT NULL
# MAGIC   AND p.net_debt_to_ebitda IS NOT NULL
# MAGIC ORDER BY leverage_change DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_financials_vs_estimates
# MAGIC One row per (symbol, reporting period). Period-matches actuals to analyst consensus
# MAGIC using a ±7-day window. Covers EPS, revenue, EBITDA, and net income beat/miss classification.
# MAGIC
# MAGIC Use this table for:
# MAGIC   - Beat/miss trend charts in Lakeview
# MAGIC   - Genie queries: "who beat estimates last quarter", "ARCC vs consensus"
# MAGIC   - Agent queries: identifying names consistently missing to flag earnings risk

# COMMAND ----------

# DBTITLE 1,─── SECTION D: FINANCIALS VS ESTIMATES ─────────────────────────────────────────

# COMMAND ----------

# DBTITLE 1,gold_financials_vs_estimates — One Row per (Symbol, Period) with Beat/Miss
# MAGIC %sql
# MAGIC -- One row per (symbol, reporting period).
# MAGIC -- Period-matches actual reported results to the analyst consensus estimate for that
# MAGIC -- same period using a ±7-day date window (period end dates sometimes differ by 1 day
# MAGIC -- across sources). Covers EPS, revenue, EBITDA, and net income.
# MAGIC --
# MAGIC -- Use this table for:
# MAGIC --   • Beat/miss trend charts in Lakeview
# MAGIC --   • Genie queries: "who beat estimates last quarter", "ARCC vs consensus"
# MAGIC --   • Agent queries: identifying names consistently missing to flag earnings risk
# MAGIC CREATE OR REPLACE TABLE gold_financials_vs_estimates AS
# MAGIC WITH period_matched AS (
# MAGIC   SELECT
# MAGIC     ae.symbol,
# MAGIC     i.date                                                AS period_end,
# MAGIC     i.fiscalYear                                          AS fiscal_year,
# MAGIC     i.period,
# MAGIC     -- Actuals
# MAGIC     i.revenue                                             AS actual_revenue,
# MAGIC     i.ebitda                                              AS actual_ebitda,
# MAGIC     i.netIncome                                           AS actual_net_income,
# MAGIC     i.epsDiluted                                          AS actual_eps,
# MAGIC     i.interestExpense                                     AS actual_interest_expense,
# MAGIC     -- Estimates (consensus at time of reporting)
# MAGIC     ae.revenueAvg                                         AS est_revenue,
# MAGIC     ae.revenueLow                                         AS est_revenue_low,
# MAGIC     ae.revenueHigh                                        AS est_revenue_high,
# MAGIC     ae.ebitdaAvg                                          AS est_ebitda,
# MAGIC     ae.netIncomeAvg                                       AS est_net_income,
# MAGIC     ae.epsAvg                                             AS est_eps,
# MAGIC     ae.epsLow                                             AS est_eps_low,
# MAGIC     ae.epsHigh                                            AS est_eps_high,
# MAGIC     ae.numAnalystsRevenue                                 AS num_analysts_revenue,
# MAGIC     ae.numAnalystsEps                                     AS num_analysts_eps,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY ae.symbol, i.date
# MAGIC       ORDER BY ABS(DATEDIFF(ae.date, i.date))
# MAGIC     )                                                     AS _rn
# MAGIC   FROM bronze_analyst_estimates ae
# MAGIC   JOIN bronze_income_statements i
# MAGIC     ON ae.symbol = i.symbol
# MAGIC     AND ABS(DATEDIFF(ae.date, i.date)) <= 7
# MAGIC )
# MAGIC SELECT
# MAGIC   pm.symbol,
# MAGIC   cp.companyName                                          AS company_name,
# MAGIC   cp.sector,
# MAGIC   cp.industry,
# MAGIC   cp.isEtf                                                AS is_etf,
# MAGIC   pm.period_end,
# MAGIC   pm.fiscal_year,
# MAGIC   pm.period,
# MAGIC
# MAGIC   -- ── Actuals ($M) ──────────────────────────────────────────────────────────
# MAGIC   ROUND(pm.actual_revenue       / 1e6, 2)                AS actual_revenue_m,
# MAGIC   ROUND(pm.actual_ebitda        / 1e6, 2)                AS actual_ebitda_m,
# MAGIC   ROUND(pm.actual_net_income    / 1e6, 2)                AS actual_net_income_m,
# MAGIC   pm.actual_eps,
# MAGIC   ROUND(pm.actual_interest_expense / 1e6, 2)             AS actual_interest_expense_m,
# MAGIC
# MAGIC   -- ── Estimates ($M) ────────────────────────────────────────────────────────
# MAGIC   ROUND(pm.est_revenue          / 1e6, 2)                AS est_revenue_m,
# MAGIC   ROUND(pm.est_revenue_low      / 1e6, 2)                AS est_revenue_low_m,
# MAGIC   ROUND(pm.est_revenue_high     / 1e6, 2)                AS est_revenue_high_m,
# MAGIC   ROUND(pm.est_ebitda           / 1e6, 2)                AS est_ebitda_m,
# MAGIC   ROUND(pm.est_net_income       / 1e6, 2)                AS est_net_income_m,
# MAGIC   pm.est_eps,
# MAGIC   pm.est_eps_low,
# MAGIC   pm.est_eps_high,
# MAGIC   pm.num_analysts_revenue,
# MAGIC   pm.num_analysts_eps,
# MAGIC
# MAGIC   -- ── Surprise (actual − estimate, signed) ──────────────────────────────────
# MAGIC   ROUND(
# MAGIC     (pm.actual_eps - pm.est_eps) / NULLIF(ABS(pm.est_eps), 0) * 100,
# MAGIC     2)                                                    AS eps_surprise_pct,
# MAGIC   ROUND(pm.actual_eps - pm.est_eps, 4)                   AS eps_surprise_abs,
# MAGIC   ROUND(
# MAGIC     (pm.actual_revenue - pm.est_revenue) / NULLIF(ABS(pm.est_revenue), 0) * 100,
# MAGIC     2)                                                    AS revenue_surprise_pct,
# MAGIC   ROUND((pm.actual_revenue - pm.est_revenue) / 1e6, 2)   AS revenue_surprise_m,
# MAGIC   ROUND(
# MAGIC     (pm.actual_ebitda - pm.est_ebitda) / NULLIF(ABS(pm.est_ebitda), 0) * 100,
# MAGIC     2)                                                    AS ebitda_surprise_pct,
# MAGIC   ROUND(
# MAGIC     (pm.actual_net_income - pm.est_net_income) / NULLIF(ABS(pm.est_net_income), 0) * 100,
# MAGIC     2)                                                    AS net_income_surprise_pct,
# MAGIC
# MAGIC   -- ── Beat / Miss flags ─────────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN pm.est_eps IS NULL               THEN NULL
# MAGIC     WHEN pm.actual_eps > pm.est_eps       THEN 'Beat'
# MAGIC     WHEN pm.actual_eps < pm.est_eps       THEN 'Miss'
# MAGIC     ELSE 'In-Line'
# MAGIC   END                                                     AS eps_beat_miss,
# MAGIC   CASE
# MAGIC     WHEN pm.est_revenue IS NULL                THEN NULL
# MAGIC     WHEN pm.actual_revenue > pm.est_revenue    THEN 'Beat'
# MAGIC     WHEN pm.actual_revenue < pm.est_revenue    THEN 'Miss'
# MAGIC     ELSE 'In-Line'
# MAGIC   END                                                     AS revenue_beat_miss,
# MAGIC   CASE
# MAGIC     WHEN pm.est_eps IS NULL OR pm.est_revenue IS NULL THEN NULL
# MAGIC     WHEN pm.actual_eps > pm.est_eps AND pm.actual_revenue > pm.est_revenue THEN 'Double Beat'
# MAGIC     WHEN pm.actual_eps < pm.est_eps AND pm.actual_revenue < pm.est_revenue THEN 'Double Miss'
# MAGIC     WHEN pm.actual_eps > pm.est_eps AND pm.actual_revenue < pm.est_revenue THEN 'EPS Beat / Rev Miss'
# MAGIC     WHEN pm.actual_eps < pm.est_eps AND pm.actual_revenue > pm.est_revenue THEN 'EPS Miss / Rev Beat'
# MAGIC     ELSE 'In-Line'
# MAGIC   END                                                     AS combined_beat_miss,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                                     AS updated_at
# MAGIC
# MAGIC FROM period_matched pm
# MAGIC LEFT JOIN bronze_company_profiles cp ON pm.symbol = cp.symbol
# MAGIC WHERE pm._rn = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count,
# MAGIC        COUNT(DISTINCT symbol) AS symbols,
# MAGIC        SUM(CASE WHEN eps_beat_miss = 'Beat' THEN 1 ELSE 0 END) AS beats,
# MAGIC        SUM(CASE WHEN eps_beat_miss = 'Miss' THEN 1 ELSE 0 END) AS misses,
# MAGIC        MIN(period_end) AS earliest_period,
# MAGIC        MAX(period_end) AS latest_period
# MAGIC FROM gold_financials_vs_estimates

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Financials vs Estimates — Beat/Miss Dataset
# MAGIC %sql
# MAGIC -- Single dataset for all beat/miss dashboard visualizations.
# MAGIC -- One row per (symbol, fiscal period). Lakeview filters/aggregates on the front end:
# MAGIC --   • Beat/miss bar: filter by :ticker or :sector, bar chart by period
# MAGIC --   • Surprise scatter: eps_surprise_pct vs revenue_surprise_pct per symbol
# MAGIC --   • Consistency table: group by symbol, count beats over N periods
# MAGIC --   • Sector heat: filter by :sector, color by combined_beat_miss
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   industry,
# MAGIC   is_etf,
# MAGIC   period_end,
# MAGIC   fiscal_year,
# MAGIC   period,
# MAGIC
# MAGIC   -- Actuals
# MAGIC   actual_revenue_m,
# MAGIC   actual_ebitda_m,
# MAGIC   actual_net_income_m,
# MAGIC   actual_eps,
# MAGIC
# MAGIC   -- Estimates
# MAGIC   est_revenue_m,
# MAGIC   est_revenue_low_m,
# MAGIC   est_revenue_high_m,
# MAGIC   est_ebitda_m,
# MAGIC   est_eps,
# MAGIC   est_eps_low,
# MAGIC   est_eps_high,
# MAGIC   num_analysts_eps,
# MAGIC
# MAGIC   -- Surprises
# MAGIC   eps_surprise_pct,
# MAGIC   eps_surprise_abs,
# MAGIC   revenue_surprise_pct,
# MAGIC   revenue_surprise_m,
# MAGIC   ebitda_surprise_pct,
# MAGIC
# MAGIC   -- Beat/Miss
# MAGIC   eps_beat_miss,
# MAGIC   revenue_beat_miss,
# MAGIC   combined_beat_miss
# MAGIC
# MAGIC FROM gold_financials_vs_estimates
# MAGIC WHERE
# MAGIC   (:ticker IS NULL OR array_contains(:ticker, symbol))
# MAGIC   AND (:sector IS NULL OR array_contains(:sector, sector))
# MAGIC   AND (period_end >= :date.min OR :date.min IS NULL)
# MAGIC   AND (period_end <= :date.max OR :date.max IS NULL)
# MAGIC   AND (is_etf = false OR is_etf IS NULL)
# MAGIC ORDER BY symbol, period_end DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which companies beat EPS estimates by the most last quarter?"
# MAGIC %sql
# MAGIC -- Latest period per symbol, ranked by EPS surprise. Positive = beat.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   period_end,
# MAGIC   period,
# MAGIC   actual_eps,
# MAGIC   est_eps,
# MAGIC   eps_surprise_pct,
# MAGIC   eps_surprise_abs,
# MAGIC   eps_beat_miss,
# MAGIC   revenue_surprise_pct,
# MAGIC   combined_beat_miss,
# MAGIC   num_analysts_eps
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_end DESC) AS _rn
# MAGIC   FROM gold_financials_vs_estimates
# MAGIC   WHERE is_etf = false OR is_etf IS NULL
# MAGIC )
# MAGIC WHERE _rn = 1
# MAGIC ORDER BY eps_surprise_pct DESC NULLS LAST
# MAGIC LIMIT 30

# COMMAND ----------

# DBTITLE 1,[GENIE] "Show the beat/miss history for ARCC over the last 8 quarters"
# MAGIC %sql
# MAGIC -- Quarter-by-quarter EPS and revenue surprises for a single BDC.
# MAGIC -- Negative surprise_pct = miss; positive = beat.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   period_end,
# MAGIC   period,
# MAGIC   actual_eps,
# MAGIC   est_eps,
# MAGIC   eps_surprise_pct,
# MAGIC   eps_beat_miss,
# MAGIC   actual_revenue_m,
# MAGIC   est_revenue_m,
# MAGIC   revenue_surprise_pct,
# MAGIC   revenue_beat_miss,
# MAGIC   combined_beat_miss
# MAGIC FROM gold_financials_vs_estimates
# MAGIC WHERE symbol = 'ARCC'
# MAGIC ORDER BY period_end DESC
# MAGIC LIMIT 8

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which Financial Services companies consistently beat EPS estimates?"
# MAGIC %sql
# MAGIC -- Consistency screen: beat rate and average surprise over last 8 quarters.
# MAGIC -- Identifies management teams that under-promise and over-deliver.
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   COUNT(*)                                                        AS quarters_with_estimates,
# MAGIC   SUM(CASE WHEN eps_beat_miss = 'Beat'    THEN 1 ELSE 0 END)     AS eps_beats,
# MAGIC   SUM(CASE WHEN eps_beat_miss = 'Miss'    THEN 1 ELSE 0 END)     AS eps_misses,
# MAGIC   SUM(CASE WHEN combined_beat_miss = 'Double Beat' THEN 1 ELSE 0 END) AS double_beats,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN eps_beat_miss = 'Beat' THEN 1 ELSE 0 END)
# MAGIC     / NULLIF(COUNT(*), 0) * 100, 1)                              AS beat_rate_pct,
# MAGIC   ROUND(AVG(eps_surprise_pct), 2)                                AS avg_eps_surprise_pct,
# MAGIC   ROUND(AVG(revenue_surprise_pct), 2)                            AS avg_rev_surprise_pct
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_end DESC) AS _rn
# MAGIC   FROM gold_financials_vs_estimates
# MAGIC   WHERE sector = 'Financial Services'
# MAGIC     AND (is_etf = false OR is_etf IS NULL)
# MAGIC )
# MAGIC WHERE _rn <= 8
# MAGIC GROUP BY symbol, company_name
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY beat_rate_pct DESC, avg_eps_surprise_pct DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which companies had a double beat last quarter?"
# MAGIC %sql
# MAGIC -- Latest period only, filtered to Double Beat (both EPS and revenue above consensus).
# MAGIC SELECT
# MAGIC   symbol,
# MAGIC   company_name,
# MAGIC   sector,
# MAGIC   period_end,
# MAGIC   period,
# MAGIC   actual_eps,
# MAGIC   est_eps,
# MAGIC   eps_surprise_pct,
# MAGIC   actual_revenue_m,
# MAGIC   est_revenue_m,
# MAGIC   revenue_surprise_pct,
# MAGIC   combined_beat_miss,
# MAGIC   num_analysts_eps
# MAGIC FROM (
# MAGIC   SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY period_end DESC) AS _rn
# MAGIC   FROM gold_financials_vs_estimates
# MAGIC   WHERE is_etf = false OR is_etf IS NULL
# MAGIC )
# MAGIC WHERE _rn = 1
# MAGIC   AND combined_beat_miss = 'Double Beat'
# MAGIC ORDER BY eps_surprise_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_portfolio_sector_exposure
# MAGIC One row per (account_id, source_ticker, sector). ETF positions fan out into N rows
# MAGIC (one per sector in bronze_etf_sectors). All financial metrics are scaled by
# MAGIC weight_in_source and remain additive at any grain.
# MAGIC
# MAGIC SECTION A — Gold Table Creation
# MAGIC   gold_portfolio_sector_exposure — look-through exposure per (account_id, sector)
# MAGIC                                    ETF positions are expanded into constituent sectors
# MAGIC
# MAGIC SECTION B — Lakeview Dashboard Queries  (:param syntax)
# MAGIC
# MAGIC SECTION C — Genie Context Queries  (paste into Genie as context)

# COMMAND ----------

# DBTITLE 1,─── SECTION A: GOLD TABLE CREATION ─────────────────────────────────────────────

# COMMAND ----------

# DBTITLE 1,gold_portfolio_sector_exposure — ETF Look-Through Sector View
# MAGIC %sql
# MAGIC -- One row per (account_id, source_ticker, sector).
# MAGIC -- An ETF holding produces N rows (one per sector in bronze_etf_sectors).
# MAGIC -- An equity/BDC holding produces 1 row (sector from bronze_company_profiles).
# MAGIC -- Cash produces 1 row (sector = 'Cash').
# MAGIC --
# MAGIC -- All financial metrics are multiplied by weight_in_source so they remain
# MAGIC -- additive at any grain. ETF weights sum to 1.0, so:
# MAGIC --   SUM(exposure_market_value)  = position market value
# MAGIC --   SUM(exposure_unrealized_gl) = position unrealized G/L
# MAGIC --   SUM(exposure_cost_basis)    = position cost basis
# MAGIC --   SUM(exposure_unrealized_gl) / SUM(exposure_cost_basis) = unrealized return %
# MAGIC CREATE OR REPLACE TABLE gold_portfolio_sector_exposure AS
# MAGIC
# MAGIC -- ── Direct equity and BDC positions ──────────────────────────────────────────
# MAGIC SELECT
# MAGIC   h.account_id,
# MAGIC   a.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   h.ticker                                             AS source_ticker,
# MAGIC   cp.companyName                                       AS source_name,
# MAGIC   h.asset_class                                        AS source_asset_class,
# MAGIC   'Direct'                                             AS source_type,
# MAGIC   h.ticker                                             AS constituent_ticker,
# MAGIC   cp.companyName                                       AS constituent_name,
# MAGIC   COALESCE(cp.sector,   'Unknown')                     AS sector,
# MAGIC   COALESCE(cp.industry, 'Unknown')                     AS industry,
# MAGIC   1.0                                                  AS weight_in_source,
# MAGIC   h.market_value                                       AS exposure_market_value,
# MAGIC   h.unrealized_gl                                      AS exposure_unrealized_gl,
# MAGIC   h.total_cost_basis                                   AS exposure_cost_basis,
# MAGIC   h.cost_basis_per_share,
# MAGIC   h.quantity,
# MAGIC   h.price                                              AS current_price
# MAGIC FROM holdings h
# MAGIC JOIN accounts a    ON h.account_id = a.account_id
# MAGIC JOIN clients  c    ON a.client_id  = c.client_id
# MAGIC JOIN bronze_company_profiles cp ON h.ticker = cp.symbol
# MAGIC WHERE h.ticker != 'CASH'
# MAGIC   AND NOT EXISTS (SELECT 1 FROM bronze_etf_info ei WHERE ei.symbol = h.ticker)
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- ── ETF positions — expanded by sector weights ────────────────────────────────
# MAGIC -- Financial metrics are scaled by weightPercentage/100 so they remain additive.
# MAGIC -- Sector weights are as-of latest data fetch (see note on staleness in header).
# MAGIC SELECT
# MAGIC   h.account_id,
# MAGIC   a.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   h.ticker                                                       AS source_ticker,
# MAGIC   ei.name                                                        AS source_name,
# MAGIC   ei.assetClass                                                  AS source_asset_class,
# MAGIC   'ETF'                                                          AS source_type,
# MAGIC   es.symbol                                                      AS constituent_ticker,
# MAGIC   NULL                                                           AS constituent_name,
# MAGIC   COALESCE(es.sector, 'Unknown')                                 AS sector,
# MAGIC   NULL                                                           AS industry,
# MAGIC   es.weightPercentage / 100                                      AS weight_in_source,
# MAGIC   ROUND(h.market_value    * es.weightPercentage / 100, 2)        AS exposure_market_value,
# MAGIC   ROUND(h.unrealized_gl   * es.weightPercentage / 100, 2)        AS exposure_unrealized_gl,
# MAGIC   ROUND(h.total_cost_basis * es.weightPercentage / 100, 2)       AS exposure_cost_basis,
# MAGIC   h.cost_basis_per_share,
# MAGIC   ROUND(h.quantity        * es.weightPercentage / 100, 6)        AS quantity,
# MAGIC   h.price                                                        AS current_price
# MAGIC FROM holdings h
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC JOIN bronze_etf_info    ei ON h.ticker = ei.symbol
# MAGIC JOIN bronze_etf_sectors es ON h.ticker = es.etf_symbol
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC -- ── Cash positions ────────────────────────────────────────────────────────────
# MAGIC SELECT
# MAGIC   h.account_id,
# MAGIC   a.client_id,
# MAGIC   c.client_name,
# MAGIC   c.advisor_id,
# MAGIC   c.tier,
# MAGIC   c.risk_profile,
# MAGIC   a.account_name,
# MAGIC   a.account_type,
# MAGIC   'CASH'                                               AS source_ticker,
# MAGIC   'Cash'                                               AS source_name,
# MAGIC   'Cash'                                               AS source_asset_class,
# MAGIC   'Direct'                                             AS source_type,
# MAGIC   'CASH'                                               AS constituent_ticker,
# MAGIC   'Cash'                                               AS constituent_name,
# MAGIC   'Cash'                                               AS sector,
# MAGIC   NULL                                                 AS industry,
# MAGIC   1.0                                                  AS weight_in_source,
# MAGIC   h.market_value                                       AS exposure_market_value,
# MAGIC   0.0                                                  AS exposure_unrealized_gl,
# MAGIC   h.market_value                                       AS exposure_cost_basis,
# MAGIC   1.0                                                  AS cost_basis_per_share,
# MAGIC   h.quantity,
# MAGIC   1.0                                                  AS current_price
# MAGIC FROM holdings h
# MAGIC JOIN accounts a ON h.account_id = a.account_id
# MAGIC JOIN clients  c ON a.client_id  = c.client_id
# MAGIC WHERE h.ticker = 'CASH'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS row_count, COUNT(DISTINCT account_id) AS accounts,
# MAGIC        COUNT(DISTINCT sector) AS sectors,
# MAGIC        ROUND(SUM(exposure_market_value) / 1e9, 2) AS total_exposure_b
# MAGIC FROM gold_portfolio_sector_exposure

# COMMAND ----------

# DBTITLE 1,─── SECTION B: LAKEVIEW DASHBOARD QUERY ──────────────────────────────────────────
# Single dataset — Lakeview handles all aggregations via chart/widget config.
# Sum exposure_market_value and group by any column (sector, source_type, client, etc.)
# to build bar charts, counters, tables, and line charts from one dataset.
#
# Lakeview parameters (all optional; NULL = include all):
#   :advisor_id   — advisor ID     (multi-select)
#   :account_type — account type   (multi-select)
#   :client_id    — client ID      (multi-select)
#   :sector       — sector         (multi-select)
#   :source_type  — 'ETF' or 'Direct'

# COMMAND ----------

# DBTITLE 1,[LAKEVIEW] Sector Exposure — Base Dataset
# MAGIC %sql
# MAGIC -- One row per (account_id, source_ticker, sector).
# MAGIC -- ETF positions are pre-expanded by sector weight in gold_portfolio_sector_exposure.
# MAGIC -- All financial metrics are pre-scaled by weight_in_source and remain additive:
# MAGIC --   SUM(exposure_market_value)  → total market value for that sector slice
# MAGIC --   SUM(exposure_unrealized_gl) → total unrealized G/L for that sector slice
# MAGIC --   SUM(exposure_cost_basis)    → total cost basis for that sector slice
# MAGIC --   SUM(exposure_unrealized_gl) / SUM(exposure_cost_basis) → unrealized return %
# MAGIC SELECT
# MAGIC   e.account_id,
# MAGIC   e.account_name,
# MAGIC   e.account_type,
# MAGIC   e.client_id,
# MAGIC   e.client_name,
# MAGIC   e.advisor_id,
# MAGIC   e.tier,
# MAGIC   e.risk_profile,
# MAGIC   e.source_ticker,
# MAGIC   e.source_name,
# MAGIC   e.source_asset_class,
# MAGIC   e.source_type,
# MAGIC   e.constituent_ticker,
# MAGIC   e.sector,
# MAGIC   e.industry,
# MAGIC   e.weight_in_source,
# MAGIC   e.exposure_market_value,
# MAGIC   e.exposure_unrealized_gl,
# MAGIC   e.exposure_cost_basis,
# MAGIC   e.current_price,
# MAGIC   e.cost_basis_per_share
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC WHERE
# MAGIC   (array_contains(:advisor_id,   e.advisor_id)   OR :advisor_id   IS NULL)
# MAGIC   AND (array_contains(:account_type, e.account_type) OR :account_type IS NULL)
# MAGIC   AND (array_contains(:client_id,    e.client_id)    OR :client_id    IS NULL)
# MAGIC   AND (array_contains(:sector,       e.sector)       OR :sector       IS NULL)
# MAGIC   AND (e.source_type = :source_type                  OR :source_type  IS NULL)

# COMMAND ----------

# DBTITLE 1,─── SECTION C: GENIE CONTEXT QUERIES ─────────────────────────────────────────
# Static SQL for Genie context. Paste each block into Genie alongside the suggested question.

# COMMAND ----------

# DBTITLE 1,[GENIE] "What is the portfolio's largest sector exposure?"
# MAGIC %sql
# MAGIC -- Book-wide sector exposures ranked by size, including ETF look-through.
# MAGIC -- Distinguishes direct holdings from ETF-derived exposure.
# MAGIC SELECT
# MAGIC   e.sector,
# MAGIC   ROUND(SUM(e.exposure_market_value) / 1e9, 4)           AS total_exposure_b,
# MAGIC   ROUND(SUM(e.exposure_market_value) / NULLIF(SUM(SUM(e.exposure_market_value)) OVER (), 0) * 100, 2)
# MAGIC                                                          AS pct_of_total,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'Direct' THEN e.exposure_market_value ELSE 0 END) / 1e9, 4)
# MAGIC                                                          AS direct_b,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'ETF'    THEN e.exposure_market_value ELSE 0 END) / 1e9, 4)
# MAGIC                                                          AS via_etf_b,
# MAGIC   COUNT(DISTINCT e.client_id)                            AS clients_exposed
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC WHERE e.sector != 'Cash'
# MAGIC GROUP BY e.sector
# MAGIC ORDER BY total_exposure_b DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which clients have the most Technology sector exposure?"
# MAGIC %sql
# MAGIC -- Ranks clients by their total Technology exposure (direct + ETF look-through).
# MAGIC SELECT
# MAGIC   e.client_id,
# MAGIC   e.client_name,
# MAGIC   e.advisor_id,
# MAGIC   e.tier,
# MAGIC   e.risk_profile,
# MAGIC   ROUND(SUM(e.exposure_market_value) / 1e6, 3)           AS tech_exposure_m,
# MAGIC   ROUND(SUM(e.exposure_market_value) / NULLIF(
# MAGIC     SUM(SUM(e.exposure_market_value)) OVER (PARTITION BY e.client_id), 0) * 100, 2)
# MAGIC                                                          AS tech_pct_of_portfolio,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'Direct' THEN e.exposure_market_value ELSE 0 END) / 1e6, 3)
# MAGIC                                                          AS direct_m,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'ETF'    THEN e.exposure_market_value ELSE 0 END) / 1e6, 3)
# MAGIC                                                          AS via_etf_m
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC WHERE e.sector IN ('Information Technology', 'Technology', 'Communication Services')
# MAGIC GROUP BY e.client_id, e.client_name, e.advisor_id, e.tier, e.risk_profile
# MAGIC ORDER BY tech_exposure_m DESC
# MAGIC LIMIT 25

# COMMAND ----------

# DBTITLE 1,[GENIE] "How much of the Financials exposure is direct vs through ETFs?"
# MAGIC %sql
# MAGIC -- Shows all instruments contributing to Financials sector exposure,
# MAGIC -- split by direct holdings vs ETF look-through vehicles.
# MAGIC SELECT
# MAGIC   e.source_type,
# MAGIC   e.source_ticker,
# MAGIC   e.source_name,
# MAGIC   ROUND(SUM(e.exposure_market_value) / 1e6, 3)    AS exposure_m,
# MAGIC   ROUND(SUM(e.exposure_market_value) / NULLIF(SUM(SUM(e.exposure_market_value)) OVER (), 0) * 100, 2)
# MAGIC                                                   AS pct_of_financials,
# MAGIC   COUNT(DISTINCT e.account_id)                    AS accounts
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC WHERE e.sector IN ('Financials', 'Financial Services')
# MAGIC GROUP BY e.source_type, e.source_ticker, e.source_name
# MAGIC ORDER BY exposure_m DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "What is the sector breakdown for accounts with the most drift?"
# MAGIC %sql
# MAGIC -- Combines sector look-through with IPS drift data.
# MAGIC -- Shows sector exposure for the top 10 accounts by drift score.
# MAGIC WITH
# MAGIC drifted_accounts AS (
# MAGIC   SELECT account_id, MAX(out_of_bounds_pct) AS max_breach
# MAGIC   FROM gold_account_ips_drift
# MAGIC   WHERE drift_status != 'Within Band'
# MAGIC   GROUP BY account_id
# MAGIC   ORDER BY max_breach DESC
# MAGIC   LIMIT 10
# MAGIC )
# MAGIC SELECT
# MAGIC   e.account_id,
# MAGIC   e.account_name,
# MAGIC   e.client_name,
# MAGIC   e.advisor_id,
# MAGIC   e.risk_profile,
# MAGIC   d.max_breach                                    AS max_ips_breach_pct,
# MAGIC   e.sector,
# MAGIC   ROUND(SUM(e.exposure_market_value) / 1e6, 3)    AS exposure_m,
# MAGIC   ROUND(SUM(e.exposure_market_value)
# MAGIC     / NULLIF(SUM(SUM(e.exposure_market_value)) OVER (PARTITION BY e.account_id), 0) * 100, 2)
# MAGIC                                                   AS pct_of_account
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC JOIN drifted_accounts d ON e.account_id = d.account_id
# MAGIC WHERE e.sector != 'Cash'
# MAGIC GROUP BY e.account_id, e.account_name, e.client_name, e.advisor_id,
# MAGIC          e.risk_profile, d.max_breach, e.sector
# MAGIC ORDER BY d.max_breach DESC, e.account_id, exposure_m DESC

# COMMAND ----------

# DBTITLE 1,[GENIE] "Which sectors have the most ETF-derived vs direct exposure?"
# MAGIC %sql
# MAGIC -- Reveals which sectors the book is expressing passively (via ETFs) vs actively
# MAGIC -- (direct stock picks). High ETF% = passive; high direct% = active management.
# MAGIC SELECT
# MAGIC   e.sector,
# MAGIC   ROUND(SUM(e.exposure_market_value) / 1e9, 4)                              AS total_b,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'ETF'    THEN e.exposure_market_value ELSE 0 END)
# MAGIC         / NULLIF(SUM(e.exposure_market_value), 0) * 100, 1)                 AS etf_pct,
# MAGIC   ROUND(SUM(CASE WHEN e.source_type = 'Direct' THEN e.exposure_market_value ELSE 0 END)
# MAGIC         / NULLIF(SUM(e.exposure_market_value), 0) * 100, 1)                 AS direct_pct,
# MAGIC   ARRAY_JOIN(
# MAGIC     ARRAY_SORT(COLLECT_SET(
# MAGIC       CASE WHEN e.source_type = 'Direct' THEN e.source_ticker END
# MAGIC     )), ', ')                                                                AS direct_tickers
# MAGIC FROM gold_portfolio_sector_exposure e
# MAGIC WHERE e.sector NOT IN ('Cash', 'Unknown')
# MAGIC GROUP BY e.sector
# MAGIC ORDER BY total_b DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_ips_drift
# MAGIC One row per (account_id, asset_class). Computed live — no stale data.
# MAGIC Asset classes with zero holdings appear via cross-join so every IPS cell is visible.
# MAGIC holdings.asset_class is the true economic class (ETFs reclassified at write time).
# MAGIC
# MAGIC Drift metrics:
# MAGIC   drift_from_target_pct  — actual % − target % (+ = overweight, − = underweight)
# MAGIC   out_of_bounds_pct      — distance outside the min/max band (0 when in band)
# MAGIC   drift_status           — 'Over Band' | 'Under Band' | 'Within Band'
# MAGIC   drift_severity         — 'Critical' | 'Warning' | 'OK'
# MAGIC   rebalance_to_target    — $ to hit exact target (negative = sell)
# MAGIC   rebalance_to_band      — $ to get just back inside band (0 when in band)

# COMMAND ----------

# DBTITLE 1,─── 4. gold_ips_drift (view) ──────────────────────────────────────────────────────

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

# COMMAND ----------

print("Dashboard tables complete:")
print(f"  gold_financial_fundamentals    — financials, ratios, analyst estimates")
print(f"  gold_financials_vs_estimates   — actuals vs consensus, beat/miss flags")
print(f"  gold_portfolio_sector_exposure — ETF look-through sector exposure")
print(f"  gold_ips_drift                 — IPS allocation drift (live view)")
