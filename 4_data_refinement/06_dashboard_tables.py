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

# MAGIC %md
# MAGIC ## gold_portfolio_sector_exposure
# MAGIC One row per (account_id, source_ticker, sector). ETF positions fan out into N rows
# MAGIC (one per sector in bronze_etf_sectors). All financial metrics are scaled by
# MAGIC weight_in_source and remain additive at any grain.
# MAGIC
# MAGIC SECTION A — Gold Table Creation
# MAGIC   gold_portfolio_sector_exposure — look-through exposure per (account_id, sector)
# MAGIC                                    ETF positions are expanded into constituent sectors

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

print("Dashboard tables complete:")
print(f"  gold_financial_fundamentals    — financials, ratios, analyst estimates")
print(f"  gold_financials_vs_estimates   — actuals vs consensus, beat/miss flags")
print(f"  gold_portfolio_sector_exposure — ETF look-through sector exposure")
print(f"  gold_ips_drift                 — IPS allocation drift (live view)")
