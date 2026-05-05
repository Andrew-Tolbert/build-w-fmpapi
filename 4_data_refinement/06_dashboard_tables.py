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

# DBTITLE 1,─── 1. gold_financial_fundamentals ────────────────────────────────────────────────


# COMMAND ----------

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
# MAGIC   ROUND(
# MAGIC     (pt.targetConsensus - lp.current_price) / NULLIF(lp.current_price, 0) * 100,
# MAGIC     2)                                          AS analyst_upside_pct,
# MAGIC   ROUND(i.ebitda   / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                AS ebitda_margin_calc,
# MAGIC   ROUND(i.grossProfit / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                AS gross_margin_calc,
# MAGIC   ROUND(i.netIncome / NULLIF(i.revenue, 0) * 100, 2)
# MAGIC                                                AS net_margin_calc,
# MAGIC   ROUND(b.totalDebt / NULLIF(i.ebitda, 0), 2) AS total_debt_to_ebitda_calc,
# MAGIC   ROUND(i.ebit / NULLIF(i.interestExpense, 0), 2)
# MAGIC                                                AS interest_coverage_calc,
# MAGIC   ROUND(b.netDebt / NULLIF(ae.ebitdaAvg, 0), 2)
# MAGIC                                                AS forward_nd_ebitda,
# MAGIC
# MAGIC   -- ── Leverage flag ─────────────────────────────────────────────────────────
# MAGIC   CASE
# MAGIC     WHEN km.netDebtToEBITDA > 5  THEN 'High'
# MAGIC     WHEN km.netDebtToEBITDA > 3  THEN 'Elevated'
# MAGIC     WHEN km.netDebtToEBITDA <= 3 THEN 'Normal'
# MAGIC     ELSE 'N/A'
# MAGIC   END                                          AS leverage_flag,
# MAGIC
# MAGIC   CURRENT_TIMESTAMP()                          AS updated_at
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
# MAGIC SELECT COUNT(*) AS row_count, COUNT(DISTINCT symbol) AS symbols,
# MAGIC        MIN(date) AS earliest_period, MAX(date) AS latest_period
# MAGIC FROM gold_financial_fundamentals

# COMMAND ----------

# DBTITLE 1,─── 2. gold_financials_vs_estimates ──────────────────────────────────────────────


# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (symbol, reporting period).
# MAGIC -- Period-matches actuals to analyst consensus using a ±7-day window.
# MAGIC -- Covers EPS, revenue, EBITDA, and net income beat/miss classification.
# MAGIC CREATE OR REPLACE TABLE gold_financials_vs_estimates AS
# MAGIC WITH period_matched AS (
# MAGIC   SELECT
# MAGIC     ae.symbol,
# MAGIC     i.date                                                AS period_end,
# MAGIC     i.fiscalYear                                          AS fiscal_year,
# MAGIC     i.period,
# MAGIC     i.revenue                                             AS actual_revenue,
# MAGIC     i.ebitda                                              AS actual_ebitda,
# MAGIC     i.netIncome                                           AS actual_net_income,
# MAGIC     i.epsDiluted                                          AS actual_eps,
# MAGIC     i.interestExpense                                     AS actual_interest_expense,
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
# MAGIC   ROUND(pm.actual_revenue          / 1e6, 2)             AS actual_revenue_m,
# MAGIC   ROUND(pm.actual_ebitda           / 1e6, 2)             AS actual_ebitda_m,
# MAGIC   ROUND(pm.actual_net_income       / 1e6, 2)             AS actual_net_income_m,
# MAGIC   pm.actual_eps,
# MAGIC   ROUND(pm.actual_interest_expense / 1e6, 2)             AS actual_interest_expense_m,
# MAGIC
# MAGIC   ROUND(pm.est_revenue      / 1e6, 2)                    AS est_revenue_m,
# MAGIC   ROUND(pm.est_revenue_low  / 1e6, 2)                    AS est_revenue_low_m,
# MAGIC   ROUND(pm.est_revenue_high / 1e6, 2)                    AS est_revenue_high_m,
# MAGIC   ROUND(pm.est_ebitda       / 1e6, 2)                    AS est_ebitda_m,
# MAGIC   ROUND(pm.est_net_income   / 1e6, 2)                    AS est_net_income_m,
# MAGIC   pm.est_eps,
# MAGIC   pm.est_eps_low,
# MAGIC   pm.est_eps_high,
# MAGIC   pm.num_analysts_revenue,
# MAGIC   pm.num_analysts_eps,
# MAGIC
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
# MAGIC SELECT COUNT(*) AS row_count, COUNT(DISTINCT symbol) AS symbols,
# MAGIC        SUM(CASE WHEN eps_beat_miss = 'Beat' THEN 1 ELSE 0 END) AS beats,
# MAGIC        SUM(CASE WHEN eps_beat_miss = 'Miss' THEN 1 ELSE 0 END) AS misses,
# MAGIC        MIN(period_end) AS earliest_period, MAX(period_end) AS latest_period
# MAGIC FROM gold_financials_vs_estimates

# COMMAND ----------

# DBTITLE 1,─── 3. gold_portfolio_sector_exposure ─────────────────────────────────────────────


# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (account_id, source_ticker, sector).
# MAGIC -- ETF positions fan out into N rows (one per sector in bronze_etf_sectors).
# MAGIC -- All financial metrics are scaled by weight_in_source and remain additive at any grain.
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
# MAGIC   ROUND(h.market_value     * es.weightPercentage / 100, 2)       AS exposure_market_value,
# MAGIC   ROUND(h.unrealized_gl    * es.weightPercentage / 100, 2)       AS exposure_unrealized_gl,
# MAGIC   ROUND(h.total_cost_basis * es.weightPercentage / 100, 2)       AS exposure_cost_basis,
# MAGIC   h.cost_basis_per_share,
# MAGIC   ROUND(h.quantity         * es.weightPercentage / 100, 6)       AS quantity,
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

# DBTITLE 1,─── 4. gold_ips_drift (view) ──────────────────────────────────────────────────────


# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (account_id, asset_class). Computed live — no stale data.
# MAGIC -- Asset classes with zero holdings appear via cross-join so every IPS cell is visible.
# MAGIC -- holdings.asset_class is the true economic class (ETFs reclassified at write time).
# MAGIC CREATE OR REPLACE VIEW gold_ips_drift AS
# MAGIC WITH
# MAGIC account_totals AS (
# MAGIC   SELECT account_id, SUM(market_value) AS total_account_value
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id
# MAGIC ),
# MAGIC actual_by_class AS (
# MAGIC   SELECT account_id, asset_class,
# MAGIC          SUM(market_value) AS actual_market_value,
# MAGIC          COUNT(*)          AS positions_count
# MAGIC   FROM holdings
# MAGIC   GROUP BY account_id, asset_class
# MAGIC ),
# MAGIC account_class_grid AS (
# MAGIC   SELECT a.account_id, it.asset_class
# MAGIC   FROM (SELECT DISTINCT account_id FROM holdings) a
# MAGIC   CROSS JOIN (SELECT DISTINCT asset_class FROM ips_targets) it
# MAGIC )
# MAGIC SELECT
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
# MAGIC   ROUND(COALESCE(ab.actual_market_value, 0), 2)              AS actual_market_value,
# MAGIC   ROUND(at.total_account_value, 2)                           AS total_account_value,
# MAGIC   COALESCE(ab.positions_count, 0)                            AS positions_count,
# MAGIC   ROUND(
# MAGIC     COALESCE(ab.actual_market_value, 0)
# MAGIC     / NULLIF(at.total_account_value, 0) * 100, 4)            AS actual_allocation_pct,
# MAGIC
# MAGIC   it.target_allocation_pct,
# MAGIC   it.min_allocation_pct,
# MAGIC   it.max_allocation_pct,
# MAGIC   it.rebalance_trigger_pct,
# MAGIC   ROUND(it.target_allocation_pct / 100 * at.total_account_value, 2) AS target_market_value,
# MAGIC   ROUND(it.min_allocation_pct    / 100 * at.total_account_value, 2) AS min_market_value,
# MAGIC   ROUND(it.max_allocation_pct    / 100 * at.total_account_value, 2) AS max_market_value,
# MAGIC
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
# MAGIC JOIN accounts      ac ON g.account_id  = ac.account_id
# MAGIC JOIN clients       c  ON ac.client_id  = c.client_id
# MAGIC JOIN account_totals at ON g.account_id = at.account_id
# MAGIC JOIN ips_targets   it ON c.risk_profile = it.risk_profile
# MAGIC                       AND g.asset_class  = it.asset_class
# MAGIC LEFT JOIN actual_by_class ab
# MAGIC   ON g.account_id = ab.account_id AND g.asset_class = ab.asset_class

# COMMAND ----------

# MAGIC %sql
# MAGIC -- One row per (date, advisor_id), trailing 365 calendar days.
# MAGIC -- Returns are indexed to 0% at the first trading day in the window:
# MAGIC --   portfolio_baseline  = actual advisor AUM on day 1
# MAGIC --   benchmark_baseline  = GSPC close on day 1
# MAGIC CREATE OR REPLACE TABLE silver_advisor_daily_returns
# MAGIC   COMMENT 'Daily portfolio vs S&P 500 (GSPC) return timeseries per advisor, trailing 365 days.' AS
# MAGIC WITH params AS (
# MAGIC   SELECT
# MAGIC     date_sub(current_date(), 365) AS start_dt,
# MAGIC     current_date()                AS end_dt
# MAGIC ),
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     max(CASE WHEN date <= (SELECT end_dt   FROM params) THEN date END) AS end_price_dt,
# MAGIC     max(CASE WHEN date <= (SELECT start_dt FROM params) THEN date END) AS start_price_dt
# MAGIC   FROM bronze_historical_prices
# MAGIC ),
# MAGIC trading_days AS (
# MAGIC   SELECT DISTINCT date
# MAGIC   FROM bronze_historical_prices
# MAGIC   WHERE date >= (SELECT start_price_dt FROM price_dates)
# MAGIC     AND date <= (SELECT end_price_dt   FROM price_dates)
# MAGIC ),
# MAGIC filtered_positions AS (
# MAGIC   SELECT
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id,
# MAGIC     sum(t.quantity)     AS quantity,
# MAGIC     sum(t.gross_amount) AS total_cost
# MAGIC   FROM transactions t
# MAGIC   JOIN accounts a ON t.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE t.action IN ('BUY', 'DRIP')
# MAGIC     AND t.ticker != 'CASH'
# MAGIC     AND t.date <= (SELECT end_dt FROM params)
# MAGIC   GROUP BY
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id
# MAGIC ),
# MAGIC daily_portfolio AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     fp.advisor_id,
# MAGIC     sum(fp.quantity * hp.adjClose) AS portfolio_value
# MAGIC   FROM trading_days td
# MAGIC   CROSS JOIN filtered_positions fp
# MAGIC   JOIN bronze_historical_prices hp
# MAGIC     ON hp.symbol = fp.ticker
# MAGIC    AND hp.date   = td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     fp.advisor_id
# MAGIC ),
# MAGIC portfolio_baseline AS (
# MAGIC   SELECT
# MAGIC     advisor_id,
# MAGIC     portfolio_value AS base
# MAGIC   FROM daily_portfolio
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC daily_benchmark AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     max_by(v.close, v.date) AS benchmark_value
# MAGIC   FROM trading_days td
# MAGIC   LEFT JOIN bronze_indexes_and_vix v
# MAGIC     ON v.symbol = 'GSPC'
# MAGIC    AND v.date   = td.date
# MAGIC   GROUP BY td.date
# MAGIC ),
# MAGIC benchmark_baseline AS (
# MAGIC   SELECT benchmark_value AS base
# MAGIC   FROM daily_benchmark
# MAGIC   WHERE date = (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC advisor_fees AS (
# MAGIC   SELECT
# MAGIC     f.date,
# MAGIC     f.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM transactions f
# MAGIC   JOIN (
# MAGIC     SELECT DISTINCT account_id, advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) fp ON f.account_id = fp.account_id
# MAGIC   WHERE f.action = 'FEE'
# MAGIC     AND f.date >= (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC fees_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     coalesce(sum(abs(af.net_amount)), 0) AS cumulative_fees
# MAGIC   FROM trading_days td
# MAGIC   CROSS JOIN (
# MAGIC     SELECT DISTINCT advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) adv
# MAGIC   LEFT JOIN advisor_fees af
# MAGIC     ON af.advisor_id = adv.advisor_id
# MAGIC    AND af.date      <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC advisor_inflows AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.quantity AS amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM transactions t
# MAGIC   JOIN (
# MAGIC     SELECT DISTINCT account_id, advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) fp ON t.account_id = fp.account_id
# MAGIC   WHERE t.action = 'BUY'
# MAGIC     AND t.ticker = 'CASH'
# MAGIC     AND t.date  >= (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC inflows_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     coalesce(sum(ai.amount), 0) AS cumulative_inflows
# MAGIC   FROM trading_days td
# MAGIC   CROSS JOIN (
# MAGIC     SELECT DISTINCT advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) adv
# MAGIC   LEFT JOIN advisor_inflows ai
# MAGIC     ON ai.advisor_id = adv.advisor_id
# MAGIC    AND ai.date      <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC advisor_dividends AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM transactions t
# MAGIC   JOIN (
# MAGIC     SELECT DISTINCT account_id, advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) fp ON t.account_id = fp.account_id
# MAGIC   WHERE t.action = 'DIVIDEND'
# MAGIC     AND t.date  >= (SELECT start_price_dt FROM price_dates)
# MAGIC ),
# MAGIC dividends_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     coalesce(sum(ad.net_amount), 0) AS cumulative_dividends
# MAGIC   FROM trading_days td
# MAGIC   CROSS JOIN (
# MAGIC     SELECT DISTINCT advisor_id
# MAGIC     FROM filtered_positions
# MAGIC   ) adv
# MAGIC   LEFT JOIN advisor_dividends ad
# MAGIC     ON ad.advisor_id = adv.advisor_id
# MAGIC    AND ad.date      <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   dp.date,
# MAGIC   dp.advisor_id,
# MAGIC   round(dp.portfolio_value / nullif(pb.base, 0) - 1, 6) AS portfolio_return_before_fees,
# MAGIC   round((dp.portfolio_value - fd.cumulative_fees) / nullif(pb.base, 0) - 1, 6) AS portfolio_return_after_fees,
# MAGIC   round(
# MAGIC     (dp.portfolio_value - fd.cumulative_fees) / nullif(pb.base, 0)
# MAGIC       - db.benchmark_value / nullif(bb.base, 0),
# MAGIC     6
# MAGIC   ) AS portfolio_alpha,
# MAGIC   round(fd.cumulative_fees,     2) AS cumulative_fees,
# MAGIC   round(id.cumulative_inflows,  2) AS cumulative_inflows,
# MAGIC   round(dd.cumulative_dividends,2) AS cumulative_dividends,
# MAGIC   round(db.benchmark_value / nullif(bb.base, 0) - 1, 6) AS benchmark_return,
# MAGIC   'GSPC' AS benchmark
# MAGIC FROM daily_portfolio dp
# MAGIC LEFT JOIN daily_benchmark db
# MAGIC   ON dp.date = db.date
# MAGIC LEFT JOIN fees_by_day fd
# MAGIC   ON dp.date       = fd.date
# MAGIC  AND dp.advisor_id = fd.advisor_id
# MAGIC LEFT JOIN inflows_by_day id
# MAGIC   ON dp.date       = id.date
# MAGIC  AND dp.advisor_id = id.advisor_id
# MAGIC LEFT JOIN dividends_by_day dd
# MAGIC   ON dp.date       = dd.date
# MAGIC  AND dp.advisor_id = dd.advisor_id
# MAGIC JOIN portfolio_baseline pb
# MAGIC   ON dp.advisor_id = pb.advisor_id
# MAGIC CROSS JOIN benchmark_baseline bb
# MAGIC ORDER BY
# MAGIC   dp.advisor_id,
# MAGIC   dp.date;

# COMMAND ----------

print("Dashboard tables complete:")
print(f"  gold_financial_fundamentals    — financials, ratios, analyst estimates")
print(f"  gold_financials_vs_estimates   — actuals vs consensus, beat/miss flags")
print(f"  gold_portfolio_sector_exposure — ETF look-through sector exposure")
print(f"  gold_ips_drift                 — IPS allocation drift (live view)")
print(f"  silver_advisor_daily_returns   — daily returns")
