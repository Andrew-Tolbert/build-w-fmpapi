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
# MAGIC # Genie Context Queries
# MAGIC
# MAGIC Static SQL for Genie space context. Paste each block into Genie alongside the suggested question.
# MAGIC Organized by topic — each section corresponds to a gold table.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Financial Fundamentals

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
# MAGIC ## Financials vs Estimates

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
# MAGIC ## Sector Exposure

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
# MAGIC ## IPS Drift

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
