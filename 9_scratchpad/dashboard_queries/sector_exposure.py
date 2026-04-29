# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Sector Exposure — Look-Through Dashboard & Genie Queries
#
# Answers: "What is my true sector exposure when I look through ETF wrappers?"
#
# ETFs are not a sector — they are wrappers around real sector bets. This notebook
# builds a look-through exposure view by:
#   • Equities / BDCs:  sector from bronze_company_profiles.sector (GICS)
#   • ETF positions:    expand by bronze_etf_sectors.weightPercentage × market_value
#   • Cash:             sector = 'Cash'
#
# All financial metrics (market_value, unrealized_gl, cost_basis) are scaled by
# weight_in_source so they stay additive at any aggregation grain. ETF sector
# weights sum to 1.0, so portfolio-level totals are preserved.
#
# Note: sector weights reflect the latest bronze_etf_sectors fetch. Historical
# queries use current weights — acceptable for demo, not production-accurate.
#
# Holdings and transactions are not modified. This is a reporting-layer enrichment only.
#
#   SECTION A — Gold Table Creation
#     gold_portfolio_sector_exposure — look-through exposure per (account_id, sector)
#                                      ETF positions are expanded into constituent sectors
#
#   SECTION B — Lakeview Dashboard Queries  (:param syntax)
#     1. Sector Exposure Overview     — total exposure by sector across the book
#     2. Sector Exposure by Account   — per-account sector breakdown with ETF look-through
#     3. Sector Concentration Risk    — top sector concentrations per client
#     4. ETF vs Direct Exposure       — for each sector, how much is via ETF vs direct stock
#
#   SECTION C — Genie Context Queries  (paste into Genie as context)
#     1. What is the portfolio's largest sector exposure?
#     2. Which clients have the most Technology exposure?
#     3. How much of the Financials exposure is direct vs through ETFs?
#     4. What is the sector breakdown for a specific account?
#     5. Which sectors are most exposed to ETF look-through drift?
#
# Lakeview parameters (all optional; NULL = no filter / include all):
#   :advisor_id   — advisor ID      (multi-select)
#   :account_type — account type    (multi-select)
#   :client_id    — client ID       (multi-select)
#   :sector       — sector          (multi-select)
#   :source_type  — 'ETF' or 'Direct' (filter to look-through source)

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
# MAGIC select * from gold_portfolio_sector_exposure

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
