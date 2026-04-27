# Databricks notebook source
# Portfolio Performance Queries — Ask 5
#
# Three SQL-only queries using ${uc_catalog}.${uc_schema} widget params:
#   1. Overall AUM return since 2025
#   2. YTD portfolio return vs S&P 500
#   3. Individual client daily return vs S&P 500 since inception
#
# All metrics expressed as percentages.
# Holdings quantities are used as-of today — prices are applied historically
# to reconstruct portfolio value at each reference date.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %md ## Query 1 — Overall AUM Performance Since 2025

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC start_date AS (
# MAGIC   SELECT MIN(date) AS d
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC   WHERE date >= '2025-01-01'
# MAGIC ),
# MAGIC end_date AS (
# MAGIC   SELECT MAX(date) AS d
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC ),
# MAGIC -- Aggregate positions across all accounts (non-cash)
# MAGIC current_positions AS (
# MAGIC   SELECT ticker, SUM(quantity) AS total_qty
# MAGIC   FROM ${uc_catalog}.${uc_schema}.holdings
# MAGIC   WHERE ticker != 'CASH'
# MAGIC   GROUP BY ticker
# MAGIC ),
# MAGIC -- Mark positions to 2025 start prices
# MAGIC value_at_start AS (
# MAGIC   SELECT SUM(cp.total_qty * hp.adjClose) AS aum_start
# MAGIC   FROM current_positions cp
# MAGIC   JOIN ${uc_catalog}.${uc_schema}.bronze_historical_prices hp
# MAGIC     ON cp.ticker = hp.symbol
# MAGIC     AND hp.date = (SELECT d FROM start_date)
# MAGIC ),
# MAGIC -- Mark positions to latest prices
# MAGIC value_at_end AS (
# MAGIC   SELECT SUM(cp.total_qty * hp.adjClose) AS aum_end
# MAGIC   FROM current_positions cp
# MAGIC   JOIN ${uc_catalog}.${uc_schema}.bronze_historical_prices hp
# MAGIC     ON cp.ticker = hp.symbol
# MAGIC     AND hp.date = (SELECT d FROM end_date)
# MAGIC )
# MAGIC SELECT
# MAGIC   (SELECT d FROM start_date)                                        AS period_start,
# MAGIC   (SELECT d FROM end_date)                                          AS period_end,
# MAGIC   ROUND(vs.aum_start / 1e9, 2)                                     AS aum_start_B,
# MAGIC   ROUND(ve.aum_end   / 1e9, 2)                                     AS aum_end_B,
# MAGIC   ROUND((ve.aum_end - vs.aum_start) / 1e9, 2)                     AS aum_change_B,
# MAGIC   ROUND((ve.aum_end - vs.aum_start) / vs.aum_start * 100, 2)      AS return_pct_since_2025
# MAGIC FROM value_at_start vs
# MAGIC CROSS JOIN value_at_end ve

# COMMAND ----------

# MAGIC %md ## Query 2 — YTD Portfolio Return vs S&P 500

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC ytd_start AS (
# MAGIC   SELECT MIN(date) AS d
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC   WHERE date >= '2025-01-01'
# MAGIC ),
# MAGIC ytd_end AS (
# MAGIC   SELECT MAX(date) AS d
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices
# MAGIC ),
# MAGIC current_positions AS (
# MAGIC   SELECT ticker, SUM(quantity) AS total_qty
# MAGIC   FROM ${uc_catalog}.${uc_schema}.holdings
# MAGIC   WHERE ticker != 'CASH'
# MAGIC   GROUP BY ticker
# MAGIC ),
# MAGIC portfolio_ytd_start AS (
# MAGIC   SELECT SUM(cp.total_qty * hp.adjClose) AS value
# MAGIC   FROM current_positions cp
# MAGIC   JOIN ${uc_catalog}.${uc_schema}.bronze_historical_prices hp
# MAGIC     ON cp.ticker = hp.symbol
# MAGIC     AND hp.date = (SELECT d FROM ytd_start)
# MAGIC ),
# MAGIC portfolio_ytd_end AS (
# MAGIC   SELECT SUM(cp.total_qty * hp.adjClose) AS value
# MAGIC   FROM current_positions cp
# MAGIC   JOIN ${uc_catalog}.${uc_schema}.bronze_historical_prices hp
# MAGIC     ON cp.ticker = hp.symbol
# MAGIC     AND hp.date = (SELECT d FROM ytd_end)
# MAGIC ),
# MAGIC sp500_ytd_start AS (
# MAGIC   SELECT close AS value
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_indexes_and_vix
# MAGIC   WHERE symbol = '^GSPC'
# MAGIC     AND date = (SELECT d FROM ytd_start)
# MAGIC ),
# MAGIC sp500_ytd_end AS (
# MAGIC   SELECT close AS value
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_indexes_and_vix
# MAGIC   WHERE symbol = '^GSPC'
# MAGIC     AND date = (SELECT d FROM ytd_end)
# MAGIC )
# MAGIC SELECT
# MAGIC   (SELECT d FROM ytd_start)                                                  AS ytd_start,
# MAGIC   (SELECT d FROM ytd_end)                                                    AS ytd_end,
# MAGIC   ROUND((pe.value - ps.value) / ps.value * 100, 2)                          AS portfolio_ytd_pct,
# MAGIC   ROUND((se.value - ss.value) / ss.value * 100, 2)                          AS sp500_ytd_pct,
# MAGIC   ROUND(((pe.value - ps.value) / ps.value
# MAGIC          - (se.value - ss.value) / ss.value) * 100, 2)                       AS alpha_vs_sp500_pct
# MAGIC FROM portfolio_ytd_start ps
# MAGIC CROSS JOIN portfolio_ytd_end pe
# MAGIC CROSS JOIN sp500_ytd_start ss
# MAGIC CROSS JOIN sp500_ytd_end se

# COMMAND ----------

# MAGIC %md ## Query 3 — Individual Client Daily Return vs S&P 500 Since Inception
# MAGIC
# MAGIC Selects the highest-AUM UHNW client. Both series are indexed to 0% at inception
# MAGIC (the first trading day on or after the client's inception date).

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC WITH
# MAGIC -- Pick one client — largest UHNW by AUM
# MAGIC sample_client AS (
# MAGIC   SELECT client_id, client_name, inception_date
# MAGIC   FROM ${uc_catalog}.${uc_schema}.clients
# MAGIC   WHERE tier = 'UHNW'
# MAGIC   ORDER BY total_aum DESC
# MAGIC   LIMIT 1
# MAGIC ),
# MAGIC -- All accounts belonging to this client
# MAGIC client_accounts AS (
# MAGIC   SELECT a.account_id
# MAGIC   FROM ${uc_catalog}.${uc_schema}.accounts a
# MAGIC   JOIN sample_client c ON a.client_id = c.client_id
# MAGIC ),
# MAGIC -- Aggregate non-cash positions across all client accounts
# MAGIC client_positions AS (
# MAGIC   SELECT h.ticker, SUM(h.quantity) AS total_qty
# MAGIC   FROM ${uc_catalog}.${uc_schema}.holdings h
# MAGIC   JOIN client_accounts ca ON h.account_id = ca.account_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC   GROUP BY h.ticker
# MAGIC ),
# MAGIC -- Daily portfolio value since client inception
# MAGIC daily_portfolio AS (
# MAGIC   SELECT
# MAGIC     hp.date,
# MAGIC     SUM(cp.total_qty * hp.adjClose) AS portfolio_value
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_historical_prices hp
# MAGIC   JOIN client_positions cp  ON hp.symbol = cp.ticker
# MAGIC   JOIN sample_client sc     ON hp.date >= sc.inception_date
# MAGIC   GROUP BY hp.date
# MAGIC ),
# MAGIC -- Portfolio value on the first available trading day at or after inception
# MAGIC portfolio_inception AS (
# MAGIC   SELECT portfolio_value AS value_at_inception
# MAGIC   FROM daily_portfolio
# MAGIC   WHERE date = (SELECT MIN(date) FROM daily_portfolio)
# MAGIC ),
# MAGIC -- S&P 500 daily series from client inception onward
# MAGIC daily_sp500 AS (
# MAGIC   SELECT sp.date, sp.close AS sp500_close
# MAGIC   FROM ${uc_catalog}.${uc_schema}.bronze_indexes_and_vix sp
# MAGIC   JOIN sample_client sc ON sp.date >= sc.inception_date
# MAGIC   WHERE sp.symbol = '^GSPC'
# MAGIC ),
# MAGIC -- S&P 500 value on that same inception date
# MAGIC sp500_inception AS (
# MAGIC   SELECT sp500_close AS value_at_inception
# MAGIC   FROM daily_sp500
# MAGIC   WHERE date = (SELECT MIN(date) FROM daily_sp500)
# MAGIC )
# MAGIC SELECT
# MAGIC   dp.date,
# MAGIC   sc.client_name,
# MAGIC   ROUND((dp.portfolio_value / pi.value_at_inception - 1) * 100, 2) AS portfolio_return_pct,
# MAGIC   ROUND((dsp.sp500_close   / si.value_at_inception - 1) * 100, 2) AS sp500_return_pct,
# MAGIC   ROUND(
# MAGIC     ((dp.portfolio_value / pi.value_at_inception)
# MAGIC      - (dsp.sp500_close  / si.value_at_inception)) * 100, 2
# MAGIC   )                                                                 AS alpha_pct
# MAGIC FROM daily_portfolio dp
# MAGIC JOIN daily_sp500 dsp         ON dp.date = dsp.date
# MAGIC CROSS JOIN portfolio_inception pi
# MAGIC CROSS JOIN sp500_inception si
# MAGIC CROSS JOIN sample_client sc
# MAGIC ORDER BY dp.date
