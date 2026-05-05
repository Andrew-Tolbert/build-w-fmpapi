# Databricks notebook source
# MAGIC %md
# MAGIC # AWM Advisor App — Portfolio Intelligence Queries
# MAGIC
# MAGIC Builds five `gold_app_*` tables covering **all advisors**.
# MAGIC The app filters by `:advisor_id` at query time — no hardcoded IDs here.
# MAGIC
# MAGIC | Gold table | Page Widget | App file |
# MAGIC |---|---|---|
# MAGIC | `gold_app_portfolio_summary` | 4 KPI stat cards | `config/queries/portfolio_summary.sql` |
# MAGIC | `gold_app_asset_allocation` | Asset Allocation donut chart | `config/queries/asset_allocation.sql` |
# MAGIC | `gold_app_performance_timeseries` | Performance vs Benchmark area chart | `config/queries/performance_timeseries.sql` |
# MAGIC | `gold_app_top_holdings` | Top 10 Holdings table | `config/queries/top_holdings.sql` |
# MAGIC | `gold_app_concentration_risk` | Client Concentration Risk heatmap | `config/queries/concentration_risk.sql` |
# MAGIC
# MAGIC **Key table:** `silver_advisor_daily_returns` — pre-computed cumulative portfolio return,
# MAGIC benchmark return (GSPC), and alpha per advisor per trading day. Anchored to 0% on 2025-05-05.
# MAGIC
# MAGIC Run all cells top-to-bottom to rebuild every gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog ahtsa;
# MAGIC use schema awm; 

# COMMAND ----------

# DBTITLE 1,Silver_Advisor_Daily_Returns
# MAGIC %sql
# MAGIC -- One row per (date, advisor_id). Covers the trailing 365 calendar days.
# MAGIC -- Returns are indexed to 0% at the first trading day in the window:
# MAGIC --   portfolio_baseline = actual advisor AUM on day 1 (not a computed cost basis)
# MAGIC --   benchmark_baseline = GSPC close on day 1
# MAGIC -- This guarantees both series start at exactly 0.0 on the first row.
# MAGIC CREATE OR REPLACE TABLE silver_advisor_daily_returns
# MAGIC   COMMENT 'Daily portfolio vs S&P 500 (GSPC) return timeseries per advisor, trailing 365 days.' AS
# MAGIC WITH -- ── Window: trailing 365 calendar days ───────────────────────────────────────
# MAGIC params AS (
# MAGIC   SELECT
# MAGIC     DATE_SUB(CURRENT_DATE(), 365) AS start_dt,
# MAGIC     CURRENT_DATE() AS end_dt
# MAGIC ),
# MAGIC -- ── Nearest available trading day on or before each bound ────────────────────
# MAGIC price_dates AS (
# MAGIC   SELECT
# MAGIC     MAX(
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           date
# MAGIC             <= (
# MAGIC               SELECT
# MAGIC                 end_dt
# MAGIC               FROM
# MAGIC                 params
# MAGIC             )
# MAGIC         THEN
# MAGIC           date
# MAGIC       END
# MAGIC     ) AS end_price_dt,
# MAGIC     MAX(
# MAGIC       CASE
# MAGIC         WHEN
# MAGIC           date
# MAGIC             <= (
# MAGIC               SELECT
# MAGIC                 start_dt
# MAGIC               FROM
# MAGIC                 params
# MAGIC             )
# MAGIC         THEN
# MAGIC           date
# MAGIC       END
# MAGIC     ) AS start_price_dt
# MAGIC   FROM
# MAGIC     bronze_historical_prices
# MAGIC ),
# MAGIC -- ── All trading days in the window ───────────────────────────────────────────
# MAGIC trading_days AS (
# MAGIC   SELECT DISTINCT
# MAGIC     date
# MAGIC   FROM
# MAGIC     bronze_historical_prices
# MAGIC   WHERE
# MAGIC     date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC     AND date
# MAGIC       <= (
# MAGIC         SELECT
# MAGIC           end_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Equity positions as of today, carrying advisor_id ────────────────────────
# MAGIC filtered_positions AS (
# MAGIC   SELECT
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id,
# MAGIC     SUM(t.quantity) AS quantity,
# MAGIC     SUM(t.gross_amount) AS total_cost
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN accounts a
# MAGIC         ON t.account_id = a.account_id
# MAGIC       JOIN clients c
# MAGIC         ON a.client_id = c.client_id
# MAGIC   WHERE
# MAGIC     t.action IN ('BUY', 'DRIP')
# MAGIC     AND t.ticker != 'CASH'
# MAGIC     AND t.date
# MAGIC       <= (
# MAGIC         SELECT
# MAGIC           end_dt
# MAGIC         FROM
# MAGIC           params
# MAGIC       )
# MAGIC   GROUP BY
# MAGIC     t.account_id,
# MAGIC     t.ticker,
# MAGIC     c.advisor_id
# MAGIC ),
# MAGIC -- ── Daily portfolio value per advisor ─────────────────────────────────────────
# MAGIC daily_portfolio AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     fp.advisor_id,
# MAGIC     SUM(fp.quantity * hp.adjClose) AS portfolio_value
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN filtered_positions fp
# MAGIC       JOIN bronze_historical_prices hp
# MAGIC         ON hp.symbol = fp.ticker
# MAGIC         AND hp.date = td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     fp.advisor_id
# MAGIC ),
# MAGIC -- ── Portfolio baseline = actual AUM on day 1 ─────────────────────────────────
# MAGIC portfolio_baseline AS (
# MAGIC   SELECT
# MAGIC     advisor_id,
# MAGIC     portfolio_value AS base
# MAGIC   FROM
# MAGIC     daily_portfolio
# MAGIC   WHERE
# MAGIC     date
# MAGIC       = (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Daily S&P 500 series (symbol = 'GSPC') ───────────────────────────────────
# MAGIC daily_benchmark AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     MAX_BY(v.close, v.date) AS benchmark_value
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       LEFT JOIN bronze_indexes_and_vix v
# MAGIC         ON v.symbol = 'GSPC'
# MAGIC         AND v.date = td.date
# MAGIC   GROUP BY
# MAGIC     td.date
# MAGIC ),
# MAGIC -- ── Benchmark baseline = GSPC close on day 1 ─────────────────────────────────
# MAGIC benchmark_baseline AS (
# MAGIC   SELECT
# MAGIC     benchmark_value AS base
# MAGIC   FROM
# MAGIC     daily_benchmark
# MAGIC   WHERE
# MAGIC     date
# MAGIC       = (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Fee transactions per advisor, pre-filtered to the window ─────────────────
# MAGIC advisor_fees AS (
# MAGIC   SELECT
# MAGIC     f.date,
# MAGIC     f.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions f
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON f.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     f.action = 'FEE'
# MAGIC     AND f.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative fees per advisor as a step function ────────────────────────────
# MAGIC fees_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ABS(af.net_amount)), 0) AS cumulative_fees
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_fees af
# MAGIC         ON af.advisor_id = adv.advisor_id
# MAGIC         AND af.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC -- ── Inflow transactions per advisor (cash deposits = BUY where ticker = CASH) ─
# MAGIC -- Mirrors the cash_positions logic in holdings_by_date_range: initial capital
# MAGIC -- coming into an account is recorded as BUY / CASH. No window filter — we want
# MAGIC -- the full cumulative inflow from the start of the return series.
# MAGIC advisor_inflows AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.quantity AS amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON t.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     t.action = 'BUY'
# MAGIC     AND t.ticker = 'CASH'
# MAGIC     AND t.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative inflows per advisor as a step function ─────────────────────────
# MAGIC inflows_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ai.amount), 0) AS cumulative_inflows
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_inflows ai
# MAGIC         ON ai.advisor_id = adv.advisor_id
# MAGIC         AND ai.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC ),
# MAGIC -- ── Dividend transactions per advisor, pre-filtered to the window ─────────────
# MAGIC -- net_amount on DIVIDEND rows is positive cash received (matches cash_positions logic).
# MAGIC advisor_dividends AS (
# MAGIC   SELECT
# MAGIC     t.date,
# MAGIC     t.net_amount,
# MAGIC     fp.advisor_id
# MAGIC   FROM
# MAGIC     transactions t
# MAGIC       JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           account_id,
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) fp
# MAGIC         ON t.account_id = fp.account_id
# MAGIC   WHERE
# MAGIC     t.action = 'DIVIDEND'
# MAGIC     AND t.date
# MAGIC       >= (
# MAGIC         SELECT
# MAGIC           start_price_dt
# MAGIC         FROM
# MAGIC           price_dates
# MAGIC       )
# MAGIC ),
# MAGIC -- ── Cumulative dividends per advisor as a step function ───────────────────────
# MAGIC dividends_by_day AS (
# MAGIC   SELECT
# MAGIC     td.date,
# MAGIC     adv.advisor_id,
# MAGIC     COALESCE(SUM(ad.net_amount), 0) AS cumulative_dividends
# MAGIC   FROM
# MAGIC     trading_days td
# MAGIC       CROSS JOIN (
# MAGIC         SELECT DISTINCT
# MAGIC           advisor_id
# MAGIC         FROM
# MAGIC           filtered_positions
# MAGIC       ) adv
# MAGIC       LEFT JOIN advisor_dividends ad
# MAGIC         ON ad.advisor_id = adv.advisor_id
# MAGIC         AND ad.date <= td.date
# MAGIC   GROUP BY
# MAGIC     td.date,
# MAGIC     adv.advisor_id
# MAGIC )
# MAGIC -- ── Final output — one row per (date, advisor_id) ─────────────────────────────
# MAGIC SELECT
# MAGIC   dp.date,
# MAGIC   dp.advisor_id,
# MAGIC   ROUND(dp.portfolio_value / NULLIF(pb.base, 0) - 1, 6) AS portfolio_return_before_fees,
# MAGIC   ROUND(
# MAGIC     (dp.portfolio_value - fd.cumulative_fees) / NULLIF(pb.base, 0) - 1,
# MAGIC     6
# MAGIC   ) AS portfolio_return_after_fees,
# MAGIC   ROUND(
# MAGIC     (dp.portfolio_value - fd.cumulative_fees) / NULLIF(pb.base, 0)
# MAGIC       - db.benchmark_value / NULLIF(bb.base, 0),
# MAGIC     6
# MAGIC   ) AS portfolio_alpha,
# MAGIC   ROUND(fd.cumulative_fees, 2) AS cumulative_fees,
# MAGIC   ROUND(id.cumulative_inflows, 2) AS cumulative_inflows,
# MAGIC   ROUND(dd.cumulative_dividends, 2) AS cumulative_dividends,
# MAGIC   ROUND(db.benchmark_value / NULLIF(bb.base, 0) - 1, 6) AS benchmark_return,
# MAGIC   'GSPC' AS benchmark
# MAGIC FROM
# MAGIC   daily_portfolio dp
# MAGIC     LEFT JOIN daily_benchmark db
# MAGIC       ON dp.date = db.date
# MAGIC     LEFT JOIN fees_by_day fd
# MAGIC       ON dp.date = fd.date
# MAGIC       AND dp.advisor_id = fd.advisor_id
# MAGIC     LEFT JOIN inflows_by_day id
# MAGIC       ON dp.date = id.date
# MAGIC       AND dp.advisor_id = id.advisor_id
# MAGIC     LEFT JOIN dividends_by_day dd
# MAGIC       ON dp.date = dd.date
# MAGIC       AND dp.advisor_id = dd.advisor_id
# MAGIC     JOIN portfolio_baseline pb
# MAGIC       ON dp.advisor_id = pb.advisor_id
# MAGIC     CROSS JOIN benchmark_baseline bb
# MAGIC ORDER BY
# MAGIC   dp.advisor_id,
# MAGIC   dp.date

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `gold_app_portfolio_summary`
# MAGIC **Schema:** `advisor_id`, `total_aum`, `perf_vs_bench_pct`, `drift_count`, `clients_at_risk`, `qtd_aum_change`
# MAGIC
# MAGIC - **total_aum** — SUM of `clients.total_aum` in raw dollars; frontend formats to $B/$M
# MAGIC - **perf_vs_bench_pct** — `portfolio_alpha` from the latest row of `silver_advisor_daily_returns`
# MAGIC - **drift_count** — count of account × asset_class slots with a Critical IPS breach
# MAGIC - **clients_at_risk** — distinct clients with at least one Critical severity IPS breach
# MAGIC - **qtd_aum_change** — raw dollar change (total_aum × QTD return); frontend formats to $B/$M

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_portfolio_summary AS
# MAGIC WITH
# MAGIC aum AS (
# MAGIC   SELECT advisor_id, SUM(total_aum) AS total_aum
# MAGIC   FROM ahtsa.awm.clients
# MAGIC   GROUP BY advisor_id
# MAGIC ),
# MAGIC perf AS (
# MAGIC   SELECT advisor_id, ROUND(portfolio_alpha * 100, 1) AS perf_vs_bench_pct
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC qtd_start AS (
# MAGIC   SELECT advisor_id, portfolio_return_before_fees AS qtd_start_return
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   WHERE date >= DATE_TRUNC('quarter', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC qtd_end AS (
# MAGIC   SELECT advisor_id, portfolio_return_before_fees AS qtd_end_return
# MAGIC   FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC qtd AS (
# MAGIC   SELECT
# MAGIC     s.advisor_id,
# MAGIC     ROUND((e.qtd_end_return - s.qtd_start_return) / (1 + s.qtd_start_return), 4) AS qtd_return
# MAGIC   FROM qtd_start s JOIN qtd_end e ON s.advisor_id = e.advisor_id
# MAGIC ),
# MAGIC drift AS (
# MAGIC   SELECT advisor_id, COUNT(*) AS drift_count
# MAGIC   FROM ahtsa.awm.gold_ips_drift
# MAGIC   WHERE drift_status != 'Within Band' AND drift_severity = 'Critical'
# MAGIC   GROUP BY advisor_id
# MAGIC ),
# MAGIC at_risk AS (
# MAGIC   SELECT d.advisor_id, COUNT(DISTINCT a.client_id) AS clients_at_risk
# MAGIC   FROM ahtsa.awm.gold_ips_drift d
# MAGIC   JOIN ahtsa.awm.accounts a ON d.account_id = a.account_id
# MAGIC   WHERE d.drift_severity = 'Critical'
# MAGIC   GROUP BY d.advisor_id
# MAGIC )
# MAGIC SELECT
# MAGIC   a.advisor_id,
# MAGIC   a.total_aum,
# MAGIC   p.perf_vs_bench_pct,
# MAGIC   COALESCE(d.drift_count, 0)                              AS drift_count,
# MAGIC   COALESCE(ar.clients_at_risk, 0)                         AS clients_at_risk,
# MAGIC   ROUND(a.total_aum * COALESCE(q.qtd_return, 0), 0)      AS qtd_aum_change
# MAGIC FROM aum a
# MAGIC LEFT JOIN perf    p  ON a.advisor_id = p.advisor_id
# MAGIC LEFT JOIN qtd     q  ON a.advisor_id = q.advisor_id
# MAGIC LEFT JOIN drift   d  ON a.advisor_id = d.advisor_id
# MAGIC LEFT JOIN at_risk ar ON a.advisor_id = ar.advisor_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ahtsa.awm.gold_app_portfolio_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `gold_app_asset_allocation`
# MAGIC **Schema:** `advisor_id`, `asset_class`, `pct_of_portfolio`
# MAGIC
# MAGIC Weighted allocation of each advisor's book by asset class.
# MAGIC Window `SUM` is partitioned by `advisor_id` so percentages sum to 100% per advisor.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_asset_allocation AS
# MAGIC SELECT
# MAGIC   c.advisor_id,
# MAGIC   h.asset_class,
# MAGIC   ROUND(
# MAGIC     SUM(h.market_value) / SUM(SUM(h.market_value)) OVER (PARTITION BY c.advisor_id) * 100,
# MAGIC     1
# MAGIC   ) AS pct_of_portfolio
# MAGIC FROM ahtsa.awm.holdings h
# MAGIC JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC JOIN ahtsa.awm.clients  c ON a.client_id  = c.client_id
# MAGIC GROUP BY c.advisor_id, h.asset_class
# MAGIC ORDER BY c.advisor_id, pct_of_portfolio DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `gold_app_performance_timeseries`
# MAGIC **Schema:** `advisor_id`, `date`, `portfolio_return`, `benchmark_return`
# MAGIC
# MAGIC Daily cumulative returns from `silver_advisor_daily_returns` — one row per advisor per trading day.
# MAGIC Both series are anchored to 0% on 2025-05-05. Returns are multiplied by 100 for display as percentages.
# MAGIC No month-end aggregation — the app renders the full daily series.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_performance_timeseries AS
# MAGIC SELECT
# MAGIC   advisor_id,
# MAGIC   date,
# MAGIC   ROUND(portfolio_return_before_fees * 100, 1) AS portfolio_return,
# MAGIC   ROUND(benchmark_return * 100, 1)             AS benchmark_return
# MAGIC FROM ahtsa.awm.silver_advisor_daily_returns
# MAGIC ORDER BY advisor_id, date

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. `gold_app_top_holdings`
# MAGIC **Schema:** `advisor_id`, `holding_id`, `name`, `asset_class`, `aum_millions`, `pct_of_portfolio`, `ytd_return`, `risk_flag`
# MAGIC
# MAGIC - Top 10 holdings **per advisor** by market value (CASH excluded)
# MAGIC - `pct_of_portfolio` — window partitioned by `advisor_id`
# MAGIC - `risk_flag` driven by `gold_unified_signals` in the last 30 days:
# MAGIC   `alert` ≥ 0.8 severity · `watch` ≥ 0.5 · `none` otherwise
# MAGIC - `ytd_return` — price return from first trading day of the current calendar year

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_top_holdings AS
# MAGIC WITH adv_holdings AS (
# MAGIC   SELECT
# MAGIC     c.advisor_id,
# MAGIC     h.ticker,
# MAGIC     ANY_VALUE(h.asset_class)                                                        AS asset_class,
# MAGIC     SUM(h.market_value)                                                             AS total_mv,
# MAGIC     ROUND(SUM(h.market_value) / SUM(SUM(h.market_value)) OVER (PARTITION BY c.advisor_id) * 100, 1) AS pct_of_portfolio
# MAGIC   FROM ahtsa.awm.holdings  h
# MAGIC   JOIN ahtsa.awm.accounts  a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients   c ON a.client_id  = c.client_id
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC   GROUP BY c.advisor_id, h.ticker
# MAGIC ),
# MAGIC ytd_start AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC     AND date >= DATE_TRUNC('year', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC ytd_end AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC ytd_returns AS (
# MAGIC   SELECT s.symbol, ROUND((e.end_price / s.start_price - 1) * 100, 1) AS ytd_return
# MAGIC   FROM ytd_start s JOIN ytd_end e ON s.symbol = e.symbol
# MAGIC ),
# MAGIC signals AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     CASE
# MAGIC       WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.8 THEN 'alert'
# MAGIC       WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.5 THEN 'watch'
# MAGIC       ELSE 'none'
# MAGIC     END AS risk_flag
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE signal_date >= DATE_SUB(CURRENT_DATE, 30)
# MAGIC     AND symbol IN (SELECT ticker FROM adv_holdings)
# MAGIC     AND source_type != 'news'
# MAGIC   GROUP BY symbol
# MAGIC )
# MAGIC SELECT
# MAGIC   t.advisor_id,
# MAGIC   t.ticker                            AS holding_id,
# MAGIC   COALESCE(cp.companyName, t.ticker)  AS name,
# MAGIC   t.asset_class,
# MAGIC   ROUND(t.total_mv / 1e6, 1)         AS aum_millions,
# MAGIC   t.pct_of_portfolio,
# MAGIC   COALESCE(yr.ytd_return, 0.0)        AS ytd_return,
# MAGIC   COALESCE(s.risk_flag, 'none')       AS risk_flag
# MAGIC FROM adv_holdings t
# MAGIC LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON t.ticker = cp.symbol
# MAGIC LEFT JOIN ytd_returns yr                        ON t.ticker = yr.symbol
# MAGIC LEFT JOIN signals s                             ON t.ticker = s.symbol
# MAGIC ORDER BY t.advisor_id, t.total_mv DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. `gold_app_concentration_risk`
# MAGIC **Schema:** `advisor_id`, `asset_class`, `client_name`, `delta_pct`
# MAGIC
# MAGIC IPS drift (actual − target) for the **top 5 clients by AUM per advisor**.
# MAGIC Positive `delta_pct` = overweight vs IPS target; negative = underweight.
# MAGIC Averaged across all accounts per client using `gold_ips_drift.drift_from_target_pct`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_concentration_risk AS
# MAGIC WITH ranked_clients AS (
# MAGIC   SELECT
# MAGIC     client_id,
# MAGIC     advisor_id,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY total_aum DESC) AS rn
# MAGIC   FROM ahtsa.awm.clients
# MAGIC ),
# MAGIC top_clients AS (
# MAGIC   SELECT client_id, advisor_id FROM ranked_clients WHERE rn <= 5
# MAGIC )
# MAGIC SELECT
# MAGIC   tc.advisor_id,
# MAGIC   d.asset_class,
# MAGIC   d.client_name,
# MAGIC   ROUND(AVG(d.drift_from_target_pct), 1) AS delta_pct
# MAGIC FROM ahtsa.awm.gold_ips_drift d
# MAGIC JOIN top_clients tc ON d.client_id = tc.client_id
# MAGIC GROUP BY tc.advisor_id, d.asset_class, d.client_name
# MAGIC ORDER BY tc.advisor_id, d.asset_class, delta_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ahtsa.awm.gold_app_concentration_risk
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Gold table | Key source tables | Notes |
# MAGIC |---|---|---|
# MAGIC | `gold_app_portfolio_summary` | `clients`, `silver_advisor_daily_returns`, `gold_ips_drift`, `accounts` | Latest alpha from silver; COALESCE 0 for advisors with no drift/risk |
# MAGIC | `gold_app_asset_allocation` | `holdings → accounts → clients` | Window SUM partitioned by `advisor_id` |
# MAGIC | `gold_app_performance_timeseries` | `silver_advisor_daily_returns` | All trading days; no month-end aggregation |
# MAGIC | `gold_app_top_holdings` | `holdings`, `bronze_historical_prices`, `gold_unified_signals`, `bronze_company_profiles` | Top-10 per advisor; risk_flag from last-30-day signals |
# MAGIC | `gold_app_concentration_risk` | `gold_ips_drift`, `clients` | Top-5 clients by AUM per advisor |
# MAGIC
# MAGIC App SQL files (`config/queries/*.sql`) query these tables with `WHERE advisor_id = :advisor_id`.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH blackstone_rows AS (
# MAGIC   SELECT 'EBITDA'             AS kpi_name, '$242M' AS prior_value, '$228M' AS current_value, -5.8  AS change_pct, 'down'  AS flag, 0.0  AS covenant_value UNION ALL
# MAGIC   SELECT 'Covenant Headroom',               '0.7x',                '0.3x',                  -57.1,              'alert',           0.3               UNION ALL
# MAGIC   SELECT 'Leverage Ratio',                  '4.2x',                '4.8x',                   14.3,              'down',            0.0               UNION ALL
# MAGIC   SELECT 'Revenue Growth',                  '+12%',                '+7%',                   -41.7,              'down',            0.0               UNION ALL
# MAGIC   SELECT 'Interest Coverage',               '3.1x',                '2.4x',                  -22.6,              'alert',           0.0
# MAGIC ),
# MAGIC default_rows AS (
# MAGIC   SELECT 'EBITDA'             AS kpi_name, '$312M' AS prior_value, '$328M' AS current_value,  5.1  AS change_pct, 'up'    AS flag, 0.0  AS covenant_value UNION ALL
# MAGIC   SELECT 'Leverage Ratio',                  '3.8x',                '3.6x',                   -5.3,              'up',              0.0               UNION ALL
# MAGIC   SELECT 'Revenue Growth',                  '+8%',                 '+11%',                   37.5,              'up',              0.0               UNION ALL
# MAGIC   SELECT 'Interest Coverage',               '4.2x',                '4.5x',                    7.1,              'up',              0.0               UNION ALL
# MAGIC   SELECT 'Net Debt/EBITDA',                 '3.2x',                '3.1x',                   -3.1,              'up',              0.0
# MAGIC )
# MAGIC SELECT kpi_name, prior_value, current_value, change_pct, flag, covenant_value
# MAGIC FROM blackstone_rows
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## gold_app_holdings_list
# MAGIC One row per advisor × ticker. `has_alert` = true when the ticker has any signal with
# MAGIC `advisor_action_needed = true` AND `sentiment = 'Negative'` in the last two quarters (~6 months).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_holdings_list AS
# MAGIC WITH latest_date AS (
# MAGIC   SELECT MAX(date) AS max_date FROM ahtsa.awm.holdings
# MAGIC ),
# MAGIC advisor_holdings AS (
# MAGIC   -- Distinct ticker per advisor from the most recent holdings snapshot, excluding cash
# MAGIC   SELECT DISTINCT
# MAGIC     c.advisor_id,
# MAGIC     h.ticker,
# MAGIC     h.asset_class
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts  a  ON a.account_id = h.account_id
# MAGIC   JOIN ahtsa.awm.clients   c  ON c.client_id  = a.client_id
# MAGIC   JOIN latest_date         ld ON h.date        = ld.max_date
# MAGIC   WHERE h.ticker != 'CASH'
# MAGIC ),
# MAGIC alert_tickers AS (
# MAGIC   -- Tickers flagged: advisor_action_needed + Negative sentiment within last 2 quarters
# MAGIC   SELECT DISTINCT symbol
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE advisor_action_needed = true
# MAGIC     AND LOWER(sentiment) = 'negative'
# MAGIC     AND source_type != 'news'
# MAGIC     AND signal_date >= ADD_MONTHS(CURRENT_DATE(), -6)
# MAGIC )
# MAGIC SELECT
# MAGIC   ah.advisor_id,
# MAGIC   ah.ticker                                                        AS holding_id,
# MAGIC   COALESCE(p.companyName, ah.ticker)                              AS name,
# MAGIC   ah.asset_class,
# MAGIC   COALESCE(NULLIF(p.sector, ''), NULLIF(p.industry, ''), ah.asset_class) AS strategy,
# MAGIC   CASE WHEN al.symbol IS NOT NULL THEN true ELSE false END        AS has_alert
# MAGIC FROM advisor_holdings ah
# MAGIC LEFT JOIN ahtsa.awm.bronze_company_profiles p  ON p.symbol = ah.ticker
# MAGIC LEFT JOIN alert_tickers                     al ON al.symbol = ah.ticker
# MAGIC ORDER BY ah.advisor_id, has_alert DESC, ah.asset_class, ah.ticker
# MAGIC
