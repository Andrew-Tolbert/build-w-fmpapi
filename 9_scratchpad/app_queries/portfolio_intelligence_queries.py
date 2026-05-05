# Databricks notebook source


# COMMAND ----------

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
# MAGIC top10 AS (
# MAGIC   SELECT * FROM adv_holdings
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY advisor_id ORDER BY total_mv DESC) <= 10
# MAGIC ),
# MAGIC ytd_start AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM top10)
# MAGIC     AND date >= DATE_TRUNC('year', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC ytd_end AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM top10)
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
# MAGIC     AND symbol IN (SELECT ticker FROM top10)
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
# MAGIC FROM top10 t
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
