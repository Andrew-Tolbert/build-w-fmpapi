# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # AWM Advisor App — Portfolio Intelligence Queries
# MAGIC
# MAGIC Real SQL queries for the **Portfolio Intelligence** page of the GS AWM Advisor App.
# MAGIC Each query targets `ahtsa.awm` and is filtered by `:advisor_id`.
# MAGIC
# MAGIC | Query | Page Widget | App file |
# MAGIC |---|---|---|
# MAGIC | `portfolio_summary` | 4 KPI stat cards | `config/queries/portfolio_summary.sql` |
# MAGIC | `asset_allocation` | Asset Allocation donut chart | `config/queries/asset_allocation.sql` |
# MAGIC | `performance_timeseries` | Performance vs Benchmark area chart | `config/queries/performance_timeseries.sql` |
# MAGIC | `top_holdings` | Top 10 Holdings table | `config/queries/top_holdings.sql` |
# MAGIC | `concentration_risk` | Client Concentration Risk heatmap | `config/queries/concentration_risk.sql` |
# MAGIC
# MAGIC **Note:** In the notebook, use `${advisor_id}` widget syntax. In the exported `.sql` files, replace with `:advisor_id` (AppKit parameterized query syntax).

# COMMAND ----------

dbutils.widgets.text("advisor_id", "ADV002", "Advisor ID")
advisor_id = dbutils.widgets.get("advisor_id")
print(f"Running queries for advisor: {advisor_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. `portfolio_summary`
# MAGIC **Schema:** `total_aum_billions`, `perf_vs_bench_pct`, `drift_count`, `clients_at_risk`
# MAGIC
# MAGIC - **total_aum_billions** — SUM of `clients.total_aum` / 1B
# MAGIC - **perf_vs_bench_pct** — YTD weighted portfolio return minus S&P 500 (GSPC) YTD return
# MAGIC - **drift_count** — count of account × asset_class slots outside IPS band
# MAGIC - **clients_at_risk** — distinct clients with at least one `Critical` drift breach

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH
# MAGIC advisor_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
# MAGIC   WHERE c.advisor_id = '${advisor_id}' AND h.ticker != 'CASH'
# MAGIC ),
# MAGIC advisor_holdings AS (
# MAGIC   SELECT h.ticker, SUM(h.market_value) AS total_mv
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
# MAGIC   WHERE c.advisor_id = '${advisor_id}' AND h.ticker != 'CASH'
# MAGIC   GROUP BY h.ticker
# MAGIC ),
# MAGIC grand_total AS (SELECT SUM(total_mv) AS gt FROM advisor_holdings),
# MAGIC ytd_start AS (
# MAGIC   SELECT symbol, adjClose AS start_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM advisor_tickers)
# MAGIC     AND date >= DATE_TRUNC('year', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC ytd_end AS (
# MAGIC   SELECT symbol, adjClose AS end_price
# MAGIC   FROM ahtsa.awm.bronze_historical_prices
# MAGIC   WHERE symbol IN (SELECT ticker FROM advisor_tickers)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC portfolio_ytd AS (
# MAGIC   SELECT SUM((e.end_price / s.start_price - 1) * ah.total_mv / gt.gt) * 100 AS ytd_return
# MAGIC   FROM ytd_start s
# MAGIC   JOIN ytd_end e ON s.symbol = e.symbol
# MAGIC   JOIN advisor_holdings ah ON s.symbol = ah.ticker
# MAGIC   CROSS JOIN grand_total gt
# MAGIC ),
# MAGIC bench_start AS (
# MAGIC   SELECT close AS start_close
# MAGIC   FROM ahtsa.awm.bronze_indexes_and_vix
# MAGIC   WHERE symbol = 'GSPC' AND date >= DATE_TRUNC('year', CURRENT_DATE)
# MAGIC   QUALIFY ROW_NUMBER() OVER (ORDER BY date ASC) = 1
# MAGIC ),
# MAGIC bench_end AS (
# MAGIC   SELECT close AS end_close
# MAGIC   FROM ahtsa.awm.bronze_indexes_and_vix
# MAGIC   WHERE symbol = 'GSPC'
# MAGIC   QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
# MAGIC ),
# MAGIC bench_ytd AS (
# MAGIC   SELECT (e.end_close / s.start_close - 1) * 100 AS bench_return
# MAGIC   FROM bench_start s CROSS JOIN bench_end e
# MAGIC ),
# MAGIC drift AS (
# MAGIC   SELECT COUNT(*) AS drift_count
# MAGIC   FROM ahtsa.awm.gold_ips_drift
# MAGIC   WHERE advisor_id = '${advisor_id}' AND drift_status != 'Within Band'
# MAGIC ),
# MAGIC at_risk AS (
# MAGIC   SELECT COUNT(DISTINCT a.client_id) AS clients_at_risk
# MAGIC   FROM ahtsa.awm.gold_ips_drift d
# MAGIC   JOIN ahtsa.awm.accounts a ON d.account_id = a.account_id
# MAGIC   WHERE d.advisor_id = '${advisor_id}' AND d.drift_severity = 'Critical'
# MAGIC ),
# MAGIC aum AS (
# MAGIC   SELECT ROUND(SUM(total_aum) / 1e9, 2) AS total_aum_billions
# MAGIC   FROM ahtsa.awm.clients
# MAGIC   WHERE advisor_id = '${advisor_id}'
# MAGIC )
# MAGIC SELECT
# MAGIC   a.total_aum_billions,
# MAGIC   ROUND(p.ytd_return - b.bench_return, 1) AS perf_vs_bench_pct,
# MAGIC   d.drift_count,
# MAGIC   ar.clients_at_risk
# MAGIC FROM aum a
# MAGIC CROSS JOIN portfolio_ytd p
# MAGIC CROSS JOIN bench_ytd b
# MAGIC CROSS JOIN drift d
# MAGIC CROSS JOIN at_risk ar

# COMMAND ----------

# Validation: portfolio_summary
df = spark.sql(f"""
WITH
advisor_tickers AS (
  SELECT DISTINCT h.ticker
  FROM ahtsa.awm.holdings h
  JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
  JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
  WHERE c.advisor_id = '{advisor_id}' AND h.ticker != 'CASH'
),
advisor_holdings AS (
  SELECT h.ticker, SUM(h.market_value) AS total_mv
  FROM ahtsa.awm.holdings h
  JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
  JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
  WHERE c.advisor_id = '{advisor_id}' AND h.ticker != 'CASH'
  GROUP BY h.ticker
),
grand_total AS (SELECT SUM(total_mv) AS gt FROM advisor_holdings),
ytd_start AS (
  SELECT symbol, adjClose AS start_price
  FROM ahtsa.awm.bronze_historical_prices
  WHERE symbol IN (SELECT ticker FROM advisor_tickers)
    AND date >= DATE_TRUNC('year', CURRENT_DATE)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
),
ytd_end AS (
  SELECT symbol, adjClose AS end_price
  FROM ahtsa.awm.bronze_historical_prices
  WHERE symbol IN (SELECT ticker FROM advisor_tickers)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
portfolio_ytd AS (
  SELECT SUM((e.end_price / s.start_price - 1) * ah.total_mv / gt.gt) * 100 AS ytd_return
  FROM ytd_start s
  JOIN ytd_end e ON s.symbol = e.symbol
  JOIN advisor_holdings ah ON s.symbol = ah.ticker
  CROSS JOIN grand_total gt
),
bench_start AS (
  SELECT close AS start_close
  FROM ahtsa.awm.bronze_indexes_and_vix
  WHERE symbol = 'GSPC' AND date >= DATE_TRUNC('year', CURRENT_DATE)
  QUALIFY ROW_NUMBER() OVER (ORDER BY date ASC) = 1
),
bench_end AS (
  SELECT close AS end_close
  FROM ahtsa.awm.bronze_indexes_and_vix
  WHERE symbol = 'GSPC'
  QUALIFY ROW_NUMBER() OVER (ORDER BY date DESC) = 1
),
bench_ytd AS (
  SELECT (e.end_close / s.start_close - 1) * 100 AS bench_return
  FROM bench_start s CROSS JOIN bench_end e
),
drift AS (
  SELECT COUNT(*) AS drift_count
  FROM ahtsa.awm.gold_ips_drift
  WHERE advisor_id = '{advisor_id}' AND drift_status != 'Within Band'
),
at_risk AS (
  SELECT COUNT(DISTINCT a.client_id) AS clients_at_risk
  FROM ahtsa.awm.gold_ips_drift d
  JOIN ahtsa.awm.accounts a ON d.account_id = a.account_id
  WHERE d.advisor_id = '{advisor_id}' AND d.drift_severity = 'Critical'
),
aum AS (
  SELECT ROUND(SUM(total_aum) / 1e9, 2) AS total_aum_billions
  FROM ahtsa.awm.clients
  WHERE advisor_id = '{advisor_id}'
)
SELECT
  a.total_aum_billions,
  ROUND(p.ytd_return - b.bench_return, 1) AS perf_vs_bench_pct,
  d.drift_count,
  ar.clients_at_risk
FROM aum a
CROSS JOIN portfolio_ytd p
CROSS JOIN bench_ytd b
CROSS JOIN drift d
CROSS JOIN at_risk ar
""")
row = df.collect()[0]
checks = [
  ("row count == 1",             df.count() == 1),
  ("total_aum_billions > 0",     row["total_aum_billions"] > 0),
  ("total_aum_billions < 1000",  row["total_aum_billions"] < 1000),
  ("drift_count >= 0",           row["drift_count"] >= 0),
  ("clients_at_risk >= 0",       row["clients_at_risk"] >= 0),
]
all_pass = True
for label, result in checks:
  status = "PASS" if result else "FAIL"
  if not result: all_pass = False
  print(f"  [{status}] {label}")
print(f"\nportfolio_summary: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
print(f"  Values: AUM={row['total_aum_billions']}B, alpha={row['perf_vs_bench_pct']}%, drift={row['drift_count']}, at_risk={row['clients_at_risk']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. `asset_allocation`
# MAGIC **Schema:** `asset_class`, `pct_of_portfolio`
# MAGIC
# MAGIC Weighted allocation of the advisor's book by asset class, derived from current `holdings`.
# MAGIC The window function computes the percentage within the same SELECT so no subquery is needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   h.asset_class,
# MAGIC   ROUND(
# MAGIC     SUM(h.market_value) / SUM(SUM(h.market_value)) OVER () * 100,
# MAGIC     1
# MAGIC   ) AS pct_of_portfolio
# MAGIC FROM ahtsa.awm.holdings h
# MAGIC JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
# MAGIC WHERE c.advisor_id = '${advisor_id}'
# MAGIC GROUP BY h.asset_class
# MAGIC ORDER BY pct_of_portfolio DESC

# COMMAND ----------

# Validation: asset_allocation
df = spark.sql(f"""
SELECT
  h.asset_class,
  ROUND(SUM(h.market_value) / SUM(SUM(h.market_value)) OVER () * 100, 1) AS pct_of_portfolio
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
WHERE c.advisor_id = '{advisor_id}'
GROUP BY h.asset_class
ORDER BY pct_of_portfolio DESC
""")
rows = df.collect()
total_pct = sum(r["pct_of_portfolio"] for r in rows)
checks = [
  ("at least 2 asset classes",     len(rows) >= 2),
  ("pct sums to ~100",             abs(total_pct - 100.0) < 1.0),
  ("no negative pct",              all(r["pct_of_portfolio"] >= 0 for r in rows)),
  ("largest class < 90%",          rows[0]["pct_of_portfolio"] < 90),
]
all_pass = True
for label, result in checks:
  status = "PASS" if result else "FAIL"
  if not result: all_pass = False
  print(f"  [{status}] {label}")
print(f"\nasset_allocation: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
print(f"  Breakdown: { {r['asset_class']: r['pct_of_portfolio'] for r in rows} }")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. `performance_timeseries`
# MAGIC **Schema:** `month`, `portfolio_return`, `benchmark_return`
# MAGIC
# MAGIC - 13 months of data: the earliest month anchors both series at 0%, giving 12 deltas.
# MAGIC - Portfolio return = weighted average of constituent ticker cumulative returns (current-weight approximation).
# MAGIC - Benchmark = S&P 500 (`GSPC`) cumulative return from the same base month.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH
# MAGIC months AS (
# MAGIC   SELECT ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE), -n) AS month_start
# MAGIC   FROM (SELECT EXPLODE(SEQUENCE(0, 12)) AS n)
# MAGIC ),
# MAGIC advisor_holdings AS (
# MAGIC   SELECT h.ticker, SUM(h.market_value) AS total_mv
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
# MAGIC   WHERE c.advisor_id = '${advisor_id}' AND h.ticker != 'CASH'
# MAGIC   GROUP BY h.ticker
# MAGIC ),
# MAGIC grand_total AS (SELECT SUM(total_mv) AS gt FROM advisor_holdings),
# MAGIC weights AS (
# MAGIC   SELECT ticker, total_mv / gt AS weight FROM advisor_holdings CROSS JOIN grand_total
# MAGIC ),
# MAGIC monthly_last_prices AS (
# MAGIC   SELECT p.symbol, DATE_TRUNC('month', p.date) AS month_start, p.adjClose
# MAGIC   FROM ahtsa.awm.bronze_historical_prices p
# MAGIC   JOIN months m ON DATE_TRUNC('month', p.date) = m.month_start
# MAGIC   WHERE p.symbol IN (SELECT ticker FROM advisor_holdings)
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol, DATE_TRUNC('month', p.date) ORDER BY p.date DESC) = 1
# MAGIC ),
# MAGIC base_prices AS (
# MAGIC   SELECT symbol, adjClose AS base_price
# MAGIC   FROM monthly_last_prices
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY month_start ASC) = 1
# MAGIC ),
# MAGIC holding_returns AS (
# MAGIC   SELECT mp.month_start, mp.symbol,
# MAGIC     (mp.adjClose / bp.base_price - 1) * 100 AS cum_return
# MAGIC   FROM monthly_last_prices mp
# MAGIC   JOIN base_prices bp ON mp.symbol = bp.symbol
# MAGIC ),
# MAGIC portfolio_monthly AS (
# MAGIC   SELECT hr.month_start, SUM(hr.cum_return * w.weight) AS portfolio_return
# MAGIC   FROM holding_returns hr
# MAGIC   JOIN weights w ON hr.symbol = w.ticker
# MAGIC   GROUP BY hr.month_start
# MAGIC ),
# MAGIC bench_monthly AS (
# MAGIC   SELECT DATE_TRUNC('month', b.date) AS month_start, b.close
# MAGIC   FROM ahtsa.awm.bronze_indexes_and_vix b
# MAGIC   JOIN months m ON DATE_TRUNC('month', b.date) = m.month_start
# MAGIC   WHERE b.symbol = 'GSPC'
# MAGIC   QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', b.date) ORDER BY b.date DESC) = 1
# MAGIC ),
# MAGIC bench_base AS (
# MAGIC   SELECT close AS base_close
# MAGIC   FROM bench_monthly
# MAGIC   QUALIFY ROW_NUMBER() OVER (ORDER BY month_start ASC) = 1
# MAGIC ),
# MAGIC bench_returns AS (
# MAGIC   SELECT bm.month_start, (bm.close / bb.base_close - 1) * 100 AS benchmark_return
# MAGIC   FROM bench_monthly bm CROSS JOIN bench_base bb
# MAGIC )
# MAGIC SELECT
# MAGIC   DATE_FORMAT(pm.month_start, 'MMM yy') AS month,
# MAGIC   ROUND(pm.portfolio_return, 1) AS portfolio_return,
# MAGIC   ROUND(br.benchmark_return, 1) AS benchmark_return
# MAGIC FROM portfolio_monthly pm
# MAGIC JOIN bench_returns br ON pm.month_start = br.month_start
# MAGIC ORDER BY pm.month_start

# COMMAND ----------

# Validation: performance_timeseries
df = spark.sql(f"""
WITH
months AS (
  SELECT ADD_MONTHS(DATE_TRUNC('month', CURRENT_DATE), -n) AS month_start
  FROM (SELECT EXPLODE(SEQUENCE(0, 12)) AS n)
),
advisor_holdings AS (
  SELECT h.ticker, SUM(h.market_value) AS total_mv
  FROM ahtsa.awm.holdings h
  JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
  JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
  WHERE c.advisor_id = '{advisor_id}' AND h.ticker != 'CASH'
  GROUP BY h.ticker
),
grand_total AS (SELECT SUM(total_mv) AS gt FROM advisor_holdings),
weights AS (SELECT ticker, total_mv / gt AS weight FROM advisor_holdings CROSS JOIN grand_total),
monthly_last_prices AS (
  SELECT p.symbol, DATE_TRUNC('month', p.date) AS month_start, p.adjClose
  FROM ahtsa.awm.bronze_historical_prices p
  JOIN months m ON DATE_TRUNC('month', p.date) = m.month_start
  WHERE p.symbol IN (SELECT ticker FROM advisor_holdings)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.symbol, DATE_TRUNC('month', p.date) ORDER BY p.date DESC) = 1
),
base_prices AS (
  SELECT symbol, adjClose AS base_price FROM monthly_last_prices
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY month_start ASC) = 1
),
holding_returns AS (
  SELECT mp.month_start, mp.symbol, (mp.adjClose / bp.base_price - 1) * 100 AS cum_return
  FROM monthly_last_prices mp JOIN base_prices bp ON mp.symbol = bp.symbol
),
portfolio_monthly AS (
  SELECT hr.month_start, SUM(hr.cum_return * w.weight) AS portfolio_return
  FROM holding_returns hr JOIN weights w ON hr.symbol = w.ticker
  GROUP BY hr.month_start
),
bench_monthly AS (
  SELECT DATE_TRUNC('month', b.date) AS month_start, b.close
  FROM ahtsa.awm.bronze_indexes_and_vix b
  JOIN months m ON DATE_TRUNC('month', b.date) = m.month_start
  WHERE b.symbol = 'GSPC'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', b.date) ORDER BY b.date DESC) = 1
),
bench_base AS (
  SELECT close AS base_close FROM bench_monthly
  QUALIFY ROW_NUMBER() OVER (ORDER BY month_start ASC) = 1
),
bench_returns AS (
  SELECT bm.month_start, (bm.close / bb.base_close - 1) * 100 AS benchmark_return
  FROM bench_monthly bm CROSS JOIN bench_base bb
)
SELECT
  DATE_FORMAT(pm.month_start, 'MMM yy') AS month,
  ROUND(pm.portfolio_return, 1) AS portfolio_return,
  ROUND(br.benchmark_return, 1) AS benchmark_return
FROM portfolio_monthly pm
JOIN bench_returns br ON pm.month_start = br.month_start
ORDER BY pm.month_start
""")
rows = df.collect()
first = rows[0]
checks = [
  ("returns 13 months",                     len(rows) == 13),
  ("base month portfolio_return == 0.0",    first["portfolio_return"] == 0.0),
  ("base month benchmark_return == 0.0",    first["benchmark_return"] == 0.0),
  ("returns stay in -50% to +200% range",   all(-50 <= r["portfolio_return"] <= 200 for r in rows)),
  ("no null portfolio_return",              all(r["portfolio_return"] is not None for r in rows)),
]
all_pass = True
for label, result in checks:
  status = "PASS" if result else "FAIL"
  if not result: all_pass = False
  print(f"  [{status}] {label}")
print(f"\nperformance_timeseries: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
for r in rows:
  print(f"  {r['month']:>8}  portfolio={r['portfolio_return']:>6.1f}%  benchmark={r['benchmark_return']:>6.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. `top_holdings`
# MAGIC **Schema:** `holding_id`, `name`, `asset_class`, `aum_millions`, `pct_of_portfolio`, `ytd_return`, `risk_flag`
# MAGIC
# MAGIC - `risk_flag` derived from `gold_unified_signals` in the last 30 days:
# MAGIC   - `alert` = max advisor-action-needed signal severity ≥ 0.8
# MAGIC   - `watch` = max advisor-action-needed signal severity ≥ 0.5
# MAGIC   - `none` = no recent actionable signals
# MAGIC - `ytd_return` = price return since first trading day of current year.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH adv_holdings AS (
# MAGIC   SELECT
# MAGIC     h.ticker,
# MAGIC     ANY_VALUE(h.asset_class) AS asset_class,
# MAGIC     SUM(h.market_value)      AS total_mv,
# MAGIC     ROUND(
# MAGIC       SUM(h.market_value) / SUM(SUM(h.market_value)) OVER () * 100,
# MAGIC       1
# MAGIC     ) AS pct_of_portfolio
# MAGIC   FROM ahtsa.awm.holdings h
# MAGIC   JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
# MAGIC   JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
# MAGIC   WHERE c.advisor_id = '${advisor_id}' AND h.ticker != 'CASH'
# MAGIC   GROUP BY h.ticker
# MAGIC ),
# MAGIC top10 AS (
# MAGIC   SELECT * FROM adv_holdings
# MAGIC   QUALIFY ROW_NUMBER() OVER (ORDER BY total_mv DESC) <= 10
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
# MAGIC   t.ticker                                  AS holding_id,
# MAGIC   COALESCE(cp.companyName, t.ticker)        AS name,
# MAGIC   t.asset_class,
# MAGIC   ROUND(t.total_mv / 1e6, 1)               AS aum_millions,
# MAGIC   t.pct_of_portfolio,
# MAGIC   COALESCE(yr.ytd_return, 0.0)              AS ytd_return,
# MAGIC   COALESCE(s.risk_flag, 'none')             AS risk_flag
# MAGIC FROM top10 t
# MAGIC LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON t.ticker = cp.symbol
# MAGIC LEFT JOIN ytd_returns yr ON t.ticker = yr.symbol
# MAGIC LEFT JOIN signals s ON t.ticker = s.symbol
# MAGIC ORDER BY t.total_mv DESC

# COMMAND ----------

# Validation: top_holdings
df = spark.sql(f"""
WITH adv_holdings AS (
  SELECT
    h.ticker, ANY_VALUE(h.asset_class) AS asset_class,
    SUM(h.market_value) AS total_mv,
    ROUND(SUM(h.market_value) / SUM(SUM(h.market_value)) OVER () * 100, 1) AS pct_of_portfolio
  FROM ahtsa.awm.holdings h
  JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
  JOIN ahtsa.awm.clients c ON a.client_id = c.client_id
  WHERE c.advisor_id = '{advisor_id}' AND h.ticker != 'CASH'
  GROUP BY h.ticker
),
top10 AS (SELECT * FROM adv_holdings QUALIFY ROW_NUMBER() OVER (ORDER BY total_mv DESC) <= 10),
ytd_start AS (
  SELECT symbol, adjClose AS start_price FROM ahtsa.awm.bronze_historical_prices
  WHERE symbol IN (SELECT ticker FROM top10) AND date >= DATE_TRUNC('year', CURRENT_DATE)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date ASC) = 1
),
ytd_end AS (
  SELECT symbol, adjClose AS end_price FROM ahtsa.awm.bronze_historical_prices
  WHERE symbol IN (SELECT ticker FROM top10)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
ytd_returns AS (
  SELECT s.symbol, ROUND((e.end_price / s.start_price - 1) * 100, 1) AS ytd_return
  FROM ytd_start s JOIN ytd_end e ON s.symbol = e.symbol
),
signals AS (
  SELECT symbol,
    CASE
      WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.8 THEN 'alert'
      WHEN MAX(CASE WHEN advisor_action_needed THEN severity_score ELSE 0 END) >= 0.5 THEN 'watch'
      ELSE 'none'
    END AS risk_flag
  FROM ahtsa.awm.gold_unified_signals
  WHERE signal_date >= DATE_SUB(CURRENT_DATE, 30) AND symbol IN (SELECT ticker FROM top10)
  GROUP BY symbol
)
SELECT
  t.ticker AS holding_id, COALESCE(cp.companyName, t.ticker) AS name,
  t.asset_class, ROUND(t.total_mv / 1e6, 1) AS aum_millions,
  t.pct_of_portfolio, COALESCE(yr.ytd_return, 0.0) AS ytd_return,
  COALESCE(s.risk_flag, 'none') AS risk_flag
FROM top10 t
LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON t.ticker = cp.symbol
LEFT JOIN ytd_returns yr ON t.ticker = yr.symbol
LEFT JOIN signals s ON t.ticker = s.symbol
ORDER BY t.total_mv DESC
""")
rows = df.collect()
valid_flags = {"alert", "watch", "none"}
checks = [
  ("returns exactly 10 rows",           len(rows) == 10),
  ("all risk_flag values are valid",     all(r["risk_flag"] in valid_flags for r in rows)),
  ("all aum_millions > 0",              all(r["aum_millions"] > 0 for r in rows)),
  ("pct_of_portfolio sums to ~100",     abs(sum(r["pct_of_portfolio"] for r in rows) - 100.0) < 5.0),
  ("no null name",                      all(r["name"] is not None for r in rows)),
]
all_pass = True
for label, result in checks:
  status = "PASS" if result else "FAIL"
  if not result: all_pass = False
  print(f"  [{status}] {label}")
print(f"\ntop_holdings: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
for r in rows:
  print(f"  {r['holding_id']:>6}  {r['name'][:35]:<35}  {r['asset_class']:<16}  ${r['aum_millions']:>7.1f}M  {r['ytd_return']:>6.1f}% YTD  [{r['risk_flag']}]")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. `concentration_risk`
# MAGIC **Schema:** `asset_class`, `client_name`, `delta_pct`
# MAGIC
# MAGIC Shows IPS drift (actual allocation − IPS target) for the top 5 clients by AUM.
# MAGIC Positive `delta_pct` = overweight vs IPS target; negative = underweight.
# MAGIC Sourced from `gold_ips_drift.drift_from_target_pct`, averaged across all accounts per client.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH top_clients AS (
# MAGIC   SELECT client_id
# MAGIC   FROM ahtsa.awm.clients
# MAGIC   WHERE advisor_id = '${advisor_id}'
# MAGIC   ORDER BY total_aum DESC
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC SELECT
# MAGIC   d.asset_class,
# MAGIC   d.client_name,
# MAGIC   ROUND(AVG(d.drift_from_target_pct), 1) AS delta_pct
# MAGIC FROM ahtsa.awm.gold_ips_drift d
# MAGIC JOIN top_clients tc ON d.client_id = tc.client_id
# MAGIC GROUP BY d.asset_class, d.client_name
# MAGIC ORDER BY d.asset_class, delta_pct DESC

# COMMAND ----------

# Validation: concentration_risk
df = spark.sql(f"""
WITH top_clients AS (
  SELECT client_id FROM ahtsa.awm.clients
  WHERE advisor_id = '{advisor_id}'
  ORDER BY total_aum DESC
  LIMIT 5
)
SELECT
  d.asset_class,
  d.client_name,
  ROUND(AVG(d.drift_from_target_pct), 1) AS delta_pct
FROM ahtsa.awm.gold_ips_drift d
JOIN top_clients tc ON d.client_id = tc.client_id
GROUP BY d.asset_class, d.client_name
ORDER BY d.asset_class, delta_pct DESC
""")
rows = df.collect()
asset_classes = {r["asset_class"] for r in rows}
client_names  = {r["client_name"]  for r in rows}
checks = [
  ("at least 5 rows",                   len(rows) >= 5),
  ("at least 2 distinct asset classes", len(asset_classes) >= 2),
  ("at least 2 distinct clients",       len(client_names) >= 2),
  ("delta_pct in [-100, 100]",          all(-100 <= r["delta_pct"] <= 100 for r in rows)),
]
all_pass = True
for label, result in checks:
  status = "PASS" if result else "FAIL"
  if not result: all_pass = False
  print(f"  [{status}] {label}")
print(f"\nconcentration_risk: {'ALL PASS' if all_pass else 'FAILURES DETECTED'}")
print(f"  Asset classes: {sorted(asset_classes)}")
print(f"  Clients: {sorted(client_names)}")
for r in rows:
  bar = "+" * int(abs(r["delta_pct"])) if r["delta_pct"] > 0 else "-" * int(abs(r["delta_pct"]))
  print(f"  {r['asset_class']:<18} {r['client_name']:<30} {r['delta_pct']:>6.1f}%  {bar[:30]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC All queries above map 1:1 to files in `config/queries/`. When copying to the app:
# MAGIC - Replace `'${advisor_id}'` with `:advisor_id` (AppKit named parameter syntax)
# MAGIC - The app backend injects `:advisor_id` from the session context or a query param
# MAGIC
# MAGIC | File | Status |
# MAGIC |---|---|
# MAGIC | `portfolio_summary.sql` | Ready |
# MAGIC | `asset_allocation.sql` | Ready |
# MAGIC | `performance_timeseries.sql` | Ready |
# MAGIC | `top_holdings.sql` | Ready |
# MAGIC | `concentration_risk.sql` | Ready |

# COMMAND ----------

print("All Portfolio Intelligence queries complete.")
print(f"Advisor tested: {advisor_id}")
print("Next: copy each query body into config/queries/, replacing '${advisor_id}' with ':advisor_id'")

