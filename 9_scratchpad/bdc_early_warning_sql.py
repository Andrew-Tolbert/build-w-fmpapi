# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "edgartools",
# ]
# ///
# BDC Early Warning System — SQL-first implementation
#
# Based on: https://www.edgartools.io/building-a-bdc-early-warning-system-in-python/
#
# Approach:
#   1. Python fetches all XBRL time series via edgartools → two DataFrames
#   2. Both DataFrames registered as Spark temp views (no Delta writes needed)
#   3. All T1/T2/T3 signal calculations written as SQL
#
# Two views created:
#   bdc_time_series  — long format, every data point (ticker, cik, metric, period_end, ...)
#   bdc_fy_snapshot  — wide format, one row per ticker with latest FY value per metric
#
# ── WARNING SIGNAL FRAMEWORK ──────────────────────────────────────────────────
# T1: PIK/NII > 20% (watch) / 30% (concern)
# T1: Div coverage NII/div < 105% (watch) / 100% (concern)
# T2: NAV consecutive quarterly declines >= 3 (watch) / 6 (concern)
# T2: Unrealized depreciation/NAV > 40% (watch) / 55% (concern)
# T3: Realized loss YoY multiple > 2x (watch) / 4x (concern)
# T3: Cumulative per-share losses / current NAV > 50% (watch) / 100% (concern)

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

from edgar import Company, set_identity
import pandas as pd

set_identity("andrew.tolbert@databricks.com")

client      = FMPClient(api_key=FMP_API_KEY)
BDC_TICKERS = get_tickers(types=["private_credit"])
print(f"BDC tickers: {BDC_TICKERS}")

def get_company(ticker: str) -> Company:
    return Company(int(client.get_cik(ticker)))

CONCEPTS = {
    "pik":         "us-gaap:InterestIncomeOperatingPaidInKind",
    "nii":         "us-gaap:NetInvestmentIncome",
    "nii_ps":      "us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare",
    "div_ps":      "us-gaap:CommonStockDividendsPerShareDeclared",
    "nav_ps":      "us-gaap:NetAssetValuePerShare",
    "deprec":      "us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation",
    "net_assets":  "us-gaap:AssetsNet",
    "realized_gl": "us-gaap:RealizedInvestmentGainsLosses",
    "gl_ps":       "us-gaap:InvestmentCompanyGainLossOnInvestmentPerShare",
}

# COMMAND ----------

# ── Fetch all time series — one get_facts() call per ticker ────────────────────

rows = []
for ticker in BDC_TICKERS:
    try:
        cik  = client.get_cik(ticker)
        facts = get_company(ticker).get_facts()
        for metric, concept in CONCEPTS.items():
            ts = facts.time_series(concept)
            if ts is not None and not ts.empty:
                ts = ts[["period_end", "fiscal_period", "numeric_value"]].copy()
                ts["ticker"] = ticker
                ts["cik"]    = cik
                ts["metric"] = metric
                rows.append(ts)
        print(f"  {ticker} ({cik}): fetched")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

raw = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
    columns=["ticker", "cik", "metric", "period_end", "fiscal_period", "numeric_value"]
)
print(f"\nTotal rows: {len(raw)}")

# COMMAND ----------

# ── Build the two DataFrames and register as Spark temp views ──────────────────

# View 1: bdc_time_series — every data point, long format
bdc_time_series = raw[["ticker", "cik", "metric", "period_end", "fiscal_period", "numeric_value"]].copy()
bdc_time_series["period_end"] = pd.to_datetime(bdc_time_series["period_end"])

# View 2: bdc_fy_snapshot — latest FY value per ticker/metric, pivoted wide
bdc_fy_snapshot = (
    raw[raw["fiscal_period"] == "FY"]
    .sort_values("period_end")
    .drop_duplicates(subset=["ticker", "metric"], keep="last")
    .pivot(index=["ticker", "cik"], columns="metric", values="numeric_value")
    .reset_index()
)
# Flatten column names after pivot
bdc_fy_snapshot.columns.name = None

spark.createDataFrame(bdc_time_series).createOrReplaceTempView("bdc_time_series")
spark.createDataFrame(bdc_fy_snapshot).createOrReplaceTempView("bdc_fy_snapshot")

print("Temp views registered: bdc_time_series, bdc_fy_snapshot")
print(f"bdc_time_series: {len(bdc_time_series)} rows")
print(f"bdc_fy_snapshot: {len(bdc_fy_snapshot)} rows  |  columns: {list(bdc_fy_snapshot.columns)}")

# COMMAND ----------

# ── Inspect the raw data before any calculations ───────────────────────────────

# MAGIC %sql
SELECT ticker, cik, metric, COUNT(*) AS periods,
       MIN(period_end) AS earliest, MAX(period_end) AS latest
FROM   bdc_time_series
GROUP  BY ticker, cik, metric
ORDER  BY ticker, metric

# COMMAND ----------

# MAGIC %sql
-- FY snapshot — one row per ticker, all raw metric values side by side
SELECT *
FROM   bdc_fy_snapshot
ORDER  BY ticker

# COMMAND ----------

-- ── T1: PIK-to-NII Ratio ──────────────────────────────────────────────────────
-- Measures how much of net investment income is paid-in-kind (non-cash).
-- High PIK means borrowers can't service debt in cash — early stress signal.
-- Watch > 20%  |  Concern > 30%

-- MAGIC %sql
SELECT
    ticker,
    cik,
    ROUND(pik / nii * 100, 1)                             AS pik_nii_pct,
    CASE
        WHEN pik / nii >= 0.30 THEN '🔴 CONCERN'
        WHEN pik / nii >= 0.20 THEN '🟡 WATCH'
        ELSE                        '🟢 OK'
    END                                                    AS pik_nii_signal
FROM  bdc_fy_snapshot
WHERE pik IS NOT NULL
  AND nii IS NOT NULL
  AND nii <> 0
ORDER BY pik / nii DESC

# COMMAND ----------

-- ── T1: Dividend Coverage ─────────────────────────────────────────────────────
-- NII per share / dividends declared per share.
-- Below 1.0x means dividends exceed earnings — unsustainable without NAV erosion.
-- Watch < 105%  |  Concern < 100%

-- MAGIC %sql
SELECT
    ticker,
    cik,
    ROUND(nii_ps / div_ps * 100, 1)                       AS div_coverage_pct,
    CASE
        WHEN nii_ps / div_ps <= 1.00 THEN '🔴 CONCERN'
        WHEN nii_ps / div_ps <= 1.05 THEN '🟡 WATCH'
        ELSE                               '🟢 OK'
    END                                                    AS div_coverage_signal
FROM  bdc_fy_snapshot
WHERE nii_ps IS NOT NULL
  AND div_ps  IS NOT NULL
  AND div_ps <> 0
ORDER BY nii_ps / div_ps ASC

# COMMAND ----------

-- ── T2: NAV Trajectory — consecutive quarterly declines ───────────────────────
-- Persistent NAV decline means the portfolio is losing value faster than earnings.
-- Watch >= 3 quarters  |  Concern >= 6 quarters
--
-- Approach: use LAG() to flag each period as a decline, then find the longest
-- streak ending at the most recent observation using a gap-and-islands pattern.

-- MAGIC %sql
WITH lagged AS (
    SELECT
        ticker, cik, period_end,
        numeric_value                                                        AS nav,
        LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end)   AS prev_nav
    FROM bdc_time_series
    WHERE metric = 'nav_ps'
),
flagged AS (
    SELECT *,
        CASE WHEN nav < prev_nav THEN 1 ELSE 0 END AS is_decline,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC)    AS rn
    FROM lagged
    WHERE prev_nav IS NOT NULL
),
-- Find where the streak breaks (first non-decline from the end)
streak AS (
    SELECT ticker, cik,
        -- Sum of consecutive declines from rn=1 until first non-decline
        SUM(is_decline) OVER (
            PARTITION BY ticker
            ORDER BY rn
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                                    AS running_sum,
        is_decline,
        rn
    FROM flagged
),
-- The consecutive streak = running_sum at the point is_decline first becomes 0
consec AS (
    SELECT ticker, cik,
        COALESCE(
            MIN(CASE WHEN is_decline = 0 THEN running_sum - 1 END),
            MAX(running_sum)
        ) AS consecutive_declines
    FROM streak
    GROUP BY ticker, cik
)
SELECT
    ticker,
    cik,
    consecutive_declines,
    CASE
        WHEN consecutive_declines >= 6 THEN '🔴 CONCERN'
        WHEN consecutive_declines >= 3 THEN '🟡 WATCH'
        ELSE                                 '🟢 OK'
    END AS nav_trend_signal
FROM consec
ORDER BY consecutive_declines DESC

# COMMAND ----------

-- ── T2: Unrealized Depreciation / Net Assets ─────────────────────────────────
-- Rising unrealized losses as % of NAV confirms portfolio deterioration.
-- Watch > 40%  |  Concern > 55%

-- MAGIC %sql
SELECT
    ticker,
    cik,
    ROUND(deprec / net_assets * 100, 1)                   AS deprec_pct_nav,
    CASE
        WHEN deprec / net_assets >= 0.55 THEN '🔴 CONCERN'
        WHEN deprec / net_assets >= 0.40 THEN '🟡 WATCH'
        ELSE                                   '🟢 OK'
    END                                                    AS deprec_signal
FROM  bdc_fy_snapshot
WHERE deprec      IS NOT NULL
  AND net_assets  IS NOT NULL
  AND net_assets <> 0
ORDER BY deprec / net_assets DESC

# COMMAND ----------

-- ── T3: Realized Loss Acceleration ───────────────────────────────────────────
-- YoY growth in realized losses signals the portfolio is crystallizing stress.
-- Watch > 2x prior year  |  Concern > 4x prior year

-- MAGIC %sql
WITH fy_losses AS (
    SELECT
        ticker, cik, period_end,
        -- Flip sign: losses are negative, we want positive magnitude
        ABS(LEAST(numeric_value, 0))                                            AS loss_abs,
        LAG(ABS(LEAST(numeric_value, 0))) OVER (
            PARTITION BY ticker ORDER BY period_end
        )                                                                       AS prior_loss_abs
    FROM bdc_time_series
    WHERE metric = 'realized_gl'
      AND fiscal_period = 'FY'
),
latest AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
    FROM fy_losses
    WHERE prior_loss_abs IS NOT NULL AND prior_loss_abs > 0
)
SELECT
    ticker,
    cik,
    period_end,
    ROUND(loss_abs, 0)                                     AS realized_loss,
    ROUND(prior_loss_abs, 0)                               AS prior_year_loss,
    ROUND(loss_abs / prior_loss_abs, 2)                    AS yoy_multiple,
    CASE
        WHEN loss_abs / prior_loss_abs >= 4.0 THEN '🔴 CONCERN'
        WHEN loss_abs / prior_loss_abs >= 2.0 THEN '🟡 WATCH'
        ELSE                                        '🟢 OK'
    END                                                    AS rl_accel_signal
FROM latest
WHERE rn = 1
ORDER BY yoy_multiple DESC

# COMMAND ----------

-- ── T3: Cumulative Losses vs. Current NAV/share ───────────────────────────────
-- Cumulative realized per-share losses as % of current NAV.
-- When this exceeds 100% the portfolio has lost more than its current book value.
-- Watch > 50%  |  Concern > 100%

-- MAGIC %sql
WITH cum_losses AS (
    SELECT
        ticker, cik,
        -- Sum only negative (loss) periods
        ABS(SUM(LEAST(numeric_value, 0)))   AS cumulative_loss_ps
    FROM bdc_time_series
    WHERE metric       = 'gl_ps'
      AND fiscal_period = 'FY'
    GROUP BY ticker, cik
)
SELECT
    c.ticker,
    c.cik,
    ROUND(c.cumulative_loss_ps, 4)                         AS cum_loss_ps,
    s.nav_ps                                               AS current_nav_ps,
    ROUND(c.cumulative_loss_ps / s.nav_ps * 100, 1)        AS cum_loss_pct_nav,
    CASE
        WHEN c.cumulative_loss_ps / s.nav_ps >= 1.00 THEN '🔴 CONCERN'
        WHEN c.cumulative_loss_ps / s.nav_ps >= 0.50 THEN '🟡 WATCH'
        ELSE                                               '🟢 OK'
    END                                                    AS cum_loss_signal
FROM      cum_losses c
JOIN      bdc_fy_snapshot s USING (ticker, cik)
WHERE     s.nav_ps IS NOT NULL AND s.nav_ps > 0
ORDER BY  cum_loss_pct_nav DESC

# COMMAND ----------

-- ── Full dashboard — all signals joined ───────────────────────────────────────

-- MAGIC %sql
WITH t1_pik AS (
    SELECT ticker, cik,
        ROUND(pik / nii * 100, 1) AS pik_nii_pct,
        CASE WHEN pik/nii >= 0.30 THEN '🔴' WHEN pik/nii >= 0.20 THEN '🟡' ELSE '🟢' END AS t1_pik
    FROM bdc_fy_snapshot WHERE pik IS NOT NULL AND nii IS NOT NULL AND nii <> 0
),
t1_div AS (
    SELECT ticker, cik,
        ROUND(nii_ps / div_ps * 100, 1) AS div_coverage_pct,
        CASE WHEN nii_ps/div_ps <= 1.00 THEN '🔴' WHEN nii_ps/div_ps <= 1.05 THEN '🟡' ELSE '🟢' END AS t1_div
    FROM bdc_fy_snapshot WHERE nii_ps IS NOT NULL AND div_ps IS NOT NULL AND div_ps <> 0
),
t2_nav AS (
    WITH lagged AS (
        SELECT ticker, cik, period_end, numeric_value AS nav,
            LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav
        FROM bdc_time_series WHERE metric = 'nav_ps'
    ),
    flagged AS (
        SELECT ticker, cik,
            SUM(CASE WHEN nav < prev_nav THEN 1 ELSE 0 END) OVER (
                PARTITION BY ticker ORDER BY period_end DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_declines,
            CASE WHEN nav >= prev_nav THEN 1 ELSE 0 END AS streak_break,
            ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
        FROM lagged WHERE prev_nav IS NOT NULL
    )
    SELECT ticker, cik,
        MIN(CASE WHEN streak_break = 1 THEN running_declines - 1 END) AS consec_declines,
        CASE WHEN MIN(CASE WHEN streak_break=1 THEN running_declines-1 END) >= 6 THEN '🔴'
             WHEN MIN(CASE WHEN streak_break=1 THEN running_declines-1 END) >= 3 THEN '🟡'
             ELSE '🟢' END AS t2_nav
    FROM flagged GROUP BY ticker, cik
),
t2_deprec AS (
    SELECT ticker, cik,
        ROUND(deprec / net_assets * 100, 1) AS deprec_pct_nav,
        CASE WHEN deprec/net_assets >= 0.55 THEN '🔴' WHEN deprec/net_assets >= 0.40 THEN '🟡' ELSE '🟢' END AS t2_deprec
    FROM bdc_fy_snapshot WHERE deprec IS NOT NULL AND net_assets IS NOT NULL AND net_assets <> 0
),
t3_rl AS (
    WITH fy_l AS (
        SELECT ticker, cik, period_end,
            ABS(LEAST(numeric_value,0)) AS loss,
            LAG(ABS(LEAST(numeric_value,0))) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_loss
        FROM bdc_time_series WHERE metric='realized_gl' AND fiscal_period='FY'
    ),
    latest AS (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
        FROM fy_l WHERE prev_loss IS NOT NULL AND prev_loss > 0
    )
    SELECT ticker, cik,
        ROUND(loss/prev_loss, 2) AS rl_yoy_multiple,
        CASE WHEN loss/prev_loss >= 4 THEN '🔴' WHEN loss/prev_loss >= 2 THEN '🟡' ELSE '🟢' END AS t3_rl
    FROM latest WHERE rn = 1
),
t3_cum AS (
    WITH cl AS (
        SELECT ticker, cik, ABS(SUM(LEAST(numeric_value,0))) AS cum_loss_ps
        FROM bdc_time_series WHERE metric='gl_ps' AND fiscal_period='FY'
        GROUP BY ticker, cik
    )
    SELECT c.ticker, c.cik,
        ROUND(c.cum_loss_ps / s.nav_ps * 100, 1) AS cum_loss_pct_nav,
        CASE WHEN c.cum_loss_ps/s.nav_ps >= 1.0 THEN '🔴' WHEN c.cum_loss_ps/s.nav_ps >= 0.5 THEN '🟡' ELSE '🟢' END AS t3_cum
    FROM cl c JOIN bdc_fy_snapshot s USING(ticker,cik)
    WHERE s.nav_ps IS NOT NULL AND s.nav_ps > 0
)
SELECT
    s.ticker,
    s.cik,
    COALESCE(t1_pik.t1_pik,  '⚪') || ' ' || COALESCE(CAST(t1_pik.pik_nii_pct     AS STRING), 'n/a') || '%'  AS `T1 PIK/NII`,
    COALESCE(t1_div.t1_div,  '⚪') || ' ' || COALESCE(CAST(t1_div.div_coverage_pct AS STRING), 'n/a') || '%'  AS `T1 Div Coverage`,
    COALESCE(t2_nav.t2_nav,  '⚪') || ' ' || COALESCE(CAST(t2_nav.consec_declines  AS STRING), 'n/a') || 'q'  AS `T2 NAV Declines`,
    COALESCE(t2_deprec.t2_deprec,'⚪') || ' ' || COALESCE(CAST(t2_deprec.deprec_pct_nav AS STRING),'n/a') || '%' AS `T2 Deprec/NAV`,
    COALESCE(t3_rl.t3_rl,    '⚪') || ' ' || COALESCE(CAST(t3_rl.rl_yoy_multiple   AS STRING), 'n/a') || 'x'  AS `T3 RL Accel`,
    COALESCE(t3_cum.t3_cum,  '⚪') || ' ' || COALESCE(CAST(t3_cum.cum_loss_pct_nav AS STRING), 'n/a') || '%'  AS `T3 Cum Loss/NAV`
FROM       bdc_fy_snapshot s
LEFT JOIN  t1_pik   USING (ticker, cik)
LEFT JOIN  t1_div   USING (ticker, cik)
LEFT JOIN  t2_nav   USING (ticker, cik)
LEFT JOIN  t2_deprec USING (ticker, cik)
LEFT JOIN  t3_rl    USING (ticker, cik)
LEFT JOIN  t3_cum   USING (ticker, cik)
ORDER BY   s.ticker
