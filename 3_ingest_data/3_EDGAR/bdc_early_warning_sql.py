# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
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

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

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

# MAGIC %sql
# MAGIC -- Inspect the raw data before any calculations
# MAGIC SELECT ticker, cik, metric, COUNT(*) AS periods,
# MAGIC        MIN(period_end) AS earliest, MAX(period_end) AS latest
# MAGIC FROM   bdc_time_series
# MAGIC GROUP  BY ticker, cik, metric
# MAGIC ORDER  BY ticker, metric

# COMMAND ----------

# MAGIC %sql
# MAGIC -- FY snapshot — one row per ticker, all raw metric values side by side
# MAGIC SELECT *
# MAGIC FROM   bdc_fy_snapshot
# MAGIC ORDER  BY ticker

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T1: PIK-to-NII Ratio
# MAGIC -- Measures how much of net investment income is paid-in-kind (non-cash).
# MAGIC -- High PIK means borrowers can't service debt in cash — early stress signal.
# MAGIC -- Watch > 20%  |  Concern > 30%
# MAGIC
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     cik,
# MAGIC     ROUND(pik / nii * 100, 1)                             AS pik_nii_pct,
# MAGIC     CASE
# MAGIC         WHEN pik / nii >= 0.30 THEN '🔴 CONCERN'
# MAGIC         WHEN pik / nii >= 0.20 THEN '🟡 WATCH'
# MAGIC         ELSE                        '🟢 OK'
# MAGIC     END                                                    AS pik_nii_signal
# MAGIC FROM  bdc_fy_snapshot
# MAGIC WHERE pik IS NOT NULL
# MAGIC   AND nii IS NOT NULL
# MAGIC   AND nii <> 0
# MAGIC ORDER BY pik / nii DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T1: Dividend Coverage
# MAGIC -- NII per share / dividends declared per share.
# MAGIC -- Below 1.0x means dividends exceed earnings — unsustainable without NAV erosion.
# MAGIC -- Watch < 105%  |  Concern < 100%
# MAGIC
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     cik,
# MAGIC     ROUND(nii_ps / div_ps * 100, 1)                       AS div_coverage_pct,
# MAGIC     CASE
# MAGIC         WHEN nii_ps / div_ps <= 1.00 THEN '🔴 CONCERN'
# MAGIC         WHEN nii_ps / div_ps <= 1.05 THEN '🟡 WATCH'
# MAGIC         ELSE                               '🟢 OK'
# MAGIC     END                                                    AS div_coverage_signal
# MAGIC FROM  bdc_fy_snapshot
# MAGIC WHERE nii_ps IS NOT NULL
# MAGIC   AND div_ps  IS NOT NULL
# MAGIC   AND div_ps <> 0
# MAGIC ORDER BY nii_ps / div_ps ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T2: NAV Trajectory — consecutive quarterly declines
# MAGIC -- Persistent NAV decline means the portfolio is losing value faster than earnings.
# MAGIC -- Watch >= 3 quarters  |  Concern >= 6 quarters
# MAGIC --
# MAGIC -- Approach: use LAG() to flag each period as a decline, then find the longest
# MAGIC -- streak ending at the most recent observation using a gap-and-islands pattern.
# MAGIC
# MAGIC WITH lagged AS (
# MAGIC     SELECT
# MAGIC         ticker, cik, period_end,
# MAGIC         numeric_value                                                        AS nav,
# MAGIC         LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end)   AS prev_nav
# MAGIC     FROM bdc_time_series
# MAGIC     WHERE metric = 'nav_ps'
# MAGIC ),
# MAGIC flagged AS (
# MAGIC     SELECT *,
# MAGIC         CASE WHEN nav < prev_nav THEN 1 ELSE 0 END AS is_decline,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC)    AS rn
# MAGIC     FROM lagged
# MAGIC     WHERE prev_nav IS NOT NULL
# MAGIC ),
# MAGIC -- Find where the streak breaks (first non-decline from the end)
# MAGIC streak AS (
# MAGIC     SELECT ticker, cik,
# MAGIC         -- Sum of consecutive declines from rn=1 until first non-decline
# MAGIC         SUM(is_decline) OVER (
# MAGIC             PARTITION BY ticker
# MAGIC             ORDER BY rn
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         )                                                                    AS running_sum,
# MAGIC         is_decline,
# MAGIC         rn
# MAGIC     FROM flagged
# MAGIC ),
# MAGIC -- The consecutive streak = running_sum at the point is_decline first becomes 0
# MAGIC consec AS (
# MAGIC     SELECT ticker, cik,
# MAGIC         COALESCE(
# MAGIC             MIN(CASE WHEN is_decline = 0 THEN running_sum - 1 END),
# MAGIC             MAX(running_sum)
# MAGIC         ) AS consecutive_declines
# MAGIC     FROM streak
# MAGIC     GROUP BY ticker, cik
# MAGIC )
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     cik,
# MAGIC     consecutive_declines,
# MAGIC     CASE
# MAGIC         WHEN consecutive_declines >= 6 THEN '🔴 CONCERN'
# MAGIC         WHEN consecutive_declines >= 3 THEN '🟡 WATCH'
# MAGIC         ELSE                                 '🟢 OK'
# MAGIC     END AS nav_trend_signal
# MAGIC FROM consec
# MAGIC ORDER BY consecutive_declines DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T2: Unrealized Depreciation / Net Assets
# MAGIC -- Rising unrealized losses as % of NAV confirms portfolio deterioration.
# MAGIC -- Watch > 40%  |  Concern > 55%
# MAGIC --
# MAGIC -- net_assets (us-gaap:AssetsNet) unavailable from EDGAR for these BDCs.
# MAGIC -- Estimated as:  nav_ps × (nii / nii_ps)  ≈  NAV/share × shares outstanding
# MAGIC
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     cik,
# MAGIC     ROUND(deprec / (nav_ps * (nii / nii_ps)) * 100, 1)    AS deprec_pct_nav,
# MAGIC     CASE
# MAGIC         WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.55 THEN '🔴 CONCERN'
# MAGIC         WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.40 THEN '🟡 WATCH'
# MAGIC         ELSE                                                  '🟢 OK'
# MAGIC     END                                                    AS deprec_signal
# MAGIC FROM  bdc_fy_snapshot
# MAGIC WHERE deprec  IS NOT NULL
# MAGIC   AND nav_ps  IS NOT NULL AND nav_ps  <> 0
# MAGIC   AND nii     IS NOT NULL AND nii     <> 0
# MAGIC   AND nii_ps  IS NOT NULL AND nii_ps  <> 0
# MAGIC ORDER BY deprec / (nav_ps * (nii / nii_ps)) DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T3: Realized Loss Acceleration
# MAGIC -- YoY growth in realized losses signals the portfolio is crystallizing stress.
# MAGIC -- Watch > 2x prior year  |  Concern > 4x prior year
# MAGIC
# MAGIC WITH fy_losses AS (
# MAGIC     SELECT
# MAGIC         ticker, cik, period_end,
# MAGIC         -- Flip sign: losses are negative, we want positive magnitude
# MAGIC         ABS(LEAST(numeric_value, 0))                                            AS loss_abs,
# MAGIC         LAG(ABS(LEAST(numeric_value, 0))) OVER (
# MAGIC             PARTITION BY ticker ORDER BY period_end
# MAGIC         )                                                                       AS prior_loss_abs
# MAGIC     FROM bdc_time_series
# MAGIC     WHERE metric = 'realized_gl'
# MAGIC       AND fiscal_period = 'FY'
# MAGIC ),
# MAGIC latest AS (
# MAGIC     SELECT *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
# MAGIC     FROM fy_losses
# MAGIC     WHERE prior_loss_abs IS NOT NULL AND prior_loss_abs > 0
# MAGIC )
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     cik,
# MAGIC     period_end,
# MAGIC     ROUND(loss_abs, 0)                                     AS realized_loss,
# MAGIC     ROUND(prior_loss_abs, 0)                               AS prior_year_loss,
# MAGIC     ROUND(loss_abs / prior_loss_abs, 2)                    AS yoy_multiple,
# MAGIC     CASE
# MAGIC         WHEN loss_abs / prior_loss_abs >= 4.0 THEN '🔴 CONCERN'
# MAGIC         WHEN loss_abs / prior_loss_abs >= 2.0 THEN '🟡 WATCH'
# MAGIC         ELSE                                        '🟢 OK'
# MAGIC     END                                                    AS rl_accel_signal
# MAGIC FROM latest
# MAGIC WHERE rn = 1
# MAGIC ORDER BY yoy_multiple DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- T3: Cumulative Losses vs. Current NAV/share
# MAGIC -- Cumulative realized per-share losses as % of current NAV.
# MAGIC -- When this exceeds 100% the portfolio has lost more than its current book value.
# MAGIC -- Watch > 50%  |  Concern > 100%
# MAGIC
# MAGIC WITH cum_losses AS (
# MAGIC     SELECT
# MAGIC         ticker, cik,
# MAGIC         -- Sum only negative (loss) periods
# MAGIC         ABS(SUM(LEAST(numeric_value, 0)))   AS cumulative_loss_ps
# MAGIC     FROM bdc_time_series
# MAGIC     WHERE metric       = 'gl_ps'
# MAGIC       AND fiscal_period = 'FY'
# MAGIC     GROUP BY ticker, cik
# MAGIC )
# MAGIC SELECT
# MAGIC     c.ticker,
# MAGIC     c.cik,
# MAGIC     ROUND(c.cumulative_loss_ps, 4)                         AS cum_loss_ps,
# MAGIC     s.nav_ps                                               AS current_nav_ps,
# MAGIC     ROUND(c.cumulative_loss_ps / s.nav_ps * 100, 1)        AS cum_loss_pct_nav,
# MAGIC     CASE
# MAGIC         WHEN c.cumulative_loss_ps / s.nav_ps >= 1.00 THEN '🔴 CONCERN'
# MAGIC         WHEN c.cumulative_loss_ps / s.nav_ps >= 0.50 THEN '🟡 WATCH'
# MAGIC         ELSE                                               '🟢 OK'
# MAGIC     END                                                    AS cum_loss_signal
# MAGIC FROM      cum_losses c
# MAGIC JOIN      bdc_fy_snapshot s USING (ticker, cik)
# MAGIC WHERE     s.nav_ps IS NOT NULL AND s.nav_ps > 0
# MAGIC ORDER BY  cum_loss_pct_nav DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full dashboard — all signals joined
# MAGIC
# MAGIC WITH t1_pik AS (
# MAGIC     SELECT ticker, cik,
# MAGIC         ROUND(pik / nii * 100, 1) AS pik_nii_pct,
# MAGIC         CASE WHEN pik/nii >= 0.30 THEN '🔴' WHEN pik/nii >= 0.20 THEN '🟡' ELSE '🟢' END AS t1_pik
# MAGIC     FROM bdc_fy_snapshot WHERE pik IS NOT NULL AND nii IS NOT NULL AND nii <> 0
# MAGIC ),
# MAGIC t1_div AS (
# MAGIC     SELECT ticker, cik,
# MAGIC         ROUND(nii_ps / div_ps * 100, 1) AS div_coverage_pct,
# MAGIC         CASE WHEN nii_ps/div_ps <= 1.00 THEN '🔴' WHEN nii_ps/div_ps <= 1.05 THEN '🟡' ELSE '🟢' END AS t1_div
# MAGIC     FROM bdc_fy_snapshot WHERE nii_ps IS NOT NULL AND div_ps IS NOT NULL AND div_ps <> 0
# MAGIC ),
# MAGIC t2_nav AS (
# MAGIC     WITH lagged AS (
# MAGIC         SELECT ticker, cik, period_end, numeric_value AS nav,
# MAGIC             LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav
# MAGIC         FROM bdc_time_series WHERE metric = 'nav_ps'
# MAGIC     ),
# MAGIC     flagged AS (
# MAGIC         SELECT ticker, cik,
# MAGIC             SUM(CASE WHEN nav < prev_nav THEN 1 ELSE 0 END) OVER (
# MAGIC                 PARTITION BY ticker ORDER BY period_end DESC
# MAGIC                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC             ) AS running_declines,
# MAGIC             CASE WHEN nav >= prev_nav THEN 1 ELSE 0 END AS streak_break,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
# MAGIC         FROM lagged WHERE prev_nav IS NOT NULL
# MAGIC     )
# MAGIC     SELECT ticker, cik,
# MAGIC         MIN(CASE WHEN streak_break = 1 THEN running_declines - 1 END) AS consec_declines,
# MAGIC         CASE WHEN MIN(CASE WHEN streak_break=1 THEN running_declines-1 END) >= 6 THEN '🔴'
# MAGIC              WHEN MIN(CASE WHEN streak_break=1 THEN running_declines-1 END) >= 3 THEN '🟡'
# MAGIC              ELSE '🟢' END AS t2_nav
# MAGIC     FROM flagged GROUP BY ticker, cik
# MAGIC ),
# MAGIC t2_deprec AS (
# MAGIC     -- net_assets unavailable; estimated as nav_ps × (nii / nii_ps)
# MAGIC     SELECT ticker, cik,
# MAGIC         ROUND(deprec / (nav_ps * (nii / nii_ps)) * 100, 1) AS deprec_pct_nav,
# MAGIC         CASE WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.55 THEN '🔴'
# MAGIC              WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.40 THEN '🟡'
# MAGIC              ELSE '🟢' END AS t2_deprec
# MAGIC     FROM bdc_fy_snapshot
# MAGIC     WHERE deprec IS NOT NULL
# MAGIC       AND nav_ps IS NOT NULL AND nav_ps <> 0
# MAGIC       AND nii   IS NOT NULL AND nii   <> 0
# MAGIC       AND nii_ps IS NOT NULL AND nii_ps <> 0
# MAGIC ),
# MAGIC t3_rl AS (
# MAGIC     WITH fy_l AS (
# MAGIC         SELECT ticker, cik, period_end,
# MAGIC             ABS(LEAST(numeric_value,0)) AS loss,
# MAGIC             LAG(ABS(LEAST(numeric_value,0))) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_loss
# MAGIC         FROM bdc_time_series WHERE metric='realized_gl' AND fiscal_period='FY'
# MAGIC     ),
# MAGIC     latest AS (
# MAGIC         SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
# MAGIC         FROM fy_l WHERE prev_loss IS NOT NULL AND prev_loss > 0
# MAGIC     )
# MAGIC     SELECT ticker, cik,
# MAGIC         ROUND(loss/prev_loss, 2) AS rl_yoy_multiple,
# MAGIC         CASE WHEN loss/prev_loss >= 4 THEN '🔴' WHEN loss/prev_loss >= 2 THEN '🟡' ELSE '🟢' END AS t3_rl
# MAGIC     FROM latest WHERE rn = 1
# MAGIC ),
# MAGIC t3_cum AS (
# MAGIC     WITH cl AS (
# MAGIC         SELECT ticker, cik, ABS(SUM(LEAST(numeric_value,0))) AS cum_loss_ps
# MAGIC         FROM bdc_time_series WHERE metric='gl_ps' AND fiscal_period='FY'
# MAGIC         GROUP BY ticker, cik
# MAGIC     )
# MAGIC     SELECT c.ticker, c.cik,
# MAGIC         ROUND(c.cum_loss_ps / s.nav_ps * 100, 1) AS cum_loss_pct_nav,
# MAGIC         CASE WHEN c.cum_loss_ps/s.nav_ps >= 1.0 THEN '🔴' WHEN c.cum_loss_ps/s.nav_ps >= 0.5 THEN '🟡' ELSE '🟢' END AS t3_cum
# MAGIC     FROM cl c JOIN bdc_fy_snapshot s USING(ticker,cik)
# MAGIC     WHERE s.nav_ps IS NOT NULL AND s.nav_ps > 0
# MAGIC )
# MAGIC SELECT
# MAGIC     s.ticker,
# MAGIC     s.cik,
# MAGIC     COALESCE(t1_pik.t1_pik,  '⚪') || ' ' || COALESCE(CAST(t1_pik.pik_nii_pct     AS STRING), 'n/a') || '%'  AS `T1 PIK/NII`,
# MAGIC     COALESCE(t1_div.t1_div,  '⚪') || ' ' || COALESCE(CAST(t1_div.div_coverage_pct AS STRING), 'n/a') || '%'  AS `T1 Div Coverage`,
# MAGIC     COALESCE(t2_nav.t2_nav,  '⚪') || ' ' || COALESCE(CAST(t2_nav.consec_declines  AS STRING), 'n/a') || 'q'  AS `T2 NAV Declines`,
# MAGIC     COALESCE(t2_deprec.t2_deprec,'⚪') || ' ' || COALESCE(CAST(t2_deprec.deprec_pct_nav AS STRING),'n/a') || '%' AS `T2 Deprec/NAV`,
# MAGIC     COALESCE(t3_rl.t3_rl,    '⚪') || ' ' || COALESCE(CAST(t3_rl.rl_yoy_multiple   AS STRING), 'n/a') || 'x'  AS `T3 RL Accel`,
# MAGIC     COALESCE(t3_cum.t3_cum,  '⚪') || ' ' || COALESCE(CAST(t3_cum.cum_loss_pct_nav AS STRING), 'n/a') || '%'  AS `T3 Cum Loss/NAV`
# MAGIC FROM       bdc_fy_snapshot s
# MAGIC LEFT JOIN  t1_pik   USING (ticker, cik)
# MAGIC LEFT JOIN  t1_div   USING (ticker, cik)
# MAGIC LEFT JOIN  t2_nav   USING (ticker, cik)
# MAGIC LEFT JOIN  t2_deprec USING (ticker, cik)
# MAGIC LEFT JOIN  t3_rl    USING (ticker, cik)
# MAGIC LEFT JOIN  t3_cum   USING (ticker, cik)
# MAGIC ORDER BY   s.ticker

# COMMAND ----------

# DBTITLE 1,Persist data as Delta tables
# ── Persist the two DataFrames as Delta tables in Unity Catalog ─────────────────

TABLE_TS  = f"{UC_CATALOG}.{UC_SCHEMA}.bdc_time_series"
TABLE_FY  = f"{UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot"

spark.createDataFrame(bdc_time_series).write.mode("overwrite").saveAsTable(TABLE_TS)
spark.createDataFrame(bdc_fy_snapshot).write.mode("overwrite").saveAsTable(TABLE_FY, mergeSchema=True)

print(f"Wrote {TABLE_TS} and {TABLE_FY}")

# COMMAND ----------

jkhhjhlkjh

# COMMAND ----------

# DBTITLE 1,Scalar UDFs: T1 and T2 ratio signals
# MAGIC %sql
# MAGIC -- Scalar functions for ratio-based signals (work on any row of snapshot data)
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_pik_nii_signal(pik DOUBLE, nii DOUBLE)
# MAGIC RETURNS STRUCT<pct: DOUBLE, signal: STRING>
# MAGIC COMMENT 'T1 PIK/NII ratio. >20% WATCH, >30% CONCERN. High PIK = borrowers paying in kind, not cash.'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN pik IS NULL OR nii IS NULL OR nii = 0 THEN STRUCT(NULL, 'n/a')
# MAGIC     ELSE STRUCT(
# MAGIC       ROUND(pik / nii * 100, 1),
# MAGIC       CASE WHEN pik / nii >= 0.30 THEN 'CONCERN'
# MAGIC            WHEN pik / nii >= 0.20 THEN 'WATCH'
# MAGIC            ELSE 'OK' END
# MAGIC     )
# MAGIC   END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_div_coverage_signal(nii_ps DOUBLE, div_ps DOUBLE)
# MAGIC RETURNS STRUCT<pct: DOUBLE, signal: STRING>
# MAGIC COMMENT 'T1 Dividend Coverage. NII/share div/share. <105% WATCH, <100% CONCERN.'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN nii_ps IS NULL OR div_ps IS NULL OR div_ps = 0 THEN STRUCT(NULL, 'n/a')
# MAGIC     ELSE STRUCT(
# MAGIC       ROUND(nii_ps / div_ps * 100, 1),
# MAGIC       CASE WHEN nii_ps / div_ps <= 1.00 THEN 'CONCERN'
# MAGIC            WHEN nii_ps / div_ps <= 1.05 THEN 'WATCH'
# MAGIC            ELSE 'OK' END
# MAGIC     )
# MAGIC   END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_deprec_nav_signal(
# MAGIC     deprec DOUBLE, nav_ps DOUBLE, nii DOUBLE, nii_ps DOUBLE)
# MAGIC RETURNS STRUCT<pct: DOUBLE, signal: STRING>
# MAGIC COMMENT 'T2 Unrealised depreciation / est. net assets. >40% WATCH, >55% CONCERN. Net assets estimated as nav_ps * (nii / nii_ps).'
# MAGIC RETURN
# MAGIC   CASE
# MAGIC     WHEN deprec IS NULL OR nav_ps IS NULL OR nav_ps = 0
# MAGIC          OR nii IS NULL OR nii = 0 OR nii_ps IS NULL OR nii_ps = 0
# MAGIC       THEN STRUCT(NULL, 'n/a')
# MAGIC     ELSE STRUCT(
# MAGIC       ROUND(deprec / (nav_ps * (nii / nii_ps)) * 100, 1),
# MAGIC       CASE WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.55 THEN 'CONCERN'
# MAGIC            WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.40 THEN 'WATCH'
# MAGIC            ELSE 'OK' END
# MAGIC     )
# MAGIC   END;

# COMMAND ----------

# DBTITLE 1,Table function: T2 NAV trajectory
# MAGIC %sql
# MAGIC -- T2: NAV Trajectory — consecutive quarterly declines from the most recent period
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_nav_trend_signal()
# MAGIC RETURNS TABLE (
# MAGIC     ticker STRING, cik STRING,
# MAGIC     consecutive_declines INT, signal STRING
# MAGIC )
# MAGIC COMMENT 'T2 NAV trend. Counts consecutive quarterly NAV declines from most recent period. >=3 WATCH, >=6 CONCERN.'
# MAGIC RETURN
# MAGIC   WITH lagged AS (
# MAGIC       SELECT ticker, cik, period_end,
# MAGIC           numeric_value AS nav,
# MAGIC           LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav
# MAGIC       FROM ahtsa.awm.bdc_time_series
# MAGIC       WHERE metric = 'nav_ps'
# MAGIC   ),
# MAGIC   flagged AS (
# MAGIC       SELECT ticker, cik,
# MAGIC           CASE WHEN nav < prev_nav THEN 1 ELSE 0 END AS is_decline,
# MAGIC           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
# MAGIC       FROM lagged
# MAGIC       WHERE prev_nav IS NOT NULL
# MAGIC   ),
# MAGIC   streak AS (
# MAGIC       SELECT ticker, cik,
# MAGIC           SUM(is_decline) OVER (
# MAGIC               PARTITION BY ticker ORDER BY rn
# MAGIC               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC           ) AS running_sum,
# MAGIC           is_decline, rn
# MAGIC       FROM flagged
# MAGIC   ),
# MAGIC   consec AS (
# MAGIC       SELECT ticker, cik,
# MAGIC           COALESCE(
# MAGIC               MIN(CASE WHEN is_decline = 0 THEN running_sum - 1 END),
# MAGIC               MAX(running_sum)
# MAGIC           ) AS consecutive_declines
# MAGIC       FROM streak
# MAGIC       GROUP BY ticker, cik
# MAGIC   )
# MAGIC   SELECT ticker, cik, consecutive_declines,
# MAGIC       CASE WHEN consecutive_declines >= 6 THEN 'CONCERN'
# MAGIC            WHEN consecutive_declines >= 3 THEN 'WATCH'
# MAGIC            ELSE 'OK' END AS signal
# MAGIC   FROM consec;

# COMMAND ----------

# DBTITLE 1,Table function: T3 realized loss acceleration
# MAGIC %sql
# MAGIC -- T3: Realized Loss Acceleration — YoY multiple of realised losses
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_rl_accel_signal()
# MAGIC RETURNS TABLE (
# MAGIC     ticker STRING, cik STRING, period_end TIMESTAMP,
# MAGIC     realized_loss DOUBLE, prior_year_loss DOUBLE,
# MAGIC     yoy_multiple DOUBLE, signal STRING
# MAGIC )
# MAGIC COMMENT 'T3 Realized loss acceleration. YoY multiple of FY realized losses. >2x WATCH, >4x CONCERN.'
# MAGIC RETURN
# MAGIC   WITH fy_losses AS (
# MAGIC       SELECT ticker, cik, period_end,
# MAGIC           ABS(LEAST(numeric_value, 0)) AS loss_abs,
# MAGIC           LAG(ABS(LEAST(numeric_value, 0))) OVER (
# MAGIC               PARTITION BY ticker ORDER BY period_end
# MAGIC           ) AS prior_loss_abs
# MAGIC       FROM ahtsa.awm.bdc_time_series
# MAGIC       WHERE metric = 'realized_gl' AND fiscal_period = 'FY'
# MAGIC   ),
# MAGIC   latest AS (
# MAGIC       SELECT *,
# MAGIC           ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
# MAGIC       FROM fy_losses
# MAGIC       WHERE prior_loss_abs IS NOT NULL AND prior_loss_abs > 0
# MAGIC   )
# MAGIC   SELECT ticker, cik, period_end,
# MAGIC       ROUND(loss_abs, 0) AS realized_loss,
# MAGIC       ROUND(prior_loss_abs, 0) AS prior_year_loss,
# MAGIC       ROUND(loss_abs / prior_loss_abs, 2) AS yoy_multiple,
# MAGIC       CASE WHEN loss_abs / prior_loss_abs >= 4.0 THEN 'CONCERN'
# MAGIC            WHEN loss_abs / prior_loss_abs >= 2.0 THEN 'WATCH'
# MAGIC            ELSE 'OK' END AS signal
# MAGIC   FROM latest WHERE rn = 1;

# COMMAND ----------

# DBTITLE 1,Table function: T3 cumulative loss / NAV
# MAGIC %sql
# MAGIC -- T3: Cumulative per-share losses vs current NAV
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_cum_loss_signal()
# MAGIC RETURNS TABLE (
# MAGIC     ticker STRING, cik STRING,
# MAGIC     cum_loss_ps DOUBLE, current_nav_ps DOUBLE,
# MAGIC     cum_loss_pct_nav DOUBLE, signal STRING
# MAGIC )
# MAGIC COMMENT 'T3 Cumulative per-share losses / current NAV. >50% WATCH, >100% CONCERN.'
# MAGIC RETURN
# MAGIC   WITH cum_losses AS (
# MAGIC       SELECT ticker, cik,
# MAGIC           ABS(SUM(LEAST(numeric_value, 0))) AS cumulative_loss_ps
# MAGIC       FROM ahtsa.awm.bdc_time_series
# MAGIC       WHERE metric = 'gl_ps' AND fiscal_period = 'FY'
# MAGIC       GROUP BY ticker, cik
# MAGIC   )
# MAGIC   SELECT c.ticker, c.cik,
# MAGIC       ROUND(c.cumulative_loss_ps, 4) AS cum_loss_ps,
# MAGIC       s.nav_ps AS current_nav_ps,
# MAGIC       ROUND(c.cumulative_loss_ps / s.nav_ps * 100, 1) AS cum_loss_pct_nav,
# MAGIC       CASE WHEN c.cumulative_loss_ps / s.nav_ps >= 1.00 THEN 'CONCERN'
# MAGIC            WHEN c.cumulative_loss_ps / s.nav_ps >= 0.50 THEN 'WATCH'
# MAGIC            ELSE 'OK' END AS signal
# MAGIC   FROM cum_losses c
# MAGIC   JOIN ahtsa.awm.bdc_fy_snapshot s USING (ticker, cik)
# MAGIC   WHERE s.nav_ps IS NOT NULL AND s.nav_ps > 0;

# COMMAND ----------

# DBTITLE 1,Table function: Full dashboard (all signals)
# MAGIC %sql
# MAGIC -- Full dashboard function — calls scalar UDFs + joins table functions
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION ahtsa.awm.bdc_early_warning_dashboard()
# MAGIC RETURNS TABLE (
# MAGIC     ticker STRING, cik STRING,
# MAGIC     t1_pik_nii_pct DOUBLE,       t1_pik_nii_signal STRING,
# MAGIC     t1_div_coverage_pct DOUBLE,  t1_div_coverage_signal STRING,
# MAGIC     t2_nav_consec_declines INT,  t2_nav_trend_signal STRING,
# MAGIC     t2_deprec_pct_nav DOUBLE,    t2_deprec_signal STRING,
# MAGIC     t3_rl_yoy_multiple DOUBLE,   t3_rl_accel_signal STRING,
# MAGIC     t3_cum_loss_pct_nav DOUBLE,  t3_cum_loss_signal STRING
# MAGIC )
# MAGIC COMMENT 'Combined BDC early warning dashboard. Joins all T1/T2/T3 signals for every ticker in bdc_fy_snapshot.'
# MAGIC RETURN
# MAGIC   SELECT
# MAGIC       s.ticker,
# MAGIC       s.cik,
# MAGIC       ahtsa.awm.bdc_pik_nii_signal(s.pik, s.nii).pct           AS t1_pik_nii_pct,
# MAGIC       ahtsa.awm.bdc_pik_nii_signal(s.pik, s.nii).signal        AS t1_pik_nii_signal,
# MAGIC       ahtsa.awm.bdc_div_coverage_signal(s.nii_ps, s.div_ps).pct    AS t1_div_coverage_pct,
# MAGIC       ahtsa.awm.bdc_div_coverage_signal(s.nii_ps, s.div_ps).signal AS t1_div_coverage_signal,
# MAGIC       nav.consecutive_declines                                  AS t2_nav_consec_declines,
# MAGIC       COALESCE(nav.signal, 'n/a')                               AS t2_nav_trend_signal,
# MAGIC       ahtsa.awm.bdc_deprec_nav_signal(s.deprec, s.nav_ps, s.nii, s.nii_ps).pct    AS t2_deprec_pct_nav,
# MAGIC       ahtsa.awm.bdc_deprec_nav_signal(s.deprec, s.nav_ps, s.nii, s.nii_ps).signal AS t2_deprec_signal,
# MAGIC       rl.yoy_multiple                                           AS t3_rl_yoy_multiple,
# MAGIC       COALESCE(rl.signal, 'n/a')                                AS t3_rl_accel_signal,
# MAGIC       cl.cum_loss_pct_nav                                       AS t3_cum_loss_pct_nav,
# MAGIC       COALESCE(cl.signal, 'n/a')                                AS t3_cum_loss_signal
# MAGIC   FROM      ahtsa.awm.bdc_fy_snapshot s
# MAGIC   LEFT JOIN ahtsa.awm.bdc_nav_trend_signal() nav USING (ticker, cik)
# MAGIC   LEFT JOIN ahtsa.awm.bdc_rl_accel_signal()  rl  USING (ticker, cik)
# MAGIC   LEFT JOIN ahtsa.awm.bdc_cum_loss_signal()  cl  USING (ticker, cik)
# MAGIC   ORDER BY  s.ticker;

# COMMAND ----------

# DBTITLE 1,Validate: call the dashboard function
# MAGIC %sql
# MAGIC -- Quick validation — call the combined dashboard function
# MAGIC SELECT * FROM ahtsa.awm.bdc_early_warning_dashboard()
