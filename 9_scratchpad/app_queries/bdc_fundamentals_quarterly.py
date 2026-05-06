# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# BDC Critical Metrics — Quarterly (Current vs Prior Quarter)
#
# Companion to bdc_fundamentals (annual FY variant).
# Produces one row per (symbol × KPI) matching gold_app_company_fundamentals schema.
#
# Period identification: calendar quarter-end dates derived from MONTH(period_end):
#   March 31 → Q1 | June 30 → Q2 | Sep 30 → Q3 | Dec 31 → Q4
# fiscal_period labels are NOT used for quarter identification — BDCs use different
# fiscal year conventions so "Q1" can fall in any calendar month.
#
# 4 KPIs (metrics with clean quarterly data across ≥10/16 tickers):
#   1. NAV per Share         — point-in-time NAV/sh ($)              (higher = better)
#   2. NII per Share         — quarterly net investment income/sh ($) (higher = better)
#   3. PIK/NII Ratio         — non-cash income as % of NII            (lower  = better; alert ≥ 30%)
#   4. Unrealized Deprec/NAV — depreciation as % of estimated NAV     (lower  = better; alert ≥ 55%)
#
# Metric-specific fiscal_period filter (key design decision):
#   nav_ps: ALL fiscal_period labels — it's point-in-time; some BDCs label it FY even
#           for quarterly snapshots, so excluding FY drops valid NAV data for BXSL/OBDC/GSBD.
#   nii_ps, pik, nii, deprec: quarterly labels only (Q1/Q2/Q3/Q4) — these are flow
#           metrics where FY = full-year total ≠ single quarter; mixing them creates
#           misleading QoQ comparisons (e.g. $2.02 annual vs $0.55 quarterly).
#
# Excluded KPIs from quarterly view:
#   Dividend Coverage  — div_ps mixes quarterly rates and cumulative YTD in source
#   Realized G/L/Share — single-deal noise makes QoQ uninformative; use FY variant

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

ts_count = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series").collect()[0][0]
print(f"bdc_time_series rows: {ts_count}")
if ts_count == 0:
    raise Exception("bdc_time_series is empty — run the ingestion pipeline first.")

# COMMAND ----------

# DBTITLE 1,BDC Fundamentals — Quarterly
bdc_quarterly_sql = f"""
WITH

-- ── 1a. Point-in-time metrics: include all fiscal_period labels ────────────────
-- nav_ps is a balance-sheet snapshot; some BDCs label quarterly dates as 'FY'.
-- Deduplicate by keeping one value per (ticker, metric, period_end).
ts_nav_ps AS (
    SELECT ticker, metric, CAST(period_end AS DATE) AS period_end, numeric_value
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, metric, CAST(period_end AS DATE)
                ORDER BY numeric_value DESC NULLS LAST
            ) AS rn
        FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
        WHERE metric = 'nav_ps'
    )
    WHERE rn = 1
      AND MONTH(CAST(period_end AS DATE)) IN (3, 6, 9, 12)
),

-- ── 1b. Flow metrics: quarterly fiscal_period labels only ─────────────────────
-- nii_ps, pik, nii, deprec accumulate over a period. FY values are full-year
-- totals; including them would compare e.g. $2.02 (annual) vs $0.55 (quarterly).
ts_flow AS (
    SELECT ticker, metric, CAST(period_end AS DATE) AS period_end, numeric_value
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, metric, CAST(period_end AS DATE)
                ORDER BY numeric_value DESC NULLS LAST
            ) AS rn
        FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
        WHERE metric IN ('nii_ps', 'pik', 'nii', 'deprec')
          AND fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4')
    )
    WHERE rn = 1
      AND MONTH(CAST(period_end AS DATE)) IN (3, 6, 9, 12)
),

-- ── 2. Union and derive calendar quarter labels ────────────────────────────────
ts_quarters AS (
    SELECT ticker, metric, period_end, numeric_value,
        CASE MONTH(period_end)
            WHEN  3 THEN CONCAT('Q1 ', YEAR(period_end))
            WHEN  6 THEN CONCAT('Q2 ', YEAR(period_end))
            WHEN  9 THEN CONCAT('Q3 ', YEAR(period_end))
            WHEN 12 THEN CONCAT('Q4 ', YEAR(period_end))
        END AS quarter_label
    FROM ts_nav_ps
    UNION ALL
    SELECT ticker, metric, period_end, numeric_value,
        CASE MONTH(period_end)
            WHEN  3 THEN CONCAT('Q1 ', YEAR(period_end))
            WHEN  6 THEN CONCAT('Q2 ', YEAR(period_end))
            WHEN  9 THEN CONCAT('Q3 ', YEAR(period_end))
            WHEN 12 THEN CONCAT('Q4 ', YEAR(period_end))
        END AS quarter_label
    FROM ts_flow
),

-- ── 3. Rank quarter-end periods per (ticker, metric) ──────────────────────────
-- rk=1 → most recent quarter, rk=2 → prior quarter.
q_ranked AS (
    SELECT ticker, metric, period_end, numeric_value, quarter_label,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, metric ORDER BY period_end DESC
        ) AS rk
    FROM ts_quarters
),

-- ── 4. Pivot current (rk=1) values per ticker ─────────────────────────────────
q_curr AS (
    SELECT
        ticker,
        MAX(CASE WHEN metric = 'nav_ps' THEN quarter_label  END) AS q_nav_ps,
        MAX(CASE WHEN metric = 'nav_ps' THEN numeric_value  END) AS nav_ps,
        MAX(CASE WHEN metric = 'nii_ps' THEN quarter_label  END) AS q_nii_ps,
        MAX(CASE WHEN metric = 'nii_ps' THEN numeric_value  END) AS nii_ps,
        MAX(CASE WHEN metric = 'pik'    THEN numeric_value  END) AS pik,
        MAX(CASE WHEN metric = 'nii'    THEN numeric_value  END) AS nii,
        MAX(CASE WHEN metric = 'deprec' THEN numeric_value  END) AS deprec
    FROM q_ranked
    WHERE rk = 1
    GROUP BY ticker
),

-- ── 5. Pivot prior (rk=2) values per ticker ───────────────────────────────────
q_prior AS (
    SELECT
        ticker,
        MAX(CASE WHEN metric = 'nav_ps' THEN quarter_label  END) AS q_nav_ps,
        MAX(CASE WHEN metric = 'nav_ps' THEN numeric_value  END) AS nav_ps,
        MAX(CASE WHEN metric = 'nii_ps' THEN quarter_label  END) AS q_nii_ps,
        MAX(CASE WHEN metric = 'nii_ps' THEN numeric_value  END) AS nii_ps,
        MAX(CASE WHEN metric = 'pik'    THEN numeric_value  END) AS pik,
        MAX(CASE WHEN metric = 'nii'    THEN numeric_value  END) AS nii,
        MAX(CASE WHEN metric = 'deprec' THEN numeric_value  END) AS deprec
    FROM q_ranked
    WHERE rk = 2
    GROUP BY ticker
),

-- ── 6. Compute derived KPIs and canonical period labels ───────────────────────
-- Use nii_ps quarter label as canonical (16/16 coverage); fall back to nav_ps.
kpis AS (
    SELECT
        c.ticker,
        COALESCE(cp.companyName, c.ticker)                           AS company_name,
        COALESCE(p.q_nii_ps, p.q_nav_ps, 'Prior Q')                 AS prior_period,
        COALESCE(c.q_nii_ps, c.q_nav_ps, 'Current Q')               AS current_period,

        -- KPI 1: NAV per Share
        c.nav_ps                                                      AS nav_curr,
        p.nav_ps                                                      AS nav_prior,

        -- KPI 2: NII per Share
        c.nii_ps                                                      AS nii_ps_curr,
        p.nii_ps                                                      AS nii_ps_prior,

        -- KPI 3: PIK/NII Ratio (%)
        CASE WHEN c.pik IS NOT NULL AND c.nii IS NOT NULL AND c.nii <> 0
             THEN ROUND(c.pik / c.nii * 100, 1) END                  AS pik_nii_curr,
        CASE WHEN p.pik IS NOT NULL AND p.nii IS NOT NULL AND p.nii <> 0
             THEN ROUND(p.pik / p.nii * 100, 1) END                  AS pik_nii_prior,

        -- KPI 4: Unrealized Depreciation / NAV (%)
        CASE WHEN c.deprec IS NOT NULL AND c.nav_ps IS NOT NULL AND c.nav_ps <> 0
                  AND c.nii IS NOT NULL AND c.nii_ps IS NOT NULL AND c.nii_ps <> 0
             THEN ROUND(ABS(c.deprec) / (c.nav_ps * (c.nii / c.nii_ps)) * 100, 1) END AS deprec_nav_curr,
        CASE WHEN p.deprec IS NOT NULL AND p.nav_ps IS NOT NULL AND p.nav_ps <> 0
                  AND p.nii IS NOT NULL AND p.nii_ps IS NOT NULL AND p.nii_ps <> 0
             THEN ROUND(ABS(p.deprec) / (p.nav_ps * (p.nii / p.nii_ps)) * 100, 1) END AS deprec_nav_prior

    FROM q_curr c
    LEFT JOIN q_prior p USING (ticker)
    LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp ON cp.symbol = c.ticker
),

-- ── 7. Unpivot to long format matching gold_app_company_fundamentals ───────────
unpivoted AS (

    -- KPI 1: NAV per Share
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        1                                                                          AS sort_order,
        'NAV per Share'                                                            AS kpi_name,
        CASE WHEN nav_prior IS NOT NULL THEN CONCAT('$', ROUND(nav_prior, 2)) END  AS prior_value,
        CASE WHEN nav_curr  IS NOT NULL THEN CONCAT('$', ROUND(nav_curr,  2)) END  AS current_value,
        CASE WHEN nav_curr IS NOT NULL AND nav_prior IS NOT NULL AND nav_prior <> 0
             THEN ROUND((nav_curr - nav_prior) / ABS(nav_prior) * 100, 1) END      AS change_pct,
        CASE WHEN nav_curr > nav_prior                                             THEN 'up'
             WHEN nav_curr IS NOT NULL AND nav_prior IS NOT NULL                   THEN 'down'
             ELSE NULL END                                                          AS flag
    FROM kpis
    WHERE nav_curr IS NOT NULL

    UNION ALL

    -- KPI 2: NII per Share
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        2, 'NII per Share',
        CASE WHEN nii_ps_prior IS NOT NULL THEN CONCAT('$', ROUND(nii_ps_prior, 2)) END,
        CASE WHEN nii_ps_curr  IS NOT NULL THEN CONCAT('$', ROUND(nii_ps_curr,  2)) END,
        CASE WHEN nii_ps_curr IS NOT NULL AND nii_ps_prior IS NOT NULL AND nii_ps_prior <> 0
             THEN ROUND((nii_ps_curr - nii_ps_prior) / ABS(nii_ps_prior) * 100, 1) END,
        CASE WHEN nii_ps_curr > nii_ps_prior                                      THEN 'up'
             WHEN nii_ps_curr IS NOT NULL AND nii_ps_prior IS NOT NULL             THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE nii_ps_curr IS NOT NULL

    UNION ALL

    -- KPI 3: PIK/NII Ratio — lower is better
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        3, 'PIK/NII Ratio',
        CASE WHEN pik_nii_prior IS NOT NULL THEN CONCAT(pik_nii_prior, '%') END,
        CASE WHEN pik_nii_curr  IS NOT NULL THEN CONCAT(pik_nii_curr,  '%') END,
        CASE WHEN pik_nii_curr IS NOT NULL AND pik_nii_prior IS NOT NULL AND pik_nii_prior <> 0
             THEN ROUND((pik_nii_curr - pik_nii_prior) / ABS(pik_nii_prior) * 100, 1) END,
        CASE WHEN pik_nii_curr >= 30.0                                            THEN 'alert'
             WHEN pik_nii_curr < pik_nii_prior                                    THEN 'up'
             WHEN pik_nii_curr IS NOT NULL AND pik_nii_prior IS NOT NULL           THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE pik_nii_curr IS NOT NULL

    UNION ALL

    -- KPI 4: Unrealized Depreciation / NAV — lower is better
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        4, 'Unrealized Deprec/NAV',
        CASE WHEN deprec_nav_prior IS NOT NULL THEN CONCAT(deprec_nav_prior, '%') END,
        CASE WHEN deprec_nav_curr  IS NOT NULL THEN CONCAT(deprec_nav_curr,  '%') END,
        CASE WHEN deprec_nav_curr IS NOT NULL AND deprec_nav_prior IS NOT NULL AND deprec_nav_prior <> 0
             THEN ROUND((deprec_nav_curr - deprec_nav_prior) / ABS(deprec_nav_prior) * 100, 1) END,
        CASE WHEN deprec_nav_curr >= 55.0                                         THEN 'alert'
             WHEN deprec_nav_curr < deprec_nav_prior                              THEN 'up'
             WHEN deprec_nav_curr IS NOT NULL AND deprec_nav_prior IS NOT NULL     THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE deprec_nav_curr IS NOT NULL
)

SELECT
    symbol,
    company_name,
    prior_period,
    current_period,
    sort_order,
    kpi_name,
    prior_value,
    current_value,
    change_pct,
    flag,
    CAST(0.0 AS DECIMAL(1,1)) AS covenant_value
FROM unpivoted
ORDER BY symbol, sort_order
"""

bdc_q_df = spark.sql(bdc_quarterly_sql)
display(bdc_q_df)

# COMMAND ----------

print("=== Row counts and periods per ticker ===")
display(
    bdc_q_df.groupBy("symbol", "company_name", "prior_period", "current_period")
    .count()
    .orderBy("symbol")
)

# COMMAND ----------

from pyspark.sql import functions as F
print("=== KPI coverage ===")
display(
    bdc_q_df.groupBy("kpi_name")
    .agg(
        F.count("symbol").alias("ticker_count"),
        F.count("prior_value").alias("has_prior"),
        F.sum(F.when(F.col("flag") == "alert", 1).otherwise(0)).alias("alert_count")
    )
    .orderBy("kpi_name")
)

# COMMAND ----------

print("=== Active alerts ===")
display(bdc_q_df.filter("flag = 'alert'").orderBy("symbol", "sort_order"))

# COMMAND ----------

print("=== Sample — ARCC ===")
display(bdc_q_df.filter("symbol = 'ARCC'"))

# COMMAND ----------

# DBTITLE 1,Write to table
bdc_q_df.write.mode("overwrite").saveAsTable(
    f"{UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals_quarterly"
)
print(f"Written {bdc_q_df.count()} rows to {UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals_quarterly")

# COMMAND ----------

display(spark.sql(f"""
    SELECT *
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals_quarterly
    ORDER BY symbol, sort_order
"""))
