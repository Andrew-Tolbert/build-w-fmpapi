# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# BDC Critical Metrics — Current vs Prior Period
#
# Produces one row per (symbol × KPI) matching the gold_app_company_fundamentals schema:
#   symbol, company_name, prior_period, current_period, sort_order, kpi_name,
#   prior_value, current_value, change_pct, flag, covenant_value
#
# Source tables:
#   ahtsa.awm.bdc_time_series   — long format time series (all metrics, all periods)
#   ahtsa.awm.bdc_fy_snapshot   — latest FY snapshot (used for reference)
#   ahtsa.awm.bronze_company_profiles — company names
#
# KPIs produced (6 per BDC ticker where data exists):
#   1. PIK/NII Ratio         — % of NII that is non-cash PIK income     (lower = better)
#   2. Dividend Coverage     — NII/share ÷ Div/share as %               (higher = better; alert < 100%)
#   3. NAV per Share         — Net asset value per share ($)             (higher = better)
#   4. Unrealized Deprec/NAV — Depreciation as % of estimated net assets (lower = better; alert ≥ 55%)
#   5. NII per Share         — Net investment income per share ($)       (higher = better)
#   6. Realized G/L per Share— Cumulative annual realized gain/loss ($)  (less negative = better)
#
# Periods: both use the most recent FY period in bdc_time_series per metric.
# Missing metrics for a given ticker are skipped (no NULL rows emitted).

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# Validate source data is populated
ts_count = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series").collect()[0][0]
print(f"bdc_time_series rows: {ts_count}")
if ts_count == 0:
    raise Exception("bdc_time_series is empty — run the ingestion pipeline first.")

# COMMAND ----------

# DBTITLE 1,BDC Fundamentals — Current vs Prior Period
bdc_fundamentals_sql = f"""
WITH

-- ── 1. Deduplicate ─────────────────────────────────────────────────────────────
-- Some tickers have both a quarterly and annualised value for the same (metric, fiscal_period,
-- period_end) — e.g. div_ps FY 2024-12-31 appears as both 0.77 (quarterly) and 3.08 (annual).
-- Break ties by preferring the larger absolute value (annualised) via numeric_value DESC.
ts_deduped AS (
    SELECT ticker, metric, fiscal_period, CAST(period_end AS DATE) AS period_end, numeric_value
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY ticker, metric, fiscal_period, CAST(period_end AS DATE)
                ORDER BY ABS(numeric_value) DESC NULLS LAST
            ) AS rn
        FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
    )
    WHERE rn = 1
),

-- ── 2. True FY periods — latest period_end per calendar year ──────────────────
-- Some sources mislabel intra-year quarterly rows as fiscal_period='FY'.
-- Anchoring to the latest period_end per (ticker, metric, year) ensures we only
-- pick the true year-end observation for each annual period.
fy_year_end AS (
    SELECT ticker, metric,
        YEAR(period_end) AS fy_year,
        MAX(period_end)  AS fy_period_end
    FROM ts_deduped
    WHERE fiscal_period = 'FY'
    GROUP BY ticker, metric, YEAR(period_end)
),
ts_fy AS (
    SELECT t.ticker, t.metric, t.period_end, t.numeric_value
    FROM ts_deduped t
    JOIN fy_year_end fy
        ON  t.ticker     = fy.ticker
        AND t.metric     = fy.metric
        AND t.period_end = fy.fy_period_end
    WHERE t.fiscal_period = 'FY'
),

-- ── 3. Rank FY year-end periods per (ticker, metric) ──────────────────────────
-- rk=1 → most recent FY period, rk=2 → prior FY period.
fy_ranked AS (
    SELECT ticker, metric, period_end, numeric_value,
        ROW_NUMBER() OVER (
            PARTITION BY ticker, metric ORDER BY period_end DESC
        ) AS rk
    FROM ts_fy
),

-- ── 4. Pivot current (rk=1) FY values per ticker ──────────────────────────────
fy_curr AS (
    SELECT
        ticker,
        MAX(CASE WHEN metric = 'pik'         THEN period_end     END) AS period_pik,
        MAX(CASE WHEN metric = 'pik'         THEN numeric_value  END) AS pik,
        MAX(CASE WHEN metric = 'nii'         THEN numeric_value  END) AS nii,
        MAX(CASE WHEN metric = 'nii_ps'      THEN period_end     END) AS period_nii_ps,
        MAX(CASE WHEN metric = 'nii_ps'      THEN numeric_value  END) AS nii_ps,
        MAX(CASE WHEN metric = 'div_ps'      THEN numeric_value  END) AS div_ps,
        MAX(CASE WHEN metric = 'nav_ps'      THEN period_end     END) AS period_nav_ps,
        MAX(CASE WHEN metric = 'nav_ps'      THEN numeric_value  END) AS nav_ps,
        MAX(CASE WHEN metric = 'deprec'      THEN numeric_value  END) AS deprec,
        MAX(CASE WHEN metric = 'realized_gl' THEN numeric_value  END) AS realized_gl,
        MAX(CASE WHEN metric = 'gl_ps'       THEN period_end     END) AS period_gl_ps,
        MAX(CASE WHEN metric = 'gl_ps'       THEN numeric_value  END) AS gl_ps
    FROM fy_ranked
    WHERE rk = 1
    GROUP BY ticker
),

-- ── 5. Pivot prior (rk=2) FY values per ticker ────────────────────────────────
fy_prior AS (
    SELECT
        ticker,
        MAX(CASE WHEN metric = 'pik'         THEN period_end     END) AS period_pik,
        MAX(CASE WHEN metric = 'pik'         THEN numeric_value  END) AS pik,
        MAX(CASE WHEN metric = 'nii'         THEN numeric_value  END) AS nii,
        MAX(CASE WHEN metric = 'nii_ps'      THEN period_end     END) AS period_nii_ps,
        MAX(CASE WHEN metric = 'nii_ps'      THEN numeric_value  END) AS nii_ps,
        MAX(CASE WHEN metric = 'div_ps'      THEN numeric_value  END) AS div_ps,
        MAX(CASE WHEN metric = 'nav_ps'      THEN period_end     END) AS period_nav_ps,
        MAX(CASE WHEN metric = 'nav_ps'      THEN numeric_value  END) AS nav_ps,
        MAX(CASE WHEN metric = 'deprec'      THEN numeric_value  END) AS deprec,
        MAX(CASE WHEN metric = 'realized_gl' THEN numeric_value  END) AS realized_gl,
        MAX(CASE WHEN metric = 'gl_ps'       THEN period_end     END) AS period_gl_ps,
        MAX(CASE WHEN metric = 'gl_ps'       THEN numeric_value  END) AS gl_ps
    FROM fy_ranked
    WHERE rk = 2
    GROUP BY ticker
),

-- ── 6. Compute derived KPIs and period labels ──────────────────────────────────
kpis AS (
    SELECT
        c.ticker,
        COALESCE(cp.companyName, c.ticker)                             AS company_name,

        -- Period labels (use the period with the most available data as canonical)
        COALESCE(
            CASE WHEN p.period_nii_ps IS NOT NULL THEN CONCAT('FY ', YEAR(p.period_nii_ps)) END,
            CASE WHEN p.period_nav_ps IS NOT NULL THEN CONCAT('FY ', YEAR(p.period_nav_ps)) END,
            CASE WHEN p.period_pik    IS NOT NULL THEN CONCAT('FY ', YEAR(p.period_pik))    END,
            'Prior FY'
        )                                                              AS prior_period,
        COALESCE(
            CASE WHEN c.period_nii_ps IS NOT NULL THEN CONCAT('FY ', YEAR(c.period_nii_ps)) END,
            CASE WHEN c.period_nav_ps IS NOT NULL THEN CONCAT('FY ', YEAR(c.period_nav_ps)) END,
            CASE WHEN c.period_pik    IS NOT NULL THEN CONCAT('FY ', YEAR(c.period_pik))    END,
            'Current FY'
        )                                                              AS current_period,

        -- KPI 1: PIK/NII Ratio (%) — lower is better
        CASE WHEN c.pik IS NOT NULL AND c.nii IS NOT NULL AND c.nii <> 0
             THEN ROUND(c.pik / c.nii * 100, 1) END                   AS pik_nii_curr,
        CASE WHEN p.pik IS NOT NULL AND p.nii IS NOT NULL AND p.nii <> 0
             THEN ROUND(p.pik / p.nii * 100, 1) END                   AS pik_nii_prior,

        -- KPI 2: Dividend Coverage (%) — higher is better; alert < 100%
        CASE WHEN c.nii_ps IS NOT NULL AND c.div_ps IS NOT NULL AND c.div_ps <> 0
             THEN ROUND(c.nii_ps / c.div_ps * 100, 1) END             AS div_cov_curr,
        CASE WHEN p.nii_ps IS NOT NULL AND p.div_ps IS NOT NULL AND p.div_ps <> 0
             THEN ROUND(p.nii_ps / p.div_ps * 100, 1) END             AS div_cov_prior,

        -- KPI 3: NAV per Share ($) — higher is better
        c.nav_ps                                                       AS nav_curr,
        p.nav_ps                                                       AS nav_prior,

        -- KPI 4: Unrealized Deprec/NAV (%) — lower is better; alert ≥ 55%
        CASE WHEN c.deprec IS NOT NULL AND c.nav_ps IS NOT NULL AND c.nav_ps <> 0
                  AND c.nii IS NOT NULL AND c.nii_ps IS NOT NULL AND c.nii_ps <> 0
             THEN ROUND(ABS(c.deprec) / (c.nav_ps * (c.nii / c.nii_ps)) * 100, 1) END AS deprec_nav_curr,
        CASE WHEN p.deprec IS NOT NULL AND p.nav_ps IS NOT NULL AND p.nav_ps <> 0
                  AND p.nii IS NOT NULL AND p.nii_ps IS NOT NULL AND p.nii_ps <> 0
             THEN ROUND(ABS(p.deprec) / (p.nav_ps * (p.nii / p.nii_ps)) * 100, 1) END AS deprec_nav_prior,

        -- KPI 5: NII per Share ($) — higher is better
        c.nii_ps                                                       AS nii_ps_curr,
        p.nii_ps                                                       AS nii_ps_prior,

        -- KPI 6: Realized G/L per Share ($) — less negative / more positive is better
        c.gl_ps                                                        AS gl_ps_curr,
        p.gl_ps                                                        AS gl_ps_prior

    FROM fy_curr c
    LEFT JOIN fy_prior p      USING (ticker)
    LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp ON cp.symbol = c.ticker
),

-- ── 7. Unpivot to long format matching gold_app_company_fundamentals ───────────
unpivoted AS (

    -- KPI 1: PIK/NII Ratio
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        1                                                                        AS sort_order,
        'PIK/NII Ratio'                                                          AS kpi_name,
        CASE WHEN pik_nii_prior IS NOT NULL THEN CONCAT(pik_nii_prior, '%')      END AS prior_value,
        CASE WHEN pik_nii_curr  IS NOT NULL THEN CONCAT(pik_nii_curr,  '%')      END AS current_value,
        CASE WHEN pik_nii_curr IS NOT NULL AND pik_nii_prior IS NOT NULL AND pik_nii_prior <> 0
             THEN ROUND((pik_nii_curr - pik_nii_prior) / ABS(pik_nii_prior) * 100, 1) END AS change_pct,
        -- Lower is better: improvement = decrease = negative change_pct → flag 'up'
        CASE WHEN pik_nii_curr >= 30.0                                            THEN 'alert'
             WHEN pik_nii_curr IS NOT NULL AND pik_nii_prior IS NOT NULL
                  AND pik_nii_curr < pik_nii_prior                                THEN 'up'
             WHEN pik_nii_curr IS NOT NULL AND pik_nii_prior IS NOT NULL          THEN 'down'
             ELSE NULL END                                                         AS flag
    FROM kpis
    WHERE pik_nii_curr IS NOT NULL

    UNION ALL

    -- KPI 2: Dividend Coverage
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        2,
        'Dividend Coverage',
        CASE WHEN div_cov_prior IS NOT NULL THEN CONCAT(div_cov_prior, '%') END,
        CASE WHEN div_cov_curr  IS NOT NULL THEN CONCAT(div_cov_curr,  '%') END,
        CASE WHEN div_cov_curr IS NOT NULL AND div_cov_prior IS NOT NULL AND div_cov_prior <> 0
             THEN ROUND((div_cov_curr - div_cov_prior) / ABS(div_cov_prior) * 100, 1) END,
        -- Higher is better: improvement = increase = positive change → 'up'
        CASE WHEN div_cov_curr IS NOT NULL AND div_cov_curr < 100.0              THEN 'alert'
             WHEN div_cov_curr IS NOT NULL AND div_cov_prior IS NOT NULL
                  AND div_cov_curr > div_cov_prior                                THEN 'up'
             WHEN div_cov_curr IS NOT NULL AND div_cov_prior IS NOT NULL          THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE div_cov_curr IS NOT NULL

    UNION ALL

    -- KPI 3: NAV per Share
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        3,
        'NAV per Share',
        CASE WHEN nav_prior IS NOT NULL THEN CONCAT('$', ROUND(nav_prior, 2)) END,
        CASE WHEN nav_curr  IS NOT NULL THEN CONCAT('$', ROUND(nav_curr,  2)) END,
        CASE WHEN nav_curr IS NOT NULL AND nav_prior IS NOT NULL AND nav_prior <> 0
             THEN ROUND((nav_curr - nav_prior) / ABS(nav_prior) * 100, 1) END,
        CASE WHEN nav_curr IS NOT NULL AND nav_prior IS NOT NULL
                  AND nav_curr > nav_prior                                        THEN 'up'
             WHEN nav_curr IS NOT NULL AND nav_prior IS NOT NULL                  THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE nav_curr IS NOT NULL

    UNION ALL

    -- KPI 4: Unrealized Depreciation / NAV
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        4,
        'Unrealized Deprec/NAV',
        CASE WHEN deprec_nav_prior IS NOT NULL THEN CONCAT(deprec_nav_prior, '%') END,
        CASE WHEN deprec_nav_curr  IS NOT NULL THEN CONCAT(deprec_nav_curr,  '%') END,
        CASE WHEN deprec_nav_curr IS NOT NULL AND deprec_nav_prior IS NOT NULL AND deprec_nav_prior <> 0
             THEN ROUND((deprec_nav_curr - deprec_nav_prior) / ABS(deprec_nav_prior) * 100, 1) END,
        -- Lower is better
        CASE WHEN deprec_nav_curr >= 55.0                                         THEN 'alert'
             WHEN deprec_nav_curr IS NOT NULL AND deprec_nav_prior IS NOT NULL
                  AND deprec_nav_curr < deprec_nav_prior                          THEN 'up'
             WHEN deprec_nav_curr IS NOT NULL AND deprec_nav_prior IS NOT NULL    THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE deprec_nav_curr IS NOT NULL

    UNION ALL

    -- KPI 5: NII per Share
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        5,
        'NII per Share',
        CASE WHEN nii_ps_prior IS NOT NULL THEN CONCAT('$', ROUND(nii_ps_prior, 2)) END,
        CASE WHEN nii_ps_curr  IS NOT NULL THEN CONCAT('$', ROUND(nii_ps_curr,  2)) END,
        CASE WHEN nii_ps_curr IS NOT NULL AND nii_ps_prior IS NOT NULL AND nii_ps_prior <> 0
             THEN ROUND((nii_ps_curr - nii_ps_prior) / ABS(nii_ps_prior) * 100, 1) END,
        -- Higher is better
        CASE WHEN nii_ps_curr IS NOT NULL AND nii_ps_prior IS NOT NULL
                  AND nii_ps_curr > nii_ps_prior                                  THEN 'up'
             WHEN nii_ps_curr IS NOT NULL AND nii_ps_prior IS NOT NULL            THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE nii_ps_curr IS NOT NULL

    UNION ALL

    -- KPI 6: Realized G/L per Share
    SELECT ticker AS symbol, company_name, prior_period, current_period,
        6,
        'Realized G/L per Share',
        CASE WHEN gl_ps_prior IS NOT NULL THEN CONCAT('$', ROUND(gl_ps_prior, 2)) END,
        CASE WHEN gl_ps_curr  IS NOT NULL THEN CONCAT('$', ROUND(gl_ps_curr,  2)) END,
        CASE WHEN gl_ps_curr IS NOT NULL AND gl_ps_prior IS NOT NULL AND gl_ps_prior <> 0
             THEN ROUND((gl_ps_curr - gl_ps_prior) / ABS(gl_ps_prior) * 100, 1) END,
        -- Less negative / more positive = better
        CASE WHEN gl_ps_curr IS NOT NULL AND gl_ps_prior IS NOT NULL
                  AND gl_ps_curr > gl_ps_prior                                    THEN 'up'
             WHEN gl_ps_curr IS NOT NULL AND gl_ps_prior IS NOT NULL              THEN 'down'
             ELSE NULL END
    FROM kpis
    WHERE gl_ps_curr IS NOT NULL
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

bdc_df = spark.sql(bdc_fundamentals_sql)
display(bdc_df)

# COMMAND ----------

# Sanity checks
print("=== Row counts per ticker ===")
display(bdc_df.groupBy("symbol", "company_name", "current_period", "prior_period")
        .count()
        .orderBy("symbol"))

# COMMAND ----------

print("=== KPI coverage — metrics present per ticker ===")
display(bdc_df.groupBy("kpi_name")
        .agg(
            {"symbol": "count", "current_value": "count", "prior_value": "count"}
        )
        .withColumnRenamed("count(symbol)", "ticker_count")
        .withColumnRenamed("count(current_value)", "has_current")
        .withColumnRenamed("count(prior_value)", "has_prior")
        .orderBy("kpi_name"))

# COMMAND ----------

print("=== Sample rows — ARCC ===")
display(bdc_df.filter("symbol = 'ARCC'"))

# COMMAND ----------

print("=== Alert flags ===")
display(bdc_df.filter("flag = 'alert'").orderBy("symbol", "sort_order"))

# COMMAND ----------

# DBTITLE 1,Compare schema to gold_app_company_fundamentals
print("=== Schema comparison ===")
print("\nBDC fundamentals schema:")
bdc_df.printSchema()

ref_df = spark.sql("SELECT * FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_company_fundamentals LIMIT 0".format(
    UC_CATALOG=UC_CATALOG, UC_SCHEMA=UC_SCHEMA))
print("\ngold_app_company_fundamentals schema:")
ref_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Write to table
bdc_df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals")
print(f"Written {bdc_df.count()} rows to {UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals")

# COMMAND ----------

display(spark.sql(f"""
    SELECT symbol, company_name, current_period, prior_period, kpi_name,
           prior_value, current_value, change_pct, flag
    FROM {UC_CATALOG}.{UC_SCHEMA}.gold_app_bdc_fundamentals
    ORDER BY symbol, sort_order
"""))
