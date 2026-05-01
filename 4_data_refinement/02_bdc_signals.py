# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from BDC (Business Development Company) early-warning metrics.
#
# Computes T1/T2/T3 warning signals from bdc_time_series and bdc_fy_snapshot
# (tables populated by 3_ingest_data/3_EDGAR/bdc_early_warning_sql.py), then maps
# each ticker to a single current-state signal row in gold_unified_signals.
#
# Signal framework (from bdc_early_warning_sql.py):
#   T1: PIK/NII > 20% (WATCH) / 30% (CONCERN) — cash-flow stress
#   T1: Dividend coverage NII/div < 105% (WATCH) / 100% (CONCERN)
#   T2: NAV consecutive quarterly declines >= 3 (WATCH) / 6 (CONCERN)
#   T2: Unrealized depreciation/NAV > 40% (WATCH) / 55% (CONCERN)
#   T3: Realized loss YoY > 2x (WATCH) / 4x (CONCERN)
#   T3: Cumulative per-share losses/NAV > 50% (WATCH) / 100% (CONCERN)
#
# severity_score mapping:
#   Any CONCERN signal → 0.85 | Only WATCH signals → 0.55 | All OK → 0.15
#
# Idempotency: one signal row per BDC ticker (signal_id = md5(symbol|bdc_early_warning)).
# Each run refreshes the row with the latest metrics — no historical accumulation.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'bdc_early_warning')
# Run after: 3_ingest_data/3_EDGAR/bdc_early_warning_sql.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

# COMMAND ----------

# # Uncomment to reset BDC signals only
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE source_type = 'bdc_early_warning'")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals (
        signal_id             STRING,
        symbol                STRING,
        signal_date           DATE,
        source_type           STRING,
        source_id             STRING,
        source_description    STRING,
        sentiment             STRING,
        severity_score        DOUBLE,
        advisor_action_needed BOOLEAN,
        signal_type           STRING,
        rationale             STRING,
        processed_at          TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

# Verify source tables exist before running
_ts_count  = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series").collect()[0][0]
_fy_count  = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot").collect()[0][0]
print(f"bdc_time_series rows: {_ts_count}  |  bdc_fy_snapshot rows: {_fy_count}")

if _ts_count == 0 or _fy_count == 0:
    raise Exception(
        "Source tables are empty. Run 3_ingest_data/3_EDGAR/bdc_early_warning_sql.py first."
    )

# COMMAND ----------

# Compute all T1/T2/T3 signals inline.  Logic mirrors the full dashboard CTE in
# bdc_early_warning_sql.py, reproduced here so this notebook is self-contained.

spark.sql(f"""
    MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
    USING (

        WITH

        -- ── Latest signal_date per ticker ──────────────────────────────────────
        latest_period AS (
            SELECT ticker, CAST(MAX(period_end) AS DATE) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            GROUP BY ticker
        ),

        -- ── T1: PIK-to-NII ratio ───────────────────────────────────────────────
        t1_pik AS (
            SELECT ticker, cik,
                ROUND(pik / nii * 100, 1) AS pik_nii_pct,
                CASE WHEN pik/nii >= 0.30 THEN 'CONCERN'
                     WHEN pik/nii >= 0.20 THEN 'WATCH'
                     ELSE 'OK' END AS t1_pik
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE pik IS NOT NULL AND nii IS NOT NULL AND nii <> 0
        ),

        -- ── T1: Dividend coverage ──────────────────────────────────────────────
        t1_div AS (
            SELECT ticker, cik,
                ROUND(nii_ps / div_ps * 100, 1) AS div_coverage_pct,
                CASE WHEN nii_ps/div_ps <= 1.00 THEN 'CONCERN'
                     WHEN nii_ps/div_ps <= 1.05 THEN 'WATCH'
                     ELSE 'OK' END AS t1_div
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE nii_ps IS NOT NULL AND div_ps IS NOT NULL AND div_ps <> 0
        ),

        -- ── T2: NAV consecutive quarterly declines (gap-and-islands) ──────────
        nav_lagged AS (
            SELECT ticker, cik, period_end,
                numeric_value AS nav,
                LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series WHERE metric = 'nav_ps'
        ),
        nav_flagged AS (
            SELECT ticker, cik,
                CASE WHEN nav < prev_nav THEN 1 ELSE 0 END AS is_decline,
                CASE WHEN nav >= prev_nav THEN 1 ELSE 0 END AS streak_break,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
            FROM nav_lagged WHERE prev_nav IS NOT NULL
        ),
        nav_streak AS (
            SELECT ticker, cik,
                SUM(is_decline) OVER (
                    PARTITION BY ticker ORDER BY rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_declines,
                streak_break, rn
            FROM nav_flagged
        ),
        t2_nav AS (
            SELECT ticker, cik,
                COALESCE(MIN(CASE WHEN streak_break = 1 THEN running_declines - 1 END), 0) AS consec_declines,
                CASE WHEN COALESCE(MIN(CASE WHEN streak_break=1 THEN running_declines-1 END),0) >= 6 THEN 'CONCERN'
                     WHEN COALESCE(MIN(CASE WHEN streak_break=1 THEN running_declines-1 END),0) >= 3 THEN 'WATCH'
                     ELSE 'OK' END AS t2_nav
            FROM nav_streak GROUP BY ticker, cik
        ),

        -- ── T2: Unrealized depreciation / net assets ───────────────────────────
        -- net_assets unavailable from EDGAR; estimated as nav_ps * (nii / nii_ps)
        t2_dep AS (
            SELECT ticker, cik,
                ROUND(deprec / (nav_ps * (nii / nii_ps)) * 100, 1) AS deprec_pct_nav,
                CASE WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.55 THEN 'CONCERN'
                     WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.40 THEN 'WATCH'
                     ELSE 'OK' END AS t2_dep
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE deprec IS NOT NULL
              AND nav_ps IS NOT NULL AND nav_ps <> 0
              AND nii    IS NOT NULL AND nii    <> 0
              AND nii_ps IS NOT NULL AND nii_ps <> 0
        ),

        -- ── T3: Realized loss acceleration ────────────────────────────────────
        rl_losses AS (
            SELECT ticker, cik, period_end,
                ABS(LEAST(numeric_value, 0)) AS loss_abs,
                LAG(ABS(LEAST(numeric_value, 0))) OVER (PARTITION BY ticker ORDER BY period_end) AS prior_loss_abs
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'realized_gl' AND fiscal_period = 'FY'
        ),
        rl_latest AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
            FROM rl_losses
            WHERE prior_loss_abs IS NOT NULL AND prior_loss_abs > 0
        ),
        t3_rl AS (
            SELECT ticker, cik,
                ROUND(loss_abs / prior_loss_abs, 2) AS rl_yoy_multiple,
                CASE WHEN loss_abs / prior_loss_abs >= 4.0 THEN 'CONCERN'
                     WHEN loss_abs / prior_loss_abs >= 2.0 THEN 'WATCH'
                     ELSE 'OK' END AS t3_rl
            FROM rl_latest WHERE rn = 1
        ),

        -- ── T3: Cumulative per-share losses / current NAV ──────────────────────
        cum_losses AS (
            SELECT ticker, cik,
                ABS(SUM(LEAST(numeric_value, 0))) AS cumulative_loss_ps
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'gl_ps' AND fiscal_period = 'FY'
            GROUP BY ticker, cik
        ),
        t3_cum AS (
            SELECT c.ticker, c.cik,
                ROUND(c.cumulative_loss_ps / s.nav_ps * 100, 1) AS cum_loss_pct_nav,
                CASE WHEN c.cumulative_loss_ps / s.nav_ps >= 1.00 THEN 'CONCERN'
                     WHEN c.cumulative_loss_ps / s.nav_ps >= 0.50 THEN 'WATCH'
                     ELSE 'OK' END AS t3_cum
            FROM cum_losses c
            JOIN {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot s USING (ticker, cik)
            WHERE s.nav_ps IS NOT NULL AND s.nav_ps > 0
        ),

        -- ── Full dashboard join ────────────────────────────────────────────────
        dashboard AS (
            SELECT
                s.ticker,
                COALESCE(t1_pik.t1_pik,  'OK') AS t1_pik,  COALESCE(t1_pik.pik_nii_pct,     NULL) AS pik_nii_pct,
                COALESCE(t1_div.t1_div,  'OK') AS t1_div,  COALESCE(t1_div.div_coverage_pct, NULL) AS div_coverage_pct,
                COALESCE(t2_nav.t2_nav,  'OK') AS t2_nav,  COALESCE(t2_nav.consec_declines,     0) AS consec_declines,
                COALESCE(t2_dep.t2_dep,  'OK') AS t2_dep,  COALESCE(t2_dep.deprec_pct_nav,   NULL) AS deprec_pct_nav,
                COALESCE(t3_rl.t3_rl,    'OK') AS t3_rl,   COALESCE(t3_rl.rl_yoy_multiple,   NULL) AS rl_yoy_multiple,
                COALESCE(t3_cum.t3_cum,  'OK') AS t3_cum,  COALESCE(t3_cum.cum_loss_pct_nav, NULL) AS cum_loss_pct_nav,
                COALESCE(lp.signal_date, CURRENT_DATE()) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot s
            LEFT JOIN t1_pik  USING (ticker, cik)
            LEFT JOIN t1_div  USING (ticker, cik)
            LEFT JOIN t2_nav  USING (ticker, cik)
            LEFT JOIN t2_dep  USING (ticker, cik)
            LEFT JOIN t3_rl   USING (ticker, cik)
            LEFT JOIN t3_cum  USING (ticker, cik)
            LEFT JOIN latest_period lp ON s.ticker = lp.ticker
        ),

        -- ── Severity and advisor action derived from worst signal ──────────────
        scored AS (
            SELECT *,
                CASE
                    WHEN t1_pik = 'CONCERN' OR t1_div = 'CONCERN' OR t2_nav = 'CONCERN' OR
                         t2_dep = 'CONCERN' OR t3_rl  = 'CONCERN' OR t3_cum = 'CONCERN' THEN 0.85
                    WHEN t1_pik = 'WATCH'   OR t1_div = 'WATCH'   OR t2_nav = 'WATCH'   OR
                         t2_dep = 'WATCH'   OR t3_rl  = 'WATCH'   OR t3_cum = 'WATCH'   THEN 0.55
                    ELSE 0.15
                END AS severity_score,
                CASE
                    WHEN t1_pik = 'CONCERN' OR t1_div = 'CONCERN' OR t2_nav = 'CONCERN' OR
                         t2_dep = 'CONCERN' OR t3_rl  = 'CONCERN' OR t3_cum = 'CONCERN' THEN true
                    ELSE false
                END AS advisor_action_needed
            FROM dashboard
        ),

        -- ── AI-generated rationale (one call per ticker) ───────────────────────
        with_rationale AS (
            SELECT *,
                TRIM(ai_query(
                    '{LLM_ENDPOINT}',
                    CONCAT(
                        'You are a Goldman Sachs wealth advisor assistant analyzing BDC early-warning signals ',
                        'for a private credit / BDC holding. Write exactly 2-3 sentences explaining what ',
                        'the following signals indicate and whether the advisor should take action for ', ticker, '.\\n\\n',
                        'T1 PIK/NII Ratio:             ', COALESCE(CAST(pik_nii_pct AS STRING),     'N/A'), '%  (', t1_pik, ')\\n',
                        'T1 Dividend Coverage:          ', COALESCE(CAST(div_coverage_pct AS STRING), 'N/A'), '%  (', t1_div, ')\\n',
                        'T2 NAV Consecutive Declines:   ', CAST(consec_declines AS STRING), ' quarters (', t2_nav, ')\\n',
                        'T2 Unrealized Depreciation/NAV:', COALESCE(CAST(deprec_pct_nav AS STRING),  'N/A'), '%  (', t2_dep, ')\\n',
                        'T3 Realized Loss YoY Multiple: ', COALESCE(CAST(rl_yoy_multiple AS STRING), 'N/A'), 'x  (', t3_rl,  ')\\n',
                        'T3 Cumulative Loss/NAV:        ', COALESCE(CAST(cum_loss_pct_nav AS STRING),'N/A'), '%  (', t3_cum, ')\\n\\n',
                        'Focus on the most concerning signals. Be direct and actionable.',
                        ' Do not start with "I" or repeat all the metric values.'
                    )
                )) AS rationale
            FROM scored
        )

        SELECT
            md5(CONCAT(ticker, '|bdc_early_warning'))       AS signal_id,
            ticker                                           AS symbol,
            signal_date,
            'bdc_early_warning'                              AS source_type,
            CONCAT(ticker, '|bdc_early_warning')             AS source_id,
            CONCAT('BDC Early Warning: ', ticker)            AS source_description,
            CASE
                WHEN t1_pik = 'CONCERN' OR t1_div = 'CONCERN' OR t2_nav = 'CONCERN' OR
                     t2_dep = 'CONCERN' OR t3_rl  = 'CONCERN' OR t3_cum = 'CONCERN' THEN 'Negative'
                WHEN t1_pik = 'WATCH'   OR t1_div = 'WATCH'   OR t2_nav = 'WATCH'   OR
                     t2_dep = 'WATCH'   OR t3_rl  = 'WATCH'   OR t3_cum = 'WATCH'   THEN 'Mixed'
                ELSE 'Neutral'
            END                                              AS sentiment,
            severity_score,
            advisor_action_needed,
            'Credit Event'                                   AS signal_type,
            rationale,
            CURRENT_TIMESTAMP()                              AS processed_at
        FROM with_rationale

    ) AS src
    ON tgt.signal_id = src.signal_id
    WHEN MATCHED THEN UPDATE SET
        tgt.signal_date           = src.signal_date,
        tgt.sentiment             = src.sentiment,
        tgt.severity_score        = src.severity_score,
        tgt.advisor_action_needed = src.advisor_action_needed,
        tgt.signal_type           = src.signal_type,
        tgt.rationale             = src.rationale,
        tgt.processed_at          = src.processed_at
    WHEN NOT MATCHED THEN INSERT (
        signal_id, symbol, signal_date, source_type, source_id, source_description,
        sentiment, severity_score, advisor_action_needed, signal_type, rationale, processed_at
    ) VALUES (
        src.signal_id, src.symbol, src.signal_date, src.source_type, src.source_id,
        src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
        src.signal_type, src.rationale, src.processed_at
    )
""")

print("BDC signals merged into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, sentiment, severity_score, advisor_action_needed,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'bdc_early_warning'
        ORDER BY severity_score DESC, symbol
    """)
)

# COMMAND ----------
