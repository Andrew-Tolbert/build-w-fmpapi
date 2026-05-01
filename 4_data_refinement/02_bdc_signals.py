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
# Source tables (populated by 3_ingest_data/3_EDGAR/bdc_early_warning_sql.py):
#   bdc_time_series  — long format, every data point (ticker, metric, period_end, value)
#   bdc_fy_snapshot  — wide format, latest FY value per metric per ticker
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'bdc_early_warning')
#
# One row per (symbol, metric, signal_date) — six rows per BDC company covering all
# T1/T2/T3 signals.  Each row is independently dated (quarterly for T2 NAV, annual FY
# for the rest) so signal_date reflects the true data vintage of each measurement.
#
# Column layout:
#   signal_type  = 'Private Credit Health'  (category, consistent across all BDC rows)
#   signal       = XBRL/GAAP concept name   (granular measure driving this metric)
#   source_id    = '{symbol}|bdc|{metric_key}'  (pivot key for dashboard rebuilds)
#
# The dashboard pivot query on gold_unified_signals:
#   SELECT symbol,
#     MAX(CASE WHEN source_id LIKE '%t1_pik_nii'     THEN source_description END) AS t1_pik,
#     MAX(CASE WHEN source_id LIKE '%t1_div_coverage' THEN source_description END) AS t1_div,
#     ...
#   FROM gold_unified_signals
#   WHERE source_type = 'bdc_early_warning'
#   GROUP BY symbol
#
# This view is materialised as gold_bdc_early_warnings (CREATE OR REPLACE VIEW) with
# stoplight emojis replacing the raw signal-level text.
#
# Idempotency: MERGE on signal_id = md5(symbol|bdc|metric_key|signal_date).
# A new FY/quarterly period produces a new row; re-runs on unchanged data update in place.
#
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
        source_description    STRING,
        sentiment             STRING,
        severity_score        DOUBLE,
        advisor_action_needed BOOLEAN,
        signal_type           STRING,
        signal                STRING,
        signal_value          STRING,
        rationale             STRING,
        processed_at          TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

try:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals ADD COLUMN signal STRING")
    print("Added signal column to gold_unified_signals.")
except Exception:
    pass  # column already exists

# COMMAND ----------

_ts_count = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series").collect()[0][0]
_fy_count = spark.sql(f"SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot").collect()[0][0]
print(f"bdc_time_series rows: {_ts_count}  |  bdc_fy_snapshot rows: {_fy_count}")

if _ts_count == 0 or _fy_count == 0:
    raise Exception(
        "Source tables are empty. Run 3_ingest_data/3_EDGAR/bdc_early_warning_sql.py first."
    )

# COMMAND ----------

# One row per (symbol, metric_key, signal_date) merged into gold_unified_signals.
#   signal_type  = 'Private Credit Health'  (the category)
#   signal       = XBRL/GAAP concept name   (the granular measure)
#   source_id    = '{ticker}|bdc|{metric_key}'  (pivot key)
#   source_description = formatted value + signal level + SEC filing provenance

spark.sql(f"""
    MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
    USING (

        WITH

        -- ── Per-metric latest dates ────────────────────────────────────────────
        -- Each metric uses its own most-recent period_end so signal_date is accurate.
        fy_dates AS (
            SELECT ticker, CAST(MAX(period_end) AS DATE) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE fiscal_period = 'FY'
            GROUP BY ticker
        ),
        nav_dates AS (
            SELECT ticker, CAST(MAX(period_end) AS DATE) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'nav_ps'
            GROUP BY ticker
        ),
        rl_dates AS (
            SELECT ticker, CAST(MAX(period_end) AS DATE) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'realized_gl' AND fiscal_period = 'FY'
            GROUP BY ticker
        ),
        gl_dates AS (
            SELECT ticker, CAST(MAX(period_end) AS DATE) AS signal_date
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'gl_ps' AND fiscal_period = 'FY'
            GROUP BY ticker
        ),

        -- ── Most recent 10-K or 10-Q per ticker from the SEC filings log ─────────
        -- 10-K preferred (more complete); falls back to 10-Q when no 10-K is present.
        sec_source AS (
            SELECT symbol, form_type, accession, CAST(filing_date AS DATE) AS filing_date
            FROM (
                SELECT symbol, form_type, accession, filing_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol
                           ORDER BY
                               CASE form_type WHEN '10-K' THEN 0 ELSE 1 END,
                               filing_date DESC
                       ) AS rn
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log
                WHERE form_type IN ('10-K', '10-Q')
                  AND accession != 'NOACC'
            ) WHERE rn = 1
        ),

        -- ── T1: PIK-to-NII ratio ───────────────────────────────────────────────
        t1_pik AS (
            SELECT ticker, ROUND(pik / nii * 100, 1) AS metric_value,
                CASE WHEN pik/nii >= 0.30 THEN 'CONCERN'
                     WHEN pik/nii >= 0.20 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE pik IS NOT NULL AND nii IS NOT NULL AND nii <> 0
        ),

        -- ── T1: Dividend coverage ──────────────────────────────────────────────
        t1_div AS (
            SELECT ticker, ROUND(nii_ps / div_ps * 100, 1) AS metric_value,
                CASE WHEN nii_ps/div_ps <= 1.00 THEN 'CONCERN'
                     WHEN nii_ps/div_ps <= 1.05 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE nii_ps IS NOT NULL AND div_ps IS NOT NULL AND div_ps <> 0
        ),

        -- ── T2: Consecutive quarterly NAV declines ─────────────────────────────
        nav_lagged AS (
            SELECT ticker, period_end, numeric_value AS nav,
                LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series WHERE metric = 'nav_ps'
        ),
        nav_flagged AS (
            SELECT ticker,
                CASE WHEN nav < prev_nav THEN 1 ELSE 0 END AS is_decline,
                CASE WHEN nav >= prev_nav THEN 1 ELSE 0 END AS streak_break,
                ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
            FROM nav_lagged WHERE prev_nav IS NOT NULL
        ),
        nav_streak AS (
            SELECT ticker,
                SUM(is_decline) OVER (
                    PARTITION BY ticker ORDER BY rn
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS running_declines,
                streak_break
            FROM nav_flagged
        ),
        t2_nav AS (
            SELECT ticker,
                CAST(COALESCE(MIN(CASE WHEN streak_break = 1 THEN running_declines - 1 END), 0) AS DOUBLE) AS metric_value,
                CASE WHEN COALESCE(MIN(CASE WHEN streak_break=1 THEN running_declines-1 END),0) >= 6 THEN 'CONCERN'
                     WHEN COALESCE(MIN(CASE WHEN streak_break=1 THEN running_declines-1 END),0) >= 3 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM nav_streak GROUP BY ticker
        ),

        -- ── T2: Unrealized depreciation / estimated net assets ─────────────────
        t2_dep AS (
            SELECT ticker, ROUND(deprec / (nav_ps * (nii / nii_ps)) * 100, 1) AS metric_value,
                CASE WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.55 THEN 'CONCERN'
                     WHEN deprec / (nav_ps * (nii / nii_ps)) >= 0.40 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot
            WHERE deprec IS NOT NULL
              AND nav_ps IS NOT NULL AND nav_ps <> 0
              AND nii    IS NOT NULL AND nii    <> 0
              AND nii_ps IS NOT NULL AND nii_ps <> 0
        ),

        -- ── T3: Realized loss acceleration (YoY) ──────────────────────────────
        rl_losses AS (
            SELECT ticker, period_end,
                ABS(LEAST(numeric_value, 0)) AS loss_abs,
                LAG(ABS(LEAST(numeric_value, 0))) OVER (PARTITION BY ticker ORDER BY period_end) AS prior_loss_abs
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'realized_gl' AND fiscal_period = 'FY'
        ),
        t3_rl AS (
            SELECT ticker, ROUND(loss_abs / prior_loss_abs, 2) AS metric_value,
                CASE WHEN loss_abs / prior_loss_abs >= 4.0 THEN 'CONCERN'
                     WHEN loss_abs / prior_loss_abs >= 2.0 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY period_end DESC) AS rn
                FROM rl_losses WHERE prior_loss_abs IS NOT NULL AND prior_loss_abs > 0
            ) WHERE rn = 1
        ),

        -- ── T3: Cumulative per-share losses / current NAV ──────────────────────
        cum_losses AS (
            SELECT ticker, ABS(SUM(LEAST(numeric_value, 0))) AS cumulative_loss_ps
            FROM {UC_CATALOG}.{UC_SCHEMA}.bdc_time_series
            WHERE metric = 'gl_ps' AND fiscal_period = 'FY'
            GROUP BY ticker
        ),
        t3_cum AS (
            SELECT c.ticker, ROUND(c.cumulative_loss_ps / s.nav_ps * 100, 1) AS metric_value,
                CASE WHEN c.cumulative_loss_ps / s.nav_ps >= 1.00 THEN 'CONCERN'
                     WHEN c.cumulative_loss_ps / s.nav_ps >= 0.50 THEN 'WATCH'
                     ELSE 'OK' END AS signal_level
            FROM cum_losses c
            JOIN {UC_CATALOG}.{UC_SCHEMA}.bdc_fy_snapshot s USING (ticker)
            WHERE s.nav_ps IS NOT NULL AND s.nav_ps > 0
        ),

        -- ── UNION: one row per metric per company ──────────────────────────────
        -- signal_type (XBRL concept) carries the granular measure name per metric.
        -- filing_form/filing_date/filing_accession come from sec_source for provenance.
        all_metrics AS (
            SELECT ticker, 't1_pik_nii'      AS metric_key, 'T1 PIK/NII'         AS metric_name,
                   metric_value, '%'          AS unit, signal_level,
                   COALESCE(fd.signal_date, CURRENT_DATE())              AS signal_date,
                   'PIK-to-NII Ratio'                                    AS xbrl_concept,
                   sf.form_type AS filing_form, sf.filing_date AS filing_date, sf.accession AS filing_accession
            FROM t1_pik
            LEFT JOIN fy_dates fd  USING (ticker)
            LEFT JOIN sec_source sf ON t1_pik.ticker = sf.symbol

            UNION ALL

            SELECT ticker, 't1_div_coverage', 'T1 Div Coverage',
                   metric_value, '%', signal_level,
                   COALESCE(fd.signal_date, CURRENT_DATE()),
                   'Dividend Coverage Ratio',
                   sf.form_type, sf.filing_date, sf.accession
            FROM t1_div
            LEFT JOIN fy_dates fd  USING (ticker)
            LEFT JOIN sec_source sf ON t1_div.ticker = sf.symbol

            UNION ALL

            SELECT ticker, 't2_nav_trend', 'T2 NAV Trend',
                   metric_value, ' qtrs', signal_level,
                   COALESCE(nd.signal_date, CURRENT_DATE()),
                   'NAV Trajectory',
                   sf.form_type, sf.filing_date, sf.accession
            FROM t2_nav
            LEFT JOIN nav_dates nd USING (ticker)
            LEFT JOIN sec_source sf ON t2_nav.ticker = sf.symbol

            UNION ALL

            SELECT ticker, 't2_deprec_nav', 'T2 Deprec/NAV',
                   metric_value, '%', signal_level,
                   COALESCE(fd.signal_date, CURRENT_DATE()),
                   'Unrealized Depreciation / Net Assets',
                   sf.form_type, sf.filing_date, sf.accession
            FROM t2_dep
            LEFT JOIN fy_dates fd  USING (ticker)
            LEFT JOIN sec_source sf ON t2_dep.ticker = sf.symbol

            UNION ALL

            SELECT ticker, 't3_rl_accel', 'T3 RL Acceleration',
                   metric_value, 'x', signal_level,
                   COALESCE(rd.signal_date, CURRENT_DATE()),
                   'Realized Loss Acceleration',
                   sf.form_type, sf.filing_date, sf.accession
            FROM t3_rl
            LEFT JOIN rl_dates rd  USING (ticker)
            LEFT JOIN sec_source sf ON t3_rl.ticker = sf.symbol

            UNION ALL

            SELECT ticker, 't3_cum_loss', 'T3 Cum Loss/NAV',
                   metric_value, '%', signal_level,
                   COALESCE(gd.signal_date, CURRENT_DATE()),
                   'Cumulative Losses vs. NAV',
                   sf.form_type, sf.filing_date, sf.accession
            FROM t3_cum
            LEFT JOIN gl_dates gd  USING (ticker)
            LEFT JOIN sec_source sf ON t3_cum.ticker = sf.symbol
        ),

        -- ── AI rationale per metric ────────────────────────────────────────────
        enriched AS (
            SELECT *,
                TRIM(ai_query(
                    '{LLM_ENDPOINT}',
                    CONCAT(
                        'In 1-2 sentences, explain the investment significance of this BDC metric ',
                        'for a Goldman Sachs UHNW wealth advisor. Be specific and direct.\\n',
                        'Company: ', ticker,
                        ' | Metric: ', metric_name, ' (', xbrl_concept, ')',
                        ' | Value: ', CAST(metric_value AS STRING), unit,
                        ' | Signal level: ', signal_level,
                        CASE WHEN filing_form IS NOT NULL
                             THEN CONCAT(' | Source: ', filing_form, ' filed ', CAST(filing_date AS STRING))
                             ELSE '' END
                    )
                )) AS rationale
            FROM all_metrics
        )

        SELECT
            md5(CONCAT(ticker, '|bdc|', metric_key, '|', CAST(signal_date AS STRING))) AS signal_id,
            ticker                                                    AS symbol,
            signal_date,
            'bdc_early_warning'                                       AS source_type,
            CASE WHEN filing_form IS NOT NULL
                 THEN CONCAT(filing_form, ' Q', QUARTER(filing_date), ' ', YEAR(filing_date))
                 ELSE NULL
            END                                                       AS source_description,
            CASE signal_level
                WHEN 'CONCERN' THEN 'Negative'
                WHEN 'WATCH'   THEN 'Mixed'
                ELSE                'Neutral'
            END                                                       AS sentiment,
            CASE signal_level
                WHEN 'CONCERN' THEN 0.85
                WHEN 'WATCH'   THEN 0.55
                ELSE                0.15
            END                                                       AS severity_score,
            signal_level = 'CONCERN'                                  AS advisor_action_needed,
            'Private Credit Health'                                   AS signal_type,
            xbrl_concept                                              AS signal,
            CONCAT(CAST(metric_value AS STRING), unit, ' (', signal_level, ')') AS signal_value,
            rationale,
            CURRENT_TIMESTAMP()                                       AS processed_at
        FROM enriched

    ) AS src
    ON tgt.signal_id = src.signal_id
    WHEN MATCHED THEN UPDATE SET
        tgt.source_description    = src.source_description,
        tgt.sentiment             = src.sentiment,
        tgt.severity_score        = src.severity_score,
        tgt.advisor_action_needed = src.advisor_action_needed,
        tgt.signal_type           = src.signal_type,
        tgt.signal                = src.signal,
        tgt.signal_value          = src.signal_value,
        tgt.rationale             = src.rationale,
        tgt.processed_at          = src.processed_at
    WHEN NOT MATCHED THEN INSERT (
        signal_id, symbol, signal_date, source_type, source_description,
        sentiment, severity_score, advisor_action_needed, signal_type, signal, signal_value, rationale, processed_at
    ) VALUES (
        src.signal_id, src.symbol, src.signal_date, src.source_type,
        src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
        src.signal_type, src.signal, src.signal_value, src.rationale, src.processed_at
    )
""")

print("BDC signals merged into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, signal, signal_value,
               sentiment, severity_score, advisor_action_needed,
               LEFT(rationale, 150) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'bdc_early_warning'
        ORDER BY symbol, signal
    """)
)

# COMMAND ----------

# ── BDC early warning dashboard view ─────────────────────────────────────────
# Pivots gold_unified_signals back to the T1/T2/T3 column layout.
# Stoplight emojis replace the raw signal-level text so the table reads at a
# glance, matching the pattern used in bdc_early_warning_sql.py.

spark.sql(f"""
    CREATE OR REPLACE VIEW {UC_CATALOG}.{UC_SCHEMA}.gold_bdc_early_warnings AS
    WITH flagged AS (
        SELECT
            symbol,
            signal_date,
            signal,
            -- Prepend stoplight emoji based on signal_value suffix
            CASE
                WHEN signal_value LIKE '%(CONCERN)' THEN CONCAT('🔴 ', signal_value)
                WHEN signal_value LIKE '%(WATCH)'   THEN CONCAT('🟡 ', signal_value)
                ELSE                                     CONCAT('🟢 ', signal_value)
            END AS display_value,
            severity_score,
            advisor_action_needed
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'bdc_early_warning'
    )
    SELECT
        symbol,
        MAX(signal_date)                                                                                        AS signal_date,
        COALESCE(MAX(CASE WHEN signal = 'PIK-to-NII Ratio'                   THEN display_value END), '⚪ N/A') AS `T1 PIK/NII`,
        COALESCE(MAX(CASE WHEN signal = 'Dividend Coverage Ratio'            THEN display_value END), '⚪ N/A') AS `T1 Div Coverage`,
        COALESCE(MAX(CASE WHEN signal = 'NAV Trajectory'                     THEN display_value END), '⚪ N/A') AS `T2 NAV Trend`,
        COALESCE(MAX(CASE WHEN signal = 'Unrealized Depreciation / Net Assets' THEN display_value END), '⚪ N/A') AS `T2 Deprec/NAV`,
        COALESCE(MAX(CASE WHEN signal = 'Realized Loss Acceleration'         THEN display_value END), '⚪ N/A') AS `T3 RL Acceleration`,
        COALESCE(MAX(CASE WHEN signal = 'Cumulative Losses vs. NAV'          THEN display_value END), '⚪ N/A') AS `T3 Cum Loss/NAV`,
        MAX(severity_score)                                                                                     AS worst_severity,
        SUM(CASE WHEN advisor_action_needed THEN 1 ELSE 0 END)                                                  AS concern_count
    FROM flagged
    GROUP BY symbol
    ORDER BY worst_severity DESC, concern_count DESC
""")

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.gold_bdc_early_warnings"))

# COMMAND ----------
