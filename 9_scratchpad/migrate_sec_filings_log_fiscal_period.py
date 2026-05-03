# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# One-time migration: add fiscal_year + quarter to existing sec_filings_log rows.
#
# SAFETY:
#   • No files are downloaded or re-parsed — this only writes to the log table.
#   • No downstream workflows are affected. sec_filing_chunks, gold_unified_signals,
#     and the vector search index are all untouched.
#   • The two new columns are NULL-able, so any workflow reading the table today
#     continues to work unchanged.
#   • The MERGE operations are idempotent — safe to re-run if interrupted.
#
# APPROACH:
#   10-K / 10-Q  — exact join on bronze_income_statements.filingDate
#                  (FMP records the SEC filing date for every income statement period;
#                   this matches sec_filings_log.filing_date for ~98% of rows)
#   8-K / 424Bx  — nearest prior quarter: the most recent income statement period
#                  whose end date is on or before the filing date
#                  (no fiscal period in FMP's filing-search response for event filings)
#
# DOWNSTREAM USAGE:
#   After this runs, downstream queries can join sec_filings_log on (symbol, accession)
#   to get fiscal_year + quarter without any additional lookups. The same columns are
#   written on every new download by the updated 05_sec_filings.py.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# ── STEP 0: Inspect current state ─────────────────────────────────────────────
# Run this first to see what you're working with before touching anything.

display(spark.sql("""
    SELECT form_type,
           COUNT(*)                 AS total_filings,
           COUNT(fiscal_year)       AS already_populated,
           COUNT(*) - COUNT(fiscal_year) AS needs_population
    FROM sec_filings_log
    GROUP BY form_type
    ORDER BY total_filings DESC
"""))

# COMMAND ----------

# ── STEP 1: Add the two columns ───────────────────────────────────────────────
# ALTER TABLE ADD COLUMNS is a metadata-only operation on Delta — instant,
# no data rewritten, no table lock that blocks readers.
# Existing rows get NULL for both new columns (which is fine; they get filled below).
# If you re-run this cell after the columns exist, it will error — that's expected.

spark.sql("ALTER TABLE sec_filings_log ADD COLUMNS (fiscal_year STRING, quarter INT)")
print("Columns added.")

# COMMAND ----------

# ── STEP 2: Populate 10-K and 10-Q via exact filingDate join ──────────────────
# bronze_income_statements.filingDate is the SEC EDGAR filing date for each
# reported quarter — a direct match to sec_filings_log.filing_date for annual
# and quarterly reports.
#
# The MERGE only touches rows where fiscal_year IS NULL, so it's safe to re-run.

result = spark.sql("""
    MERGE INTO sec_filings_log AS log
    USING (
        SELECT DISTINCT
            l.symbol,
            l.accession,
            i.fiscalYear AS fiscal_year,
            CASE i.period
                WHEN 'Q1' THEN 1
                WHEN 'Q2' THEN 2
                WHEN 'Q3' THEN 3
                WHEN 'Q4' THEN 4
                WHEN 'FY' THEN 4
                ELSE NULL
            END          AS quarter
        FROM sec_filings_log l
        JOIN bronze_income_statements i
          ON  l.symbol      = i.symbol
          AND l.filing_date = i.filingDate
        WHERE l.form_type IN ('10-K', '10-Q')
          AND l.fiscal_year IS NULL
    ) AS src
    ON  log.symbol    = src.symbol
    AND log.accession = src.accession
    WHEN MATCHED AND log.fiscal_year IS NULL THEN
        UPDATE SET
            log.fiscal_year = src.fiscal_year,
            log.quarter     = src.quarter
""")
print("10-K / 10-Q update complete.")

# COMMAND ----------

# ── STEP 2b: Fallback for 10-K/10-Q where filingDate didn't match ────────────
# Some 10-K filings are submitted before FMP updates the income statement record,
# so the exact filingDate join in Step 2 misses them (e.g. ARCC's annual 10-K is
# filed in February but FMP records Q4's filingDate in March after the 10-Q drops).
# Fix: apply the nearest-prior-quarter approach to any 10-K/10-Q still NULL.

result = spark.sql("""
    MERGE INTO sec_filings_log AS log
    USING (
        SELECT symbol, accession, fiscal_year, quarter
        FROM (
            SELECT
                l.symbol,
                l.accession,
                i.fiscalYear AS fiscal_year,
                CASE i.period
                    WHEN 'Q1' THEN 1
                    WHEN 'Q2' THEN 2
                    WHEN 'Q3' THEN 3
                    WHEN 'Q4' THEN 4
                    WHEN 'FY' THEN 4
                    ELSE NULL
                END          AS quarter,
                ROW_NUMBER() OVER (
                    PARTITION BY l.symbol, l.accession
                    ORDER BY i.date DESC
                ) AS rn
            FROM sec_filings_log l
            JOIN bronze_income_statements i
              ON  l.symbol = i.symbol
              AND CAST(l.filing_date AS DATE) >= i.date
            WHERE l.form_type IN ('10-K', '10-Q')
              AND l.fiscal_year IS NULL
        )
        WHERE rn = 1
    ) AS src
    ON  log.symbol    = src.symbol
    AND log.accession = src.accession
    WHEN MATCHED AND log.fiscal_year IS NULL THEN
        UPDATE SET
            log.fiscal_year = src.fiscal_year,
            log.quarter     = src.quarter
""")
print("10-K / 10-Q fallback (nearest prior quarter) complete.")

# COMMAND ----------

# ── STEP 3: Populate 8-K, 424B2, 424B5 via nearest prior quarter ──────────────
# Event-driven filings don't have a direct income statement counterpart.
# We assign the most recent period whose end date falls on or before the filing date.
# Example: 8-K filed 2026-04-28 → ARCC Q1 2026 (period end 2026-03-31, 28 days prior).
#
# The MERGE only touches rows where fiscal_year IS NULL.

result = spark.sql("""
    MERGE INTO sec_filings_log AS log
    USING (
        SELECT symbol, accession, fiscal_year, quarter
        FROM (
            SELECT
                l.symbol,
                l.accession,
                i.fiscalYear AS fiscal_year,
                CASE i.period
                    WHEN 'Q1' THEN 1
                    WHEN 'Q2' THEN 2
                    WHEN 'Q3' THEN 3
                    WHEN 'Q4' THEN 4
                    WHEN 'FY' THEN 4
                    ELSE NULL
                END          AS quarter,
                ROW_NUMBER() OVER (
                    PARTITION BY l.symbol, l.accession
                    ORDER BY i.date DESC
                ) AS rn
            FROM sec_filings_log l
            JOIN bronze_income_statements i
              ON  l.symbol = i.symbol
              AND CAST(l.filing_date AS DATE) >= i.date
            WHERE l.form_type NOT IN ('10-K', '10-Q')
              AND l.fiscal_year IS NULL
        )
        WHERE rn = 1
    ) AS src
    ON  log.symbol    = src.symbol
    AND log.accession = src.accession
    WHEN MATCHED AND log.fiscal_year IS NULL THEN
        UPDATE SET
            log.fiscal_year = src.fiscal_year,
            log.quarter     = src.quarter
""")
print("8-K / 424B update complete.")

# COMMAND ----------

# ── STEP 4: Validate coverage ─────────────────────────────────────────────────
# Any remaining NULLs mean the ticker has no rows in bronze_income_statements
# (e.g., tickers added to TICKER_CONFIG after the last monthly financials run).
# Those will auto-populate on the next 05_sec_filings.py run.

display(spark.sql("""
    SELECT
        form_type,
        COUNT(*)                                        AS total,
        COUNT(fiscal_year)                              AS populated,
        COUNT(*) - COUNT(fiscal_year)                   AS still_null,
        ROUND(COUNT(fiscal_year) / COUNT(*) * 100, 1)   AS pct_populated
    FROM sec_filings_log
    GROUP BY form_type
    ORDER BY total DESC
"""))

# COMMAND ----------

# ── STEP 5: Spot-check — BDC names and a large-cap ───────────────────────────

display(spark.sql("""
    SELECT symbol, form_type, filing_date, fiscal_year, quarter, filename
    FROM sec_filings_log
    WHERE symbol IN ('ARCC', 'MAIN', 'AAPL', 'GS')
    ORDER BY symbol, filing_date DESC
    LIMIT 24
"""))

# COMMAND ----------

# ── STEP 6: Confirm still_null rows (if any) ─────────────────────────────────

display(spark.sql("""
    SELECT symbol, form_type, filing_date, filename
    FROM sec_filings_log
    WHERE fiscal_year IS NULL
    ORDER BY symbol, filing_date DESC
"""))

# COMMAND ----------

# ── STEP 7: Patch AINV manually ───────────────────────────────────────────────
# AINV (Morgan Stanley Investment Management) has a March 31 fiscal year-end and
# is not present in bronze_income_statements (tracked via EDGAR XBRL only, not FMP).
# Fiscal year convention: FY ends March 31, so quarters are:
#   Q1 = Apr–Jun, Q2 = Jul–Sep, Q3 = Oct–Dec, Q4 = Jan–Mar
#
# The mapping below derives quarter from filing month using AINV's fiscal calendar.
# Filing month → fiscal quarter:
#   Jan, Feb, Mar  (during/after Q4 Jan-Mar)  → Q4 of prior FY
#   Apr, May, Jun  (after Q1 Apr-Jun end)      → Q1
#   Jul, Aug, Sep  (after Q2 Jul-Sep end)      → Q2
#   Oct, Nov, Dec  (after Q3 Oct-Dec end)      → Q3
#
# For annual 10-K (filed ~May/Jun after March year-end):
#   → fiscal_year = year of the March year-end, quarter = 4

spark.sql("""
    MERGE INTO sec_filings_log AS log
    USING (
        SELECT
            symbol,
            accession,
            CASE
                WHEN form_type = '10-K' THEN
                    CAST(YEAR(CAST(filing_date AS DATE)) AS STRING)
                WHEN MONTH(CAST(filing_date AS DATE)) IN (4, 5, 6)  THEN
                    CAST(YEAR(CAST(filing_date AS DATE)) AS STRING)
                WHEN MONTH(CAST(filing_date AS DATE)) IN (7, 8, 9)  THEN
                    CAST(YEAR(CAST(filing_date AS DATE)) AS STRING)
                WHEN MONTH(CAST(filing_date AS DATE)) IN (10,11,12) THEN
                    CAST(YEAR(CAST(filing_date AS DATE)) + 1 AS STRING)
                ELSE
                    CAST(YEAR(CAST(filing_date AS DATE)) AS STRING)
            END AS fiscal_year,
            CASE
                WHEN form_type = '10-K'                                          THEN 4
                WHEN MONTH(CAST(filing_date AS DATE)) IN (4, 5, 6)               THEN 1
                WHEN MONTH(CAST(filing_date AS DATE)) IN (7, 8, 9)               THEN 2
                WHEN MONTH(CAST(filing_date AS DATE)) IN (10, 11, 12)            THEN 3
                WHEN MONTH(CAST(filing_date AS DATE)) IN (1, 2, 3)               THEN 4
            END AS quarter
        FROM sec_filings_log
        WHERE symbol = 'AINV'
          AND fiscal_year IS NULL
    ) AS src
    ON  log.symbol    = src.symbol
    AND log.accession = src.accession
    WHEN MATCHED AND log.fiscal_year IS NULL THEN
        UPDATE SET
            log.fiscal_year = src.fiscal_year,
            log.quarter     = src.quarter
""")
print("AINV patch complete.")

# Verify AINV
display(spark.sql("""
    SELECT symbol, form_type, filing_date, fiscal_year, quarter
    FROM sec_filings_log WHERE symbol = 'AINV'
    ORDER BY filing_date DESC
"""))
