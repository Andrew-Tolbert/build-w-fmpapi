# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Migration: add doc_uri to sec_filing_chunks and bronze_transcript_chunks.
#
# doc_uri is a human-readable identifier used by Databricks Vector Search to
# surface source provenance in RAG responses.
#
#   sec_filing_chunks      →  "2024 Q3 | 10-Q | Risk Factors"
#   bronze_transcript_chunks →  "ARCC Q1 2024 Earnings Call | Prepared Remarks"
#
# SAFETY:
#   • ALTER TABLE ADD COLUMN is metadata-only on Delta — no data rewrite, no reader lock.
#   • The column is added with try/except so the cell is safe to re-run if the column
#     already exists.
#   • UPDATE only touches rows where doc_uri IS NULL — idempotent on re-run.
#   • No files are re-parsed, no vectors are invalidated unless you trigger a re-index.
#
# AFTER THIS RUNS:
#   Re-sync the Vector Search indexes so newly populated doc_uri values are queryable:
#     - sec_filings_index (over sec_filing_chunks)
#     - transcripts_index (over bronze_transcript_chunks)
#   Either trigger a full re-sync from the Databricks UI, or let the next scheduled
#   pipeline run pick up the change-data-feed rows.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

spark.sql(f"USE CATALOG {UC_CATALOG}")
spark.sql(f"USE SCHEMA {UC_SCHEMA}")
print(f"Using: {UC_CATALOG}.{UC_SCHEMA}")

# COMMAND ----------

# ── STEP 0: Inspect current state ─────────────────────────────────────────────

display(spark.sql("""
    SELECT
        COUNT(*)                    AS total_chunks,
        COUNT(doc_uri)              AS already_populated,
        COUNT(*) - COUNT(doc_uri)   AS needs_population
    FROM sec_filing_chunks
""")) if "doc_uri" in [f.name for f in spark.table("sec_filing_chunks").schema.fields] else print("sec_filing_chunks: doc_uri column does not yet exist")

display(spark.sql("""
    SELECT
        COUNT(*)                    AS total_chunks,
        COUNT(doc_uri)              AS already_populated,
        COUNT(*) - COUNT(doc_uri)   AS needs_population
    FROM bronze_transcript_chunks
""")) if "doc_uri" in [f.name for f in spark.table("bronze_transcript_chunks").schema.fields] else print("bronze_transcript_chunks: doc_uri column does not yet exist")

# COMMAND ----------

# ── STEP 1: Add doc_uri column to sec_filing_chunks ───────────────────────────

try:
    spark.sql("ALTER TABLE sec_filing_chunks ADD COLUMN doc_uri STRING")
    print("sec_filing_chunks: doc_uri column added.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("sec_filing_chunks: doc_uri already exists — skipping ALTER.")
    else:
        raise

# COMMAND ----------

# ── STEP 2: Populate doc_uri for existing sec_filing_chunks rows ──────────────
#
# Format: "{fiscal_year} Q{quarter} | {FORM_TYPE} | {Section Name}"
# Example: "2024 Q3 | 10-Q | Risk Factors"
#
# section_name underscores → spaces, title-cased.
# Rows where fiscal_year IS NULL (no income statement match) fall back to
# "Unknown | {form_type} | {section_name}" so no row is left with a NULL uri.

result = spark.sql("""
    UPDATE sec_filing_chunks
    SET doc_uri = CASE
        WHEN fiscal_year IS NOT NULL AND quarter IS NOT NULL THEN
            CONCAT(
                fiscal_year, ' Q', CAST(quarter AS STRING),
                ' | ', form_type,
                ' | ', INITCAP(REPLACE(section_name, '_', ' '))
            )
        WHEN fiscal_year IS NOT NULL THEN
            CONCAT(
                fiscal_year,
                ' | ', form_type,
                ' | ', INITCAP(REPLACE(section_name, '_', ' '))
            )
        ELSE
            CONCAT(
                'Unknown',
                ' | ', form_type,
                ' | ', INITCAP(REPLACE(section_name, '_', ' '))
            )
    END
    WHERE doc_uri IS NULL
""")
print("sec_filing_chunks: doc_uri populated.")

# COMMAND ----------

# ── STEP 3: Validate sec_filing_chunks ────────────────────────────────────────

display(spark.sql("""
    SELECT
        form_type,
        COUNT(*)                                        AS total_chunks,
        COUNT(doc_uri)                                  AS populated,
        COUNT(*) - COUNT(doc_uri)                       AS still_null,
        ROUND(COUNT(doc_uri) / COUNT(*) * 100, 1)       AS pct_populated
    FROM sec_filing_chunks
    GROUP BY form_type
    ORDER BY total_chunks DESC
"""))

display(spark.sql("""
    SELECT symbol, form_type, fiscal_year, quarter, section_name, doc_uri
    FROM sec_filing_chunks
    WHERE symbol IN ('ARCC', 'AAPL', 'GS')
      AND is_latest = true
    ORDER BY symbol, form_type, section_name
    LIMIT 20
"""))

# COMMAND ----------

# ── STEP 4: Add doc_uri column to bronze_transcript_chunks ────────────────────

try:
    spark.sql("ALTER TABLE bronze_transcript_chunks ADD COLUMN doc_uri STRING")
    print("bronze_transcript_chunks: doc_uri column added.")
except Exception as e:
    if "already exists" in str(e).lower():
        print("bronze_transcript_chunks: doc_uri already exists — skipping ALTER.")
    else:
        raise

# COMMAND ----------

# ── STEP 5: Populate doc_uri for existing bronze_transcript_chunks rows ────────
#
# Format: "{title} | {Call Section}"
# Example: "ARCC Q1 2024 Earnings Call | Prepared Remarks"
#
# call_section underscores → spaces, title-cased.
# Falls back to "{symbol} Q{quarter} {year} Earnings Call" when title is NULL.

result = spark.sql("""
    UPDATE bronze_transcript_chunks
    SET doc_uri = CONCAT(
        COALESCE(
            title,
            CONCAT(symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' Earnings Call')
        ),
        ' | ',
        INITCAP(REPLACE(call_section, '_', ' '))
    )
    WHERE doc_uri IS NULL
""")
print("bronze_transcript_chunks: doc_uri populated.")

# COMMAND ----------

# ── STEP 6: Validate bronze_transcript_chunks ─────────────────────────────────

display(spark.sql("""
    SELECT
        call_section,
        COUNT(*)                                        AS total_chunks,
        COUNT(doc_uri)                                  AS populated,
        COUNT(*) - COUNT(doc_uri)                       AS still_null,
        ROUND(COUNT(doc_uri) / COUNT(*) * 100, 1)       AS pct_populated
    FROM bronze_transcript_chunks
    GROUP BY call_section
    ORDER BY total_chunks DESC
"""))

display(spark.sql("""
    SELECT symbol, year, quarter, call_section, doc_uri, chunk_index
    FROM bronze_transcript_chunks
    WHERE symbol IN ('ARCC', 'AAPL', 'MAIN')
    ORDER BY symbol, year DESC, quarter DESC, call_section, chunk_index
    LIMIT 20
"""))

# COMMAND ----------

# ── STEP 7: Spot-check full doc_uri samples ───────────────────────────────────

print("=== SEC sample ===")
display(spark.sql("""
    SELECT DISTINCT doc_uri
    FROM sec_filing_chunks
    WHERE symbol = 'ARCC'
    ORDER BY doc_uri
    LIMIT 20
"""))

print("=== Transcript sample ===")
display(spark.sql("""
    SELECT DISTINCT doc_uri
    FROM bronze_transcript_chunks
    WHERE symbol = 'ARCC'
    ORDER BY doc_uri
    LIMIT 10
"""))
