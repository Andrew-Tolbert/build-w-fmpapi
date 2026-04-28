# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Data validation for sec_filing_chunks produced by 12_sec_parsing.
# Read-only — no writes to Delta or volume.
#
# Checks:
#   1. Coverage      — every downloaded filing has at least one chunk
#   2. Section splits — 10-Ks split into named sections vs. full_text fallback
#   3. Chunk integrity — char_count accuracy, MIN_CHUNK floor, index continuity
#   4. Text quality  — avg/min/max char_count by form_type + sample text
#   5. Duplicates    — (accession, section_name, chunk_index) uniqueness
#   6. Symbol/date coverage — all tickers present, date range, per-symbol counts

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

from pyspark.sql import functions as F

results = {}   # collects PASS/FAIL for the final summary cell

# COMMAND ----------

# ── CHECK 1: Coverage ───────────────────────────────────────────────────────────
# Every accession in sec_filings_log should have at least one row in sec_filing_chunks.
# A missing accession means the file was downloaded but produced zero chunks.

total_logged = spark.sql(f"""
    SELECT COUNT(DISTINCT accession) FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log
    WHERE accession != 'NOACC'
""").collect()[0][0]

total_parsed = spark.sql(f"""
    SELECT COUNT(DISTINCT accession) FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
""").collect()[0][0]

missing_df = spark.sql(f"""
    SELECT l.symbol, l.form_type, l.accession, l.filing_date
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log l
    WHERE  l.accession != 'NOACC'
    AND    NOT EXISTS (
        SELECT 1 FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
        WHERE c.accession = l.accession
    )
    ORDER BY l.symbol, l.filing_date DESC
""")

missing_count = missing_df.count()
pct_parsed = round(total_parsed / total_logged * 100, 1) if total_logged else 0

print(f"CHECK 1 — Coverage")
print(f"  Filings in sec_filings_log : {total_logged:,}")
print(f"  Filings with chunks        : {total_parsed:,}  ({pct_parsed}%)")
print(f"  Missing (no chunks)        : {missing_count:,}")

if missing_count > 0:
    print(f"  ── Missing filings (first 20) ──")
    display(missing_df.limit(20))
    results["1_coverage"] = f"FAIL — {missing_count} filings produced no chunks"
else:
    results["1_coverage"] = "PASS"

print(f"\n  → {results['1_coverage']}")

# COMMAND ----------

# ── CHECK 2: 10-K Section Splits ────────────────────────────────────────────────
# 10-Ks should split into named sections (mda, financial_statements, etc.).
# A 10-K with only section_name = 'full_text' means the section regex found nothing.

tenk_section_df = spark.sql(f"""
    SELECT
        accession,
        CASE WHEN COUNT(DISTINCT section_name) = 1
                  AND MAX(section_name) = 'full_text'
             THEN 'full_text_only'
             ELSE 'named_sections'
        END AS split_result
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    WHERE  form_type = '10-K'
    GROUP BY accession
""")

split_summary = tenk_section_df.groupBy("split_result").count().collect()
split_counts = {r["split_result"]: r["count"] for r in split_summary}

named   = split_counts.get("named_sections", 0)
full_fb = split_counts.get("full_text_only", 0)
total_10k = named + full_fb
pct_named = round(named / total_10k * 100, 1) if total_10k else 0

print(f"CHECK 2 — 10-K Section Splits")
print(f"  Total 10-K filings : {total_10k:,}")
print(f"  Named sections     : {named:,}  ({pct_named}%)")
print(f"  full_text fallback : {full_fb:,}  ({round(100 - pct_named, 1)}%)")

print(f"\n  Section distribution across all 10-Ks:")
display(spark.sql(f"""
    SELECT section_name, COUNT(*) AS chunks, COUNT(DISTINCT accession) AS filings
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    WHERE  form_type = '10-K'
    GROUP BY section_name
    ORDER BY filings DESC
"""))

# Warn if more than 30% fell back to full_text
if full_fb / total_10k > 0.30 if total_10k else False:
    results["2_section_splits"] = f"WARN — {pct_named}% of 10-Ks have named sections ({full_fb} full_text fallbacks)"
else:
    results["2_section_splits"] = f"PASS — {pct_named}% of 10-Ks have named sections"

print(f"\n  → {results['2_section_splits']}")

# COMMAND ----------

# ── CHECK 3: Chunk Integrity ─────────────────────────────────────────────────────
# 3a. char_count should equal len(chunk_text)
# 3b. No chunk should be under 200 chars (MIN_CHUNK filter in 12_sec_parsing)
# 3c. chunk_index should be contiguous per (accession, section_name)

print("CHECK 3 — Chunk Integrity")

# 3a: char_count accuracy
mismatch_count = spark.sql(f"""
    SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    WHERE char_count != LENGTH(chunk_text)
""").collect()[0][0]

print(f"\n  3a. char_count vs LENGTH(chunk_text) mismatches : {mismatch_count:,}")
if mismatch_count > 0:
    display(spark.sql(f"""
        SELECT symbol, accession, section_name, chunk_index, char_count,
               LENGTH(chunk_text) AS actual_length
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        WHERE  char_count != LENGTH(chunk_text)
        LIMIT 20
    """))

# 3b: chunks under MIN_CHUNK (200 chars)
short_count = spark.sql(f"""
    SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    WHERE char_count < 200
""").collect()[0][0]

print(f"  3b. Chunks under 200 chars (MIN_CHUNK)          : {short_count:,}")

# 3c: index continuity — max_index + 1 should equal chunk count per group
gap_count = spark.sql(f"""
    SELECT COUNT(*) FROM (
        SELECT accession, section_name,
               MAX(chunk_index) + 1   AS expected_count,
               COUNT(chunk_index)     AS actual_count
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        GROUP BY accession, section_name
        HAVING MAX(chunk_index) + 1 != COUNT(chunk_index)
    )
""").collect()[0][0]

print(f"  3c. Sections with index gaps                    : {gap_count:,}")

all_pass = mismatch_count == 0 and short_count == 0 and gap_count == 0
if all_pass:
    results["3_chunk_integrity"] = "PASS"
else:
    issues = []
    if mismatch_count: issues.append(f"{mismatch_count} char_count mismatches")
    if short_count:    issues.append(f"{short_count} short chunks")
    if gap_count:      issues.append(f"{gap_count} index gaps")
    results["3_chunk_integrity"] = f"FAIL — {', '.join(issues)}"

print(f"\n  → {results['3_chunk_integrity']}")

# COMMAND ----------

# ── CHECK 4: Text Quality ────────────────────────────────────────────────────────
# Avg/min/max chunk size by form type confirms real text was extracted.
# Sample 3 chunks per form type for a quick human sanity check.

print("CHECK 4 — Text Quality")
print("\n  Chunk size stats by form_type:")
display(spark.sql(f"""
    SELECT
        form_type,
        COUNT(*)                        AS total_chunks,
        COUNT(DISTINCT accession)       AS filings,
        ROUND(AVG(char_count))          AS avg_chars,
        MIN(char_count)                 AS min_chars,
        MAX(char_count)                 AS max_chars
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    GROUP BY form_type
    ORDER BY total_chunks DESC
"""))

print("\n  Sample chunk text (3 per form_type — first 300 chars):")
display(spark.sql(f"""
    SELECT form_type, symbol, section_name, chunk_index,
           SUBSTRING(chunk_text, 1, 300) AS chunk_preview
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY form_type ORDER BY RAND()) AS rn
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    )
    WHERE rn <= 3
    ORDER BY form_type
"""))

# Flag if any form_type has avg chunk < 300 chars (likely tag noise)
low_quality = spark.sql(f"""
    SELECT form_type, ROUND(AVG(char_count)) AS avg_chars
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    GROUP BY form_type
    HAVING AVG(char_count) < 300
""").collect()

if low_quality:
    bad = ", ".join([f"{r['form_type']} (avg {r['avg_chars']})" for r in low_quality])
    results["4_text_quality"] = f"WARN — low avg chunk size: {bad}"
else:
    results["4_text_quality"] = "PASS"

print(f"\n  → {results['4_text_quality']}")

# COMMAND ----------

# ── CHECK 5: Duplicates ──────────────────────────────────────────────────────────
# (accession, section_name, chunk_index) should be unique — enforced by the MERGE
# in 12_sec_parsing but worth verifying directly.

print("CHECK 5 — Duplicates")

dup_count = spark.sql(f"""
    SELECT COUNT(*) FROM (
        SELECT accession, section_name, chunk_index, COUNT(*) AS cnt
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        GROUP BY accession, section_name, chunk_index
        HAVING COUNT(*) > 1
    )
""").collect()[0][0]

print(f"  Duplicate (accession, section_name, chunk_index) rows : {dup_count:,}")

if dup_count > 0:
    display(spark.sql(f"""
        SELECT accession, section_name, chunk_index, COUNT(*) AS cnt
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        GROUP BY accession, section_name, chunk_index
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
        LIMIT 20
    """))
    results["5_duplicates"] = f"FAIL — {dup_count} duplicate natural keys"
else:
    results["5_duplicates"] = "PASS"

print(f"\n  → {results['5_duplicates']}")

# COMMAND ----------

# ── CHECK 6: Symbol and Date Coverage ───────────────────────────────────────────
# All tickers returned by get_tickers() should appear in sec_filing_chunks.
# Compare per-symbol filing counts vs sec_filings_log and check date range.

print("CHECK 6 — Symbol and Date Coverage")

expected_tickers = set(get_tickers())
present_tickers  = {
    r[0] for r in spark.sql(f"""
        SELECT DISTINCT symbol FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    """).collect()
}

missing_tickers = expected_tickers - present_tickers
print(f"  Expected symbols  : {len(expected_tickers)}")
print(f"  Present in chunks : {len(present_tickers)}")
print(f"  Missing symbols   : {sorted(missing_tickers) if missing_tickers else 'none'}")

date_range = spark.sql(f"""
    SELECT MIN(filing_date) AS earliest, MAX(filing_date) AS latest
    FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
""").collect()[0]
print(f"\n  Date range: {date_range['earliest']} → {date_range['latest']}")

print(f"\n  Per-symbol filing coverage (chunks vs log):")
display(spark.sql(f"""
    SELECT
        l.symbol,
        COUNT(DISTINCT l.accession)                              AS filings_in_log,
        COUNT(DISTINCT c.accession)                              AS filings_in_chunks,
        COUNT(DISTINCT l.accession) - COUNT(DISTINCT c.accession) AS gap
    FROM      {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log  l
    LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c ON l.accession = c.accession
    WHERE l.accession != 'NOACC'
    GROUP BY l.symbol
    ORDER BY gap DESC, l.symbol
"""))

if missing_tickers:
    results["6_symbol_coverage"] = f"WARN — {len(missing_tickers)} symbols missing: {sorted(missing_tickers)}"
else:
    results["6_symbol_coverage"] = "PASS"

print(f"\n  → {results['6_symbol_coverage']}")

# COMMAND ----------

# ── Summary ──────────────────────────────────────────────────────────────────────

print("=" * 60)
print("  SEC FILING CHUNKS — VALIDATION SUMMARY")
print("=" * 60)
for check, status in results.items():
    icon = "✓" if status.startswith("PASS") else ("△" if status.startswith("WARN") else "✗")
    print(f"  {icon}  {check:<25}  {status}")
print("=" * 60)
