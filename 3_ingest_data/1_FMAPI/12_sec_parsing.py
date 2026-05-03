# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Parse downloaded 10-K and 8-K HTML into clean text chunks for LLM extraction and RAG.
#
# Reads:  sec_filings_log  (metadata) + UC Volume HTML files (from 05_sec_filings)
# Writes: {catalog}.{schema}.sec_filing_chunks  — chunked text, one row per chunk
#         {catalog}.{schema}.sec_parsed_log      — idempotency tracker
#
# Uses mapInPandas to distribute HTML extraction across all cluster workers.
# The driver only coordinates — all file I/O and parsing runs in parallel on executors.

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.window import Window
import pandas as pd

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks (
        chunk_id      STRING,
        symbol        STRING,
        form_type     STRING,
        filing_date   STRING,
        accession     STRING,
        fiscal_year   STRING,
        quarter       INT,
        section_name  STRING,
        chunk_index   INT,
        chunk_text    STRING,
        char_count    INT,
        parsed_at     TIMESTAMP,
        is_latest     BOOLEAN
    ) USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log (
        symbol         STRING,
        form_type      STRING,
        accession      STRING,
        filename       STRING,
        sections_found STRING,
        chunks_written INT,
        parsed_at      TIMESTAMP
    ) USING DELTA
""")

# COMMAND ----------

# ── Build the work list — only unprocessed filings ─────────────────────────────

sec_base = volume_subdir("sec_filings")

already_parsed = spark.sql(f"""
    SELECT DISTINCT accession FROM {UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log
""")

# row_number is deterministic (alphabetically first symbol wins for shared accessions).
# dropDuplicates is non-deterministic — a different symbol could be selected on each
# lazy re-evaluation of this plan, causing symbol/filepath inconsistencies between
# the mapInPandas call and the later log_df construction.
_dedup_win = Window.partitionBy("accession").orderBy("symbol", "filename")

filings_df = (
    spark.sql(f"""
        SELECT symbol, form_type, accession, filing_date, fiscal_year, quarter, subdir, filename
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log
        WHERE  accession != 'NOACC'
    """)
    .join(already_parsed, on="accession", how="left_anti")
    .withColumn("_rn", F.row_number().over(_dedup_win))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
    .withColumn("filepath", F.concat(
        F.lit(sec_base + "/"),
        F.col("subdir"), F.lit("/"),
        F.col("symbol"), F.lit("/"),
        F.col("filename")
    ))
)

# Materialize once — this plan is used 5 times (count, display, diagnostic,
# mapInPandas, log_df join).  Without materializing, each evaluation re-runs the
# anti-join and dedup against live tables, which is expensive and fragile.
WORK_STAGING = f"{UC_CATALOG}.{UC_SCHEMA}._sec_filings_work"
filings_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(WORK_STAGING)
filings_df = spark.table(WORK_STAGING)

total = filings_df.count()
print(f"Filings to parse: {total}")

# COMMAND ----------

filings_df.display()

# COMMAND ----------

# ── Diagnostic: verify file access + extraction on the driver ──────────────────
# Run before the distributed mapInPandas to surface the root cause when chunk_count
# comes back 0.  Two failure modes:
#   MISSING  → volume path is wrong; check sec_base / subdir / symbol columns
#   NO TEXT  → file exists but trafilatura extracted nothing; likely empty HTML or
#              an unsupported format (SGML, XBRL index page, redirect, etc.)

import os as _os

_sample = filings_df.limit(5).collect()
_ok, _missing, _empty_parse = 0, 0, 0

for _r in _sample:
    _path = _r["filepath"]
    if not _os.path.exists(_path):
        print(f"MISSING  {_r['symbol']:6s} {_r['form_type']:8s}  {_path}")
        _missing += 1
        continue
    _size = _os.path.getsize(_path)
    try:
        import trafilatura as _tf
        with open(_path, "r", encoding="utf-8", errors="replace") as _f:
            _html = _f.read(2_000_000)
        _text = _tf.extract(_html, include_tables=True, include_links=False, favor_recall=True)
        _chars = len(_text) if _text else 0
        if _chars == 0:
            print(f"NO TEXT  {_r['symbol']:6s} {_r['form_type']:8s}  {_size:>9,} B  → 0 chars")
            _empty_parse += 1
        else:
            print(f"OK       {_r['symbol']:6s} {_r['form_type']:8s}  {_size:>9,} B  → {_chars:,} chars")
            _ok += 1
    except Exception as _e:
        print(f"ERROR    {_r['symbol']:6s} {_r['form_type']:8s}  {_e}")
        _empty_parse += 1

print(f"\nSample: {_ok} OK / {_missing} missing / {_empty_parse} failed extraction (out of {len(_sample)})")
if _missing:
    print("ACTION: file paths are wrong — check volume_subdir, and subdir/symbol in sec_filings_log")
if _empty_parse and not _missing:
    print("ACTION: files exist but extraction failed — inspect raw HTML; may need a different parser")

# COMMAND ----------

# ── Distributed parsing via mapInPandas ────────────────────────────────────────
# All constants and logic are defined inside the function so they serialise
# cleanly to every executor without needing broadcast variables.

CHUNK_SCHEMA = StructType([
    StructField("chunk_id",     StringType(),  True),
    StructField("symbol",       StringType(),  True),
    StructField("form_type",    StringType(),  True),
    StructField("filing_date",  StringType(),  True),
    StructField("accession",    StringType(),  True),
    StructField("fiscal_year",  StringType(),  True),
    StructField("quarter",      IntegerType(), True),
    StructField("section_name", StringType(),  True),
    StructField("chunk_index",  IntegerType(), True),
    StructField("chunk_text",   StringType(),  True),
    StructField("char_count",   IntegerType(), True),
])


def _parse_batch(iterator):
    """Process a partition of filing rows. Runs entirely on the executor."""
    import os
    import re
    import trafilatura
    from html.parser import HTMLParser

    CHUNK_SIZE    = 1_500
    CHUNK_OVERLAP = 150
    MIN_CHUNK     = 200

    _10K_SECTIONS = {
        "1":  "business",    "1A": "risk_factors", "2": "properties",
        "3":  "legal",       "7":  "mda",           "7A": "market_risk",
        "8":  "financial_statements",               "9A": "controls",
    }
    _ITEM_RE = re.compile(r'(?im)^(ITEM\s+(\d+[A-Z]?)\.?\s+\S[^\n]{0,70})')
    _SOI_RE  = re.compile(r'(?im)^(?:CONSOLIDATED\s+)?SCHEDULE\s+OF\s+INVESTMENTS')

    MAX_HTML_BYTES = 10 * 1024 * 1024  # 10 MB cap — 10-K bodies rarely need more

    def extract_text(filepath):
        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                html = f.read(MAX_HTML_BYTES)
            text = trafilatura.extract(html, include_tables=True, include_links=False)
            if text and len(text) >= MIN_CHUNK:
                return text
            text = trafilatura.extract(html, include_tables=True, include_links=False, favor_recall=True)
            if text and len(text) >= MIN_CHUNK:
                return text
            # Fallback: stdlib tag stripper — works on any valid HTML
            class _TE(HTMLParser):
                def __init__(self):
                    super().__init__(); self._p = []; self._skip = False
                def handle_starttag(self, tag, attrs):
                    if tag in ("script", "style"): self._skip = True
                def handle_endtag(self, tag):
                    if tag in ("script", "style"): self._skip = False
                def handle_data(self, data):
                    if not self._skip:
                        s = data.strip()
                        if s: self._p.append(s)
            p = _TE(); p.feed(html)
            text = "\n".join(p._p)
            return text if len(text) >= MIN_CHUNK else None
        except Exception:
            return None

    def split_sections(text, form_type):
        boundaries = []
        for m in _ITEM_RE.finditer(text):
            item_num = m.group(2).upper()
            label = _10K_SECTIONS.get(item_num, f"item_{item_num.lower()}")
            boundaries.append((m.start(), label))
        for m in _SOI_RE.finditer(text):
            boundaries.append((m.start(), "schedule_of_investments"))
        if not boundaries:
            return [{"name": "full_text", "text": text}]
        boundaries.sort(key=lambda x: x[0])
        # Keep first occurrence of each label — adjacent-only dedup misses
        # section names that repeat non-adjacently (e.g. TOC then actual content).
        seen_labels = {boundaries[0][1]}
        deduped = [boundaries[0]]
        for pos, label in boundaries[1:]:
            if label not in seen_labels:
                seen_labels.add(label)
                deduped.append((pos, label))
        sections = []
        for i, (pos, label) in enumerate(deduped):
            end  = deduped[i + 1][0] if i + 1 < len(deduped) else len(text)
            body = text[pos:end].strip()
            if len(body) >= MIN_CHUNK:
                sections.append({"name": label, "text": body})
        return sections or [{"name": "full_text", "text": text}]

    def chunk(section_text):
        chunks, start = [], 0
        while start < len(section_text):
            end = min(start + CHUNK_SIZE, len(section_text))
            c = section_text[start:end].strip()
            if len(c) >= MIN_CHUNK:
                chunks.append(c)
            if end == len(section_text):
                break
            start += CHUNK_SIZE - CHUNK_OVERLAP
        return chunks

    _empty = pd.DataFrame(columns=[f.name for f in CHUNK_SCHEMA.fields])

    # Yield per-file so each file's memory is released before the next one loads.
    # Accumulating all rows for a partition before yielding can hit the 1 GB limit
    # when a partition contains several large 10-K HTML files.
    for pdf in iterator:
        for _, row in pdf.iterrows():
            if not os.path.exists(row["filepath"]):
                yield _empty
                continue
            text = extract_text(row["filepath"])
            if not text:
                yield _empty
                continue
            rows = []
            for section in split_sections(text, row["form_type"]):
                for idx, chunk_text in enumerate(chunk(section["text"])):
                    rows.append({
                        "chunk_id":     f"{row['accession']}|{section['name']}|{idx}",
                        "symbol":       row["symbol"],
                        "form_type":    row["form_type"],
                        "filing_date":  row["filing_date"],
                        "accession":    row["accession"],
                        "fiscal_year":  row["fiscal_year"],
                        "quarter":      row["quarter"],
                        "section_name": section["name"],
                        "chunk_index":  idx,
                        "chunk_text":   chunk_text,
                        "char_count":   len(chunk_text),
                    })
            yield pd.DataFrame(rows) if rows else _empty

# COMMAND ----------

# ── Run parsing across the cluster ─────────────────────────────────────────────
# Serverless does not support .cache() (maps to PERSIST TABLE).
# Write once to a staging table so mapInPandas runs exactly once.

STAGING = f"{UC_CATALOG}.{UC_SCHEMA}._sec_chunks_staging"

(
    filings_df
    .repartition(200)   # scatter across workers for parallel file I/O
    .mapInPandas(_parse_batch, schema=CHUNK_SCHEMA)
    .withColumn("parsed_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGING)
)

chunks_df = spark.table(STAGING)
chunk_count = chunks_df.count()
print(f"Total chunks produced: {chunk_count:,}")

# COMMAND ----------

# ── Write chunks — single batch merge ──────────────────────────────────────────

chunks_df.createOrReplaceTempView("_new_chunks")

try:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks AS tgt
        USING _new_chunks AS src
        ON  tgt.accession    = src.accession
        AND tgt.section_name = src.section_name
        AND tgt.chunk_index  = src.chunk_index
        WHEN MATCHED THEN UPDATE SET
            tgt.chunk_text = src.chunk_text,
            tgt.char_count = src.char_count,
            tgt.parsed_at  = src.parsed_at
        WHEN NOT MATCHED THEN INSERT
            (chunk_id, symbol, form_type, filing_date, accession,
             fiscal_year, quarter,
             section_name, chunk_index, chunk_text, char_count, parsed_at, is_latest)
        VALUES
            (src.chunk_id, src.symbol, src.form_type, src.filing_date, src.accession,
             src.fiscal_year, src.quarter,
             src.section_name, src.chunk_index, src.chunk_text, src.char_count, src.parsed_at, false)
    """)
    print("sec_filing_chunks updated.")
except Exception as e:
    print(f"WARNING: sec_filing_chunks merge skipped — {e}")

# COMMAND ----------

# ── Write log — one row per filing ─────────────────────────────────────────────
# Base on filings_df (the work list), not chunks_df, so every attempted accession
# is logged even when parsing produces 0 chunks.  Without this, a file that fails
# to parse is never written to sec_parsed_log and keeps appearing as "new" on
# every subsequent run.

chunk_stats = (
    chunks_df
    .groupBy("accession")
    .agg(
        F.count("*").alias("chunks_written"),
        F.concat_ws(", ", F.sort_array(F.collect_set("section_name"))).alias("sections_found"),
    )
)

log_df = (
    filings_df.select("symbol", "form_type", "accession", "filename")
    .join(chunk_stats, on="accession", how="left")
    .withColumn("chunks_written", F.coalesce(F.col("chunks_written"), F.lit(0)))
    .withColumn("sections_found", F.coalesce(F.col("sections_found"), F.lit("")))
    .withColumn("parsed_at", F.current_timestamp())
)

failed = log_df.filter(F.col("chunks_written") == 0).count()
if failed:
    print(f"WARNING: {failed} accession(s) produced 0 chunks — logged but not in sec_filing_chunks.")

log_df.createOrReplaceTempView("_new_log")

try:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log AS log
        USING _new_log AS src
        ON  log.symbol    = src.symbol
        AND log.accession = src.accession
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("sec_parsed_log updated.")
except Exception as e:
    print(f"WARNING: sec_parsed_log merge skipped — {e}")

# COMMAND ----------

# ── Flag latest filing per (symbol, form_type) ─────────────────────────────────
# Stamp is_latest=true on every chunk belonging to the most recent accession for
# each ticker + doc type.  Runs after both MERGEs so newly inserted chunks are
# included.  On filing_date ties the lexicographically largest accession wins.
# New rows are inserted with is_latest=false; this UPDATE sets the correct value.

spark.sql(f"""
    UPDATE {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    SET is_latest = (accession IN (
        SELECT DISTINCT accession
        FROM (
            SELECT accession,
                   ROW_NUMBER() OVER (
                       PARTITION BY symbol, form_type
                       ORDER BY filing_date DESC, accession DESC
                   ) AS rn
            FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        ) ranked
        WHERE rn = 1
    ))
""")

print("is_latest flags updated.")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log").orderBy("parsed_at", ascending=False))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, form_type,
               COUNT(DISTINCT accession)               AS filings,
               SUM(CASE WHEN is_latest THEN 1 ELSE 0 END) AS latest_chunks,
               COUNT(*)                                AS total_chunks
        FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        GROUP BY symbol, form_type
        ORDER BY symbol, form_type
    """)
)

# COMMAND ----------


