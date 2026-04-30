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
import pandas as pd

apply_full_refresh("sec_parsed")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks (
        chunk_id      STRING,
        symbol        STRING,
        form_type     STRING,
        filing_date   STRING,
        accession     STRING,
        section_name  STRING,
        chunk_index   INT,
        chunk_text    STRING,
        char_count    INT,
        parsed_at     TIMESTAMP
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

filings_df = (
    spark.sql(f"""
        SELECT symbol, form_type, accession, filing_date, subdir, filename
        FROM   {UC_CATALOG}.{UC_SCHEMA}.sec_filings_log
        WHERE  accession != 'NOACC'
    """)
    .join(already_parsed, on="accession", how="left_anti")
    # 12 accessions in sec_filings_log are shared across multiple symbols
    # (same filing downloaded for two tickers). Deduplicate so each accession
    # is parsed exactly once — prevents duplicate chunks and broken MERGE.
    .dropDuplicates(["accession"])
    .withColumn("filepath", F.concat(
        F.lit(sec_base + "/"),
        F.col("subdir"), F.lit("/"),
        F.col("symbol"), F.lit("/"),
        F.col("filename")
    ))
    # One partition per ~20 files — keeps tasks reasonably sized
    .repartition(200)
)

total = filings_df.count()
print(f"Filings to parse: {total}")

# COMMAND ----------

filings_df.display()

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
    WHEN NOT MATCHED THEN INSERT *
""")

print("sec_filing_chunks updated.")

# COMMAND ----------

# ── Write log — one row per filing ─────────────────────────────────────────────

log_df = (
    chunks_df
    .groupBy("symbol", "form_type", "accession")
    .agg(
        F.count("*").alias("chunks_written"),
        F.concat_ws(", ", F.sort_array(F.collect_set("section_name"))).alias("sections_found"),
    )
    .join(filings_df.select("accession", "filename"), on="accession", how="left")
    .withColumn("parsed_at", F.current_timestamp())
)

log_df.createOrReplaceTempView("_new_log")

spark.sql(f"""
    MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log AS log
    USING _new_log AS src
    ON  log.symbol    = src.symbol
    AND log.accession = src.accession
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("sec_parsed_log updated.")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.sec_parsed_log").orderBy("parsed_at", ascending=False))

# COMMAND ----------

# ── Flag latest filing + chunks per (symbol, form_type) ────────────────────────
# After each run, stamp is_latest=true on every chunk that belongs to the most
# recent accession for a given ticker + doc type.  All older filings are set to
# false.  On filing_date ties the lexicographically largest accession wins.

existing_cols = {f.name for f in spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks").schema}
if "is_latest" not in existing_cols:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks ADD COLUMN is_latest BOOLEAN")
    print("Added is_latest column to sec_filing_chunks.")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _latest_accessions AS
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
""")

spark.sql(f"""
    UPDATE {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
    SET is_latest = (accession IN (SELECT accession FROM _latest_accessions))
""")

print("is_latest flags updated.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        where symbol = 'AINV'
    """)
)

# COMMAND ----------


