# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest earnings call transcripts into bronze_transcripts (raw) then chunk
# into bronze_transcript_chunks for Databricks Vector Search.
#
# Stage 1 — Autoloader ingests JSON files from the volume into bronze_transcripts
#            (one row per transcript, unchanged raw store).
# Stage 2 — mapInPandas chunks each transcript by call section (prepared remarks /
#            Q&A) with fixed-window fallback, writing to bronze_transcript_chunks.
#            A transcript_parse_log table provides idempotency so re-runs only
#            process new transcripts.
#
# bronze_transcript_chunks schema is optimised for Databricks Vector Search:
#   chunk_id     — deterministic PK: {symbol}|Q{quarter}|{year}|{section}|{idx}
#   symbol/year/quarter/call_date/title  — hybrid-search filter columns
#   call_section — 'prepared_remarks' | 'qa' | 'full_text'
#   chunk_text   — text to embed
#
# Source: UC_VOLUME_PATH/transcripts/{TICKER}/Q{q}_{year}.json

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType
)
import pandas as pd

# COMMAND ----------

# ── Stage 1 table: raw transcripts ────────────────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts (
        symbol       STRING  NOT NULL,
        year         INT     NOT NULL,
        quarter      INT     NOT NULL,
        date         STRING,
        title        STRING,
        company_name STRING,
        content      STRING
    )
    USING DELTA
""")

# Drop legacy id column if it exists (old schema had id NOT NULL which conflicts
# with Autoloader since the JSON files don't contain an id field).
try:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts DROP COLUMN id")
    print("Dropped legacy id column from bronze_transcripts.")
except Exception:
    pass  # column doesn't exist — nothing to do

try:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts ADD COLUMN company_name STRING")
    print("Added company_name column to bronze_transcripts.")
except Exception:
    pass  # column already exists

# ── Stage 2 table: chunks for vector search ───────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks (
        chunk_id      STRING  NOT NULL,
        symbol        STRING  NOT NULL,
        year          INT     NOT NULL,
        quarter       INT     NOT NULL,
        call_date     STRING,
        title         STRING,
        company_name  STRING,
        call_section  STRING,
        chunk_index   INT,
        total_chunks  INT,
        chunk_text    STRING,
        char_count    INT,
        ingested_at   TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

try:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks ADD COLUMN company_name STRING")
    print("Added company_name column to bronze_transcript_chunks.")
except Exception:
    pass  # column already exists

# ── Idempotency log for chunking ──────────────────────────────────────────────

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.transcript_parse_log (
        symbol        STRING,
        year          INT,
        quarter       INT,
        sections_found STRING,
        chunks_written INT,
        parsed_at     TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# ── Stage 1: Autoloader — JSON files → bronze_transcripts ────────────────────

checkpoint_path = f"{UC_VOLUME_PATH}/_checkpoints/bronze_transcripts"
schema_path     = f"{UC_VOLUME_PATH}/_schemas/bronze_transcripts"
source_path     = f"{UC_VOLUME_PATH}/transcripts/*/*.json"

(
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("multiLine", "true")
        .load(source_path)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts")
).awaitTermination()

print(f"bronze_transcripts row count: {spark.table(f'{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts').count()}")

# COMMAND ----------

# ── Patch: backfill company_name and null titles ──────────────────────────────
# Runs every time but only touches rows that need fixing.
# company_name is not in the source JSON so Autoloader leaves it null — fill it
# from bronze_company_profiles (which has the canonical company name per ticker).
# title is null for transcripts fetched before the pull notebook was fixed — derive
# it as "{company_name} Q{quarter} {year} Earnings Call".

spark.sql(f"""
    MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts AS tgt
    USING (
        SELECT t.symbol, t.year, t.quarter, cp.companyName AS company_name
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts t
        JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
            ON t.symbol = cp.symbol
        WHERE t.company_name IS NULL
    ) AS src
    ON  tgt.symbol  = src.symbol
    AND tgt.year    = src.year
    AND tgt.quarter = src.quarter
    WHEN MATCHED THEN UPDATE SET tgt.company_name = src.company_name
""")

spark.sql(f"""
    UPDATE {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts
    SET title = CONCAT(
        COALESCE(company_name, symbol),
        ' Q', CAST(quarter AS STRING),
        ' ', CAST(year AS STRING),
        ' Earnings Call'
    )
    WHERE title IS NULL OR TRIM(title) = ''
""")

_null_after = spark.sql(f"""
    SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts
    WHERE title IS NULL
""").collect()[0][0]
print(f"Null titles remaining after patch: {_null_after}")

# COMMAND ----------

# ── Stage 2 work list — transcripts not yet chunked ───────────────────────────

already_parsed = spark.sql(f"""
    SELECT DISTINCT symbol, year, quarter
    FROM {UC_CATALOG}.{UC_SCHEMA}.transcript_parse_log
""")

transcripts_df = (
    spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts")
    .select("symbol", "year", "quarter", "date", "title", "company_name", "content")
    .join(already_parsed, on=["symbol", "year", "quarter"], how="left_anti")
    .filter(F.col("content").isNotNull() & (F.length(F.col("content")) > 0))
)

total = transcripts_df.count()
print(f"Transcripts to chunk: {total}")

# COMMAND ----------

transcripts_df.display()

# COMMAND ----------

# ── Chunking via mapInPandas ──────────────────────────────────────────────────

CHUNK_SCHEMA = StructType([
    StructField("chunk_id",     StringType(),  True),
    StructField("symbol",       StringType(),  True),
    StructField("year",         IntegerType(), True),
    StructField("quarter",      IntegerType(), True),
    StructField("call_date",    StringType(),  True),
    StructField("title",        StringType(),  True),
    StructField("company_name", StringType(),  True),
    StructField("call_section", StringType(),  True),
    StructField("chunk_index",  IntegerType(), True),
    StructField("total_chunks", IntegerType(), True),
    StructField("chunk_text",   StringType(),  True),
    StructField("char_count",   IntegerType(), True),
])


def _chunk_transcripts(iterator):
    """Chunk each transcript row by call section with fixed-window fallback."""
    import re

    CHUNK_SIZE    = 1_500
    CHUNK_OVERLAP = 150
    MIN_CHUNK     = 200

    # Patterns that signal the start of the Q&A portion of a call
    _QA_RE = re.compile(
        r'(?im)^\s*(?:question[- ]and[- ]answer|q\s*&\s*a|q\s*and\s*a'
        r'|questions?\s+and\s+answers?'
        r'|operator\s+instructions?\s+for\s+q(?:uestion)?'
        r'|we\s+will\s+now\s+begin\s+the\s+question)',
        re.IGNORECASE,
    )

    def split_sections(text):
        m = _QA_RE.search(text)
        if m:
            prepared = text[:m.start()].strip()
            qa       = text[m.start():].strip()
            sections = []
            if len(prepared) >= MIN_CHUNK:
                sections.append(("prepared_remarks", prepared))
            if len(qa) >= MIN_CHUNK:
                sections.append(("qa", qa))
            if sections:
                return sections
        return [("full_text", text)]

    def fixed_chunk(text):
        chunks, start = [], 0
        while start < len(text):
            end = min(start + CHUNK_SIZE, len(text))
            c = text[start:end].strip()
            if len(c) >= MIN_CHUNK:
                chunks.append(c)
            if end == len(text):
                break
            start += CHUNK_SIZE - CHUNK_OVERLAP
        return chunks

    _empty = pd.DataFrame(columns=[f.name for f in CHUNK_SCHEMA.fields])

    for pdf in iterator:
        for _, row in pdf.iterrows():
            content = row.get("content") or ""
            if len(content) < MIN_CHUNK:
                yield _empty
                continue

            sections  = split_sections(content)
            all_rows  = []
            global_idx = 0
            for section_name, section_text in sections:
                for chunk_text in fixed_chunk(section_text):
                    all_rows.append({
                        "chunk_id":     f"{row['symbol']}|Q{row['quarter']}|{row['year']}|{section_name}|{global_idx}",
                        "symbol":       row["symbol"],
                        "year":         int(row["year"]),
                        "quarter":      int(row["quarter"]),
                        "call_date":    row.get("date"),
                        "title":        row.get("title"),
                        "company_name": row.get("company_name"),
                        "call_section": section_name,
                        "chunk_index":  global_idx,
                        "total_chunks": 0,   # backfilled below
                        "chunk_text":   chunk_text,
                        "char_count":   len(chunk_text),
                    })
                    global_idx += 1

            # Backfill total_chunks now that we know the count
            for r in all_rows:
                r["total_chunks"] = len(all_rows)

            yield pd.DataFrame(all_rows) if all_rows else _empty


# COMMAND ----------

# ── Run chunking, write to staging ────────────────────────────────────────────

STAGING = f"{UC_CATALOG}.{UC_SCHEMA}._transcript_chunks_staging"

(
    transcripts_df
    .repartition(50)
    .mapInPandas(_chunk_transcripts, schema=CHUNK_SCHEMA)
    .withColumn("ingested_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(STAGING)
)

chunks_df   = spark.table(STAGING)
chunk_count = chunks_df.count()
print(f"Total chunks produced: {chunk_count:,}")

# COMMAND ----------

# ── Merge into bronze_transcript_chunks ───────────────────────────────────────

chunks_df.createOrReplaceTempView("_new_transcript_chunks")

try:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks AS tgt
        USING _new_transcript_chunks AS src
        ON tgt.chunk_id = src.chunk_id
        WHEN MATCHED THEN UPDATE SET
            tgt.title        = src.title,
            tgt.company_name = src.company_name,
            tgt.chunk_text   = src.chunk_text,
            tgt.char_count   = src.char_count,
            tgt.total_chunks = src.total_chunks,
            tgt.ingested_at  = src.ingested_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("bronze_transcript_chunks updated.")
except Exception as e:
    print(f"WARNING: bronze_transcript_chunks merge skipped — {e}")

# COMMAND ----------

# ── Patch: backfill title and company_name in bronze_transcript_chunks ─────────
# Existing chunks written before these columns existed will have nulls.
# Join back to bronze_transcripts (already patched above) to fill them in.

spark.sql(f"""
    MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks AS tgt
    USING (
        SELECT DISTINCT symbol, year, quarter, title, company_name
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts
        WHERE title IS NOT NULL OR company_name IS NOT NULL
    ) AS src
    ON  tgt.symbol  = src.symbol
    AND tgt.year    = src.year
    AND tgt.quarter = src.quarter
    AND (tgt.title IS NULL OR tgt.company_name IS NULL)
    WHEN MATCHED THEN UPDATE SET
        tgt.title        = src.title,
        tgt.company_name = src.company_name
""")

_chunk_nulls = spark.sql(f"""
    SELECT COUNT(*) FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
    WHERE title IS NULL OR company_name IS NULL
""").collect()[0][0]
print(f"Chunks still missing title or company_name: {_chunk_nulls}")

# COMMAND ----------

# ── Write parse log ───────────────────────────────────────────────────────────

chunk_stats = (
    chunks_df
    .groupBy("symbol", "year", "quarter")
    .agg(
        F.count("*").alias("chunks_written"),
        F.concat_ws(", ", F.sort_array(F.collect_set("call_section"))).alias("sections_found"),
    )
)

# Log every transcript that was in the work list, even if it produced 0 chunks
log_df = (
    transcripts_df.select("symbol", "year", "quarter")
    .join(chunk_stats, on=["symbol", "year", "quarter"], how="left")
    .withColumn("chunks_written", F.coalesce(F.col("chunks_written"), F.lit(0)))
    .withColumn("sections_found", F.coalesce(F.col("sections_found"), F.lit("")))
    .withColumn("parsed_at", F.current_timestamp())
)

failed = log_df.filter(F.col("chunks_written") == 0).count()
if failed:
    print(f"WARNING: {failed} transcript(s) produced 0 chunks — logged.")

log_df.createOrReplaceTempView("_new_transcript_log")

try:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.transcript_parse_log AS log
        USING _new_transcript_log AS src
        ON  log.symbol  = src.symbol
        AND log.year    = src.year
        AND log.quarter = src.quarter
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("transcript_parse_log updated.")
except Exception as e:
    print(f"WARNING: transcript_parse_log merge skipped — {e}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.transcript_parse_log").orderBy("parsed_at", ascending=False))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
            symbol,
            COUNT(DISTINCT CONCAT(year, '_', quarter)) AS transcripts,
            SUM(CASE WHEN call_section = 'prepared_remarks' THEN 1 ELSE 0 END) AS prepared_chunks,
            SUM(CASE WHEN call_section = 'qa'               THEN 1 ELSE 0 END) AS qa_chunks,
            SUM(CASE WHEN call_section = 'full_text'        THEN 1 ELSE 0 END) AS unsplit_chunks,
            COUNT(*) AS total_chunks
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
        GROUP BY symbol
        ORDER BY symbol
    """)
)
