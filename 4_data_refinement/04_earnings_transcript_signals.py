# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from earnings call transcripts — section-aware, multi-signal.
#
# Reads bronze_transcript_chunks (populated by 3_ingest_data/1_FMAPI/10_transcripts.py).
#
# Each section (prepared_remarks, qa) is processed with:
#   1. ai_analyze_sentiment  — overall section sentiment
#   2. ai_query              — returns a JSON array of 1-5 signals extracted from the section
#      The JSON array is then exploded into individual rows in gold_unified_signals.
#
# This replaces the old approach of 2×ai_classify + 1×ai_query (4 calls → 2 calls per section)
# while producing multiple signals per section instead of one.
#
# PREPARED REMARKS signal types (management-scripted):
#   Earnings Beat, Earnings Miss, Earnings In-Line,
#   Guidance Raised, Guidance Lowered, Guidance Withdrawn, Guidance Maintained,
#   Credit Event, Dividend Change, Management Change, Operational Update
#
# Q&A signal types (analyst-revealed):
#   Credit Stress Probe, Guidance Pressure, Surprise Disclosure,
#   Analyst Concern, Macro Risk, Guidance Confirmed, Results Confirmed
#
# Signal ID: md5(symbol|year|quarter|call_section|pos) — one row per signal per section.
# Idempotency: a section is considered processed when its position-0 signal exists in gold.
#
# NOTE: if re-running after the old single-signal-per-section schema, clear first:
#   DELETE FROM gold_unified_signals WHERE source_type = 'earnings_transcript'
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'earnings_transcript')
# Run after: 3_ingest_data/1_FMAPI/10_transcripts.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number (e.g. "6") to cap sections for testing

# COMMAND ----------

# # Uncomment to reset earnings transcript signals only
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE source_type = 'earnings_transcript'")

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

# COMMAND ----------

_test_limit = dbutils.widgets.get("test_limit").strip()
_limit_clause = f"LIMIT {_test_limit}" if _test_limit else ""

# A section is "done" when its position-0 signal exists. Using pos=0 as the sentinel
# avoids a full table scan while correctly gating re-runs on partial failures.
new_count = spark.sql(f"""
    SELECT COUNT(DISTINCT CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section))
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section, '|', '0'))
    WHERE c.chunk_text IS NOT NULL
""").collect()[0][0]

print(f"Transcript sections to process: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new transcript section signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find sections not yet in gold ─────────────────────────────────────
            WITH new_sections AS (
                SELECT DISTINCT c.symbol, c.year, c.quarter, c.call_section, c.call_date, c.title
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section, '|', '0'))
                WHERE c.chunk_text IS NOT NULL
                {_limit_clause}
            ),

            -- ── Aggregate chunks per section in order — 12 000 char cap ───────────
            -- Raised from 6 000 to cover ~8-9 chunks per section instead of ~4,
            -- ensuring Q&A captures multiple analyst exchanges rather than just the first few.
            section_text AS (
                SELECT
                    c.symbol, c.year, c.quarter, c.call_section, c.call_date, c.title,
                    LEFT(
                        CONCAT_WS(
                            '\\n\\n',
                            transform(
                                sort_array(collect_list(struct(c.chunk_index AS idx, c.chunk_text AS txt))),
                                x -> x.txt
                            )
                        ),
                        12000
                    ) AS section_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                JOIN new_sections ns USING (symbol, year, quarter, call_section)
                GROUP BY c.symbol, c.year, c.quarter, c.call_section, c.call_date, c.title
            ),

            -- ── Build framed context string ────────────────────────────────────────
            context AS (
                SELECT
                    symbol, year, quarter, call_section, call_date, title,
                    CONCAT(
                        'Earnings Call — ',
                        CASE call_section
                            WHEN 'prepared_remarks' THEN 'Prepared Remarks (management-scripted)'
                            WHEN 'qa'               THEN 'Q&A Session (analyst questions and management responses)'
                            ELSE                         'Full Transcript'
                        END, '\\n',
                        'Ticker: ', symbol, '\\n',
                        'Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                        CASE WHEN title IS NOT NULL THEN CONCAT('Title: ', title, '\\n') ELSE '' END,
                        '---\\n',
                        section_text
                    ) AS context_text
                FROM section_text
                WHERE section_text IS NOT NULL
                  AND LENGTH(TRIM(section_text)) > 200
            ),

            -- ── Two AI calls per section ───────────────────────────────────────────
            --   1. ai_analyze_sentiment — overall section sentiment (shared across all signals)
            --   2. ai_query            — returns JSON array of 1-5 signals
            --      Different prompt per section: prepared_remarks extracts disclosed events;
            --      qa extracts analyst intelligence (what analysts uncovered or probed).
            extracted AS (
                SELECT
                    symbol, year, quarter, call_section, call_date, title,
                    ai_analyze_sentiment(context_text) AS sentiment_raw,
                    TRIM(REGEXP_REPLACE(
                        ai_query(
                            '{LLM_ENDPOINT}',
                            CASE call_section
                            WHEN 'qa' THEN
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst reviewing the Q&A SESSION ',
                                    'of an earnings call. Q&A reveals what analysts are worried about — the ',
                                    'scripted message is already in the prepared remarks.\\n\\n',
                                    'Return a JSON array of 1–5 distinct analyst intelligence signals. ',
                                    'Each element must have exactly these four fields:\\n',
                                    '  signal_type  — one of: Credit Stress Probe, Guidance Pressure, ',
                                    'Surprise Disclosure, Analyst Concern, Macro Risk, ',
                                    'Guidance Confirmed, Results Confirmed\\n',
                                    '  signal_value — High, Medium, or Low\\n',
                                    '  advisor_action_needed — true or false\\n',
                                    '  rationale — 1-2 sentences: what analysts probed, what management ',
                                    'revealed or concealed\\n\\n',
                                    'Definitions:\\n',
                                    '  Credit Stress Probe: analysts asking about specific borrower names, ',
                                    'non-accruals, PIK interest, or covenant headroom — flag High if names ',
                                    'are mentioned or management hedged\\n',
                                    '  Guidance Pressure: management hedged, qualified, or reduced expectations ',
                                    'under analyst questioning\\n',
                                    '  Surprise Disclosure: material info absent from prepared remarks that ',
                                    'surfaced only under questioning\\n',
                                    '  Analyst Concern: concentrated skepticism on a specific risk management ',
                                    'did not proactively address\\n',
                                    '  Guidance Confirmed / Results Confirmed: analyst probed and management ',
                                    'reaffirmed — a positive signal, include it\\n\\n',
                                    'Only include signals clearly present. ',
                                    'Return ONLY the JSON array, no markdown, no surrounding text.\\n\\n',
                                    context_text
                                )
                            ELSE
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst reviewing PREPARED REMARKS ',
                                    'from an earnings call. This is the scripted, management-controlled section.\\n\\n',
                                    'Return a JSON array of 1–5 distinct investment signals found in the text. ',
                                    'Each element must have exactly these four fields:\\n',
                                    '  signal_type  — one of: Earnings Beat, Earnings Miss, Earnings In-Line, ',
                                    'Guidance Raised, Guidance Lowered, Guidance Withdrawn, Guidance Maintained, ',
                                    'Credit Event, Dividend Change, Management Change, Operational Update\\n',
                                    '  signal_value — High, Medium, or Low\\n',
                                    '  advisor_action_needed — true or false\\n',
                                    '  rationale — 1-2 sentences on the investment implication. ',
                                    'Be specific with numbers when available.\\n\\n',
                                    'Rules:\\n',
                                    '  Always use Earnings Beat, Earnings Miss, or Earnings In-Line — ',
                                    'never a generic Earnings label\\n',
                                    '  Credit Event is highest priority for BDCs: any non-accrual, PIK toggle, ',
                                    'NAV decline, or covenant breach\\n',
                                    '  Only include signals clearly present — do not pad\\n\\n',
                                    'Return ONLY the JSON array, no markdown, no surrounding text.\\n\\n',
                                    context_text
                                )
                            END
                        ),
                        '```json|```', ''
                    )) AS signals_json
                FROM context
            ),

            -- ── Explode JSON array into one row per signal ─────────────────────────
            -- posexplode preserves array position (pos) which seeds the signal_id,
            -- making each row's PK stable across re-runs on the same transcript.
            exploded AS (
                SELECT
                    e.symbol, e.year, e.quarter, e.call_section, e.call_date, e.title,
                    e.sentiment_raw,
                    pv.pos,
                    pv.sig
                FROM extracted e
                LATERAL VIEW posexplode(
                    from_json(
                        e.signals_json,
                        'ARRAY<STRUCT<signal_type:STRING, signal_value:STRING, advisor_action_needed:BOOLEAN, rationale:STRING>>'
                    )
                ) pv AS pos, sig
                WHERE pv.sig IS NOT NULL
                  AND pv.sig.signal_type IS NOT NULL
                  AND pv.sig.rationale   IS NOT NULL
                  AND pv.sig.signal_value IN ('High', 'Medium', 'Low')
            )

            SELECT
                md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|', call_section, '|', CAST(pos AS STRING))) AS signal_id,
                symbol,
                TRY_CAST(call_date AS DATE)                                       AS signal_date,
                'earnings_transcript'                                             AS source_type,
                CONCAT(
                    symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' — ',
                    CASE call_section
                        WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                        WHEN 'qa'               THEN 'Q&A Session'
                        ELSE                         'Full Transcript'
                    END
                )                                                                 AS source_description,
                INITCAP(COALESCE(sentiment_raw, 'neutral'))                       AS sentiment,
                CASE sig.signal_value
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                                               AS severity_score,
                COALESCE(sig.advisor_action_needed, false)                        AS advisor_action_needed,
                sig.signal_type                                                   AS signal_type,
                CONCAT(
                    'Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' ',
                    CASE call_section
                        WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                        WHEN 'qa'               THEN 'Q&A'
                        ELSE                         'Full Transcript'
                    END
                )                                                                 AS signal,
                sig.signal_value                                                  AS signal_value,
                sig.rationale                                                     AS rationale,
                CURRENT_TIMESTAMP()                                               AS processed_at
            FROM exploded

        ) AS src
        ON tgt.signal_id = src.signal_id
        WHEN MATCHED THEN UPDATE SET
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
    print(f"Merged signals from {new_count} transcript sections into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, signal, signal_type, signal_value,
               advisor_action_needed, sentiment, severity_score,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'earnings_transcript'
        ORDER BY symbol, signal_date DESC, signal, severity_score DESC
    """)
)

# COMMAND ----------
