# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Recency-scoped, batch-processing variant of 05_management_tone_signals.py.
# Identical logic and signal IDs — idempotent against the same table.
#
# Recency limits (symbols × most recent N):
#   Earnings transcripts : 3 most recent calls  (by call_date)
#   SEC 10-K             : 2 most recent filings (by filing_date)
#   SEC 10-Q             : 2 most recent filings (by filing_date)
#   SEC 8-K              : 3 most recent filings (by filing_date)
#
# Items are collected upfront then processed in configurable batches so that
# ai_query load is chunked and progress is visible throughout the run.
#
# Signal types produced:
#   Management Tone - Overall    — holistic score across all sections
#   Management Tone - <section>  — score per section (prepared_remarks, qa / mda, etc.)
#   Management Tone - Delta      — tone shift vs. prior earnings call (transcripts only)
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (signal_type LIKE 'Management Tone%')
# Run after: ingest_fmapi_10_transcripts, ingest_fmapi_13_sec_validation

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import math

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")   # cap total items per source (empty = no cap); useful for smoke tests

# ── Recency limits ─────────────────────────────────────────────────────────────
TRANSCRIPT_LIMIT = 3    # most recent N earnings calls per symbol
SEC_10K_LIMIT    = 2    # most recent N 10-K filings per symbol
SEC_10Q_LIMIT    = 2    # most recent N 10-Q filings per symbol
SEC_8K_LIMIT     = 3    # most recent N 8-K filings per symbol
BATCH_SIZE       = 10   # calls / filings per MERGE invocation

# COMMAND ----------

# # Uncomment to reset ALL management tone signals (overall + all sections + delta)
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE signal_type LIKE 'Management Tone%'")

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

# ── Collect all pending items upfront ─────────────────────────────────────────

pending_transcripts_df = spark.sql(f"""
    WITH distinct_calls AS (
        SELECT symbol, year, quarter, MAX(call_date) AS call_date
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
        WHERE chunk_text IS NOT NULL
        GROUP BY symbol, year, quarter
    ),
    ranked AS (
        SELECT symbol, year, quarter, call_date,
               ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY call_date DESC) AS rn
        FROM distinct_calls
    ),
    recent AS (
        SELECT symbol, year, quarter, call_date FROM ranked WHERE rn <= {TRANSCRIPT_LIMIT}
    )
    SELECT r.symbol, r.year, r.quarter, r.call_date
    FROM recent r
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(r.symbol, '|', CAST(r.year AS STRING), '|', CAST(r.quarter AS STRING), '|management_tone_overall'))
    ORDER BY r.symbol, r.year DESC, r.quarter DESC
""")

pending_sec_df = spark.sql(f"""
    WITH distinct_filings AS (
        SELECT DISTINCT symbol, accession, form_type, filing_date
        FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks
        WHERE is_latest = true AND form_type IN ('10-K', '10-Q', '8-K')
    ),
    ranked AS (
        SELECT symbol, accession, form_type, filing_date,
               ROW_NUMBER() OVER (PARTITION BY symbol, form_type ORDER BY filing_date DESC) AS rn
        FROM distinct_filings
    ),
    recent AS (
        SELECT symbol, accession, form_type, filing_date
        FROM ranked
        WHERE (form_type = '10-K' AND rn <= {SEC_10K_LIMIT})
           OR (form_type = '10-Q' AND rn <= {SEC_10Q_LIMIT})
           OR (form_type = '8-K'  AND rn <= {SEC_8K_LIMIT})
    )
    SELECT r.symbol, r.accession, r.form_type, r.filing_date
    FROM recent r
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(r.symbol, '|', r.accession, '|', 'management_tone_overall'))
    ORDER BY r.symbol, r.form_type, r.filing_date DESC
""")

pending_deltas_df = spark.sql(f"""
    WITH distinct_calls AS (
        SELECT symbol, year, quarter, MAX(call_date) AS call_date
        FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
        WHERE chunk_text IS NOT NULL
        GROUP BY symbol, year, quarter
    ),
    sequenced AS (
        SELECT symbol, year, quarter, call_date,
               LAG(call_date) OVER (PARTITION BY symbol ORDER BY call_date) AS prior_call_date,
               LAG(year)      OVER (PARTITION BY symbol ORDER BY call_date) AS prior_year,
               LAG(quarter)   OVER (PARTITION BY symbol ORDER BY call_date) AS prior_quarter,
               ROW_NUMBER()   OVER (PARTITION BY symbol ORDER BY call_date DESC) AS rn
        FROM distinct_calls
    )
    SELECT s.symbol, s.year, s.quarter, s.call_date,
           s.prior_year, s.prior_quarter, s.prior_call_date
    FROM sequenced s
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(s.symbol, '|', CAST(s.year AS STRING), '|', CAST(s.quarter AS STRING), '|management_tone_delta'))
    WHERE s.rn <= {TRANSCRIPT_LIMIT}
      AND s.prior_call_date IS NOT NULL
    ORDER BY s.symbol, s.year DESC, s.quarter DESC
""")

# Apply optional smoke-test cap
_test_limit = dbutils.widgets.get("test_limit").strip()
pending_transcripts = pending_transcripts_df.collect()
pending_sec         = pending_sec_df.collect()
pending_deltas      = pending_deltas_df.collect()
if _test_limit:
    n = int(_test_limit)
    pending_transcripts = pending_transcripts[:n]
    pending_sec         = pending_sec[:n]
    pending_deltas      = pending_deltas[:n]

transcript_total = len(pending_transcripts)
sec_total        = len(pending_sec)
delta_total      = len(pending_deltas)

print("Pending items to process:")
print(f"  Earnings transcripts : {transcript_total} calls   (≤{TRANSCRIPT_LIMIT} per symbol) × up to 3 signals each")
print(f"  SEC filings          : {sec_total} filings (10-K≤{SEC_10K_LIMIT}, 10-Q≤{SEC_10Q_LIMIT}, 8-K≤{SEC_8K_LIMIT}) × up to 5 signals each")
print(f"  Tone deltas          : {delta_total} call pairs  × 1 signal each")
print(f"  Batch size           : {BATCH_SIZE}")

# COMMAND ----------

# ── PART 1: Earnings transcript management tone ────────────────────────────────

if transcript_total == 0:
    print("No new earnings call management tone signals to process.")
else:
    n_batches    = math.ceil(transcript_total / BATCH_SIZE)
    total_written = 0

    print(f"{'─'*60}")
    print(f"Part 1: Earnings Transcripts  ({transcript_total} calls, {n_batches} batches)")
    print(f"{'─'*60}")

    for i in range(0, transcript_total, BATCH_SIZE):
        batch     = pending_transcripts[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        done      = min(i + len(batch), transcript_total)
        symbols   = ", ".join(sorted({r.symbol for r in batch}))
        print(f"\n  Batch {batch_num}/{n_batches}  [{done}/{transcript_total}]  {symbols}")

        spark.createDataFrame(batch, schema=pending_transcripts_df.schema)\
             .createOrReplaceTempView("_transcript_batch")

        result = spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
            USING (

                WITH calls_to_score AS (
                    SELECT * FROM _transcript_batch
                ),

                -- ── Per-section aggregation — 12 000 char cap per section ──────────
                section_texts AS (
                    SELECT
                        c.symbol, c.year, c.quarter, cts.call_date,
                        c.call_section AS section_label,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(c.chunk_index AS idx, c.chunk_text AS txt))),
                                x -> x.txt
                            )),
                            12000
                        ) AS section_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                    JOIN calls_to_score cts USING (symbol, year, quarter)
                    WHERE c.chunk_text IS NOT NULL
                    GROUP BY c.symbol, c.year, c.quarter, cts.call_date, c.call_section
                ),

                -- ── Overall: all sections combined — 24 000 char cap ────────────
                overall_texts AS (
                    SELECT
                        c.symbol, c.year, c.quarter, cts.call_date,
                        'overall' AS section_label,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(
                                    CASE c.call_section
                                        WHEN 'prepared_remarks' THEN 0
                                        WHEN 'qa'               THEN 1
                                        ELSE                         2
                                    END AS sort_key,
                                    c.chunk_index AS idx,
                                    c.chunk_text  AS txt
                                ))),
                                x -> x.txt
                            )),
                            24000
                        ) AS section_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                    JOIN calls_to_score cts USING (symbol, year, quarter)
                    WHERE c.chunk_text IS NOT NULL
                    GROUP BY c.symbol, c.year, c.quarter, cts.call_date
                ),

                -- ── Combine, filter out already-scored units ─────────────────────
                all_units AS (
                    SELECT * FROM section_texts
                    UNION ALL
                    SELECT * FROM overall_texts
                ),

                new_units AS (
                    SELECT u.*
                    FROM all_units u
                    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                        ON g.signal_id = CASE
                            WHEN u.section_label = 'overall'
                            THEN md5(CONCAT(u.symbol, '|', CAST(u.year AS STRING), '|', CAST(u.quarter AS STRING), '|management_tone_overall'))
                            ELSE md5(CONCAT(u.symbol, '|', CAST(u.year AS STRING), '|', CAST(u.quarter AS STRING), '|', u.section_label, '|management_tone'))
                        END
                    WHERE u.section_text IS NOT NULL AND LENGTH(TRIM(u.section_text)) > 100
                ),

                context AS (
                    SELECT symbol, year, quarter, call_date, section_label,
                        CONCAT(
                            'Earnings Call Transcript\\n',
                            'Ticker: ', symbol, '\\n',
                            'Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                            CASE section_label
                                WHEN 'overall'          THEN 'Scope: Full call (all sections combined)\\n'
                                WHEN 'prepared_remarks' THEN 'Scope: Prepared remarks (scripted management statements before Q&A)\\n'
                                WHEN 'qa'               THEN 'Scope: Q&A section (analyst questions and management responses)\\n'
                                ELSE CONCAT('Scope: ', section_label, '\\n')
                            END,
                            '---\\n', section_text
                        ) AS context_text
                    FROM new_units
                ),

                extracted AS (
                    SELECT symbol, year, quarter, call_date, section_label,
                        TRIM(REGEXP_REPLACE(
                            ai_query(
                                '{LLM_ENDPOINT}',
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst assessing management communication quality.\\n\\n',
                                    'Estimate the tone distribution across this earnings call content — what proportion of the ',
                                    'communication is negative, neutral, and positive. The three values must sum to 1.0 ',
                                    'and use two decimal places.\\n\\n',
                                    'Negative tone: defensive language, heavy hedging, walking back expectations, ',
                                    'evasiveness, alarming disclosures, inability to answer analyst questions directly.\\n',
                                    'Neutral tone: balanced factual reporting, standard forward-looking language, ',
                                    'routine operational updates with neither strong confidence nor concern.\\n',
                                    'Positive tone: confident and specific guidance, strong results framing, ',
                                    'transparency on upside, credible forward-looking statements, constructive ',
                                    'and direct handling of analyst challenges in Q&A.\\n\\n',
                                    'Assess across: language confidence and specificity, willingness to give forward ',
                                    'guidance, handling of analyst challenges in Q&A, frequency of hedging language, ',
                                    'transparency about problems vs. deflection.\\n\\n',
                                    'Return JSON only — no markdown, no surrounding text:\\n',
                                    '{{"negative": <0.00-1.00>, "neutral": <0.00-1.00>, "positive": <0.00-1.00>, "rationale": "<one concise sentence>"}}\\n\\n',
                                    context_text
                                )
                            ),
                            '```json|```', ''
                        )) AS tone_json
                    FROM context
                ),

                parsed AS (
                    SELECT symbol, year, quarter, call_date, section_label,
                        from_json(tone_json, 'STRUCT<negative:DOUBLE, neutral:DOUBLE, positive:DOUBLE, rationale:STRING>') AS tone
                    FROM extracted
                )

                SELECT
                    CASE
                        WHEN section_label = 'overall'
                        THEN md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|management_tone_overall'))
                        ELSE md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|', section_label, '|management_tone'))
                    END                                                                   AS signal_id,
                    symbol,
                    TRY_CAST(call_date AS DATE)                                           AS signal_date,
                    'earnings_transcript'                                                 AS source_type,
                    CONCAT(symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' — Management Tone - ',
                        CASE section_label
                            WHEN 'overall'          THEN 'Overall'
                            WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                            WHEN 'qa'               THEN 'Q&A'
                            ELSE initcap(replace(section_label, '_', ' '))
                        END)                                                              AS source_description,
                    CASE
                        WHEN tone.positive >= tone.negative AND tone.positive >= tone.neutral THEN 'Positive'
                        WHEN tone.negative >= tone.positive AND tone.negative >= tone.neutral THEN 'Negative'
                        ELSE                                                                       'Neutral'
                    END                                                                   AS sentiment,
                    CASE
                        WHEN tone.negative >= 0.4  THEN 0.9
                        WHEN tone.negative >= 0.25 THEN 0.5
                        ELSE                            0.2
                    END                                                                   AS severity_score,
                    tone.negative >= 0.4                                                  AS advisor_action_needed,
                    CASE section_label
                        WHEN 'overall' THEN 'Management Tone - Overall'
                        ELSE CONCAT('Management Tone - ', section_label)
                    END                                                                   AS signal_type,
                    CONCAT('Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' ',
                        CASE section_label
                            WHEN 'overall'          THEN 'Overall'
                            WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                            WHEN 'qa'               THEN 'Q&A'
                            ELSE initcap(replace(section_label, '_', ' '))
                        END,
                        ' Management Tone')                                               AS signal,
                    CONCAT('[', CAST(ROUND(tone.negative, 2) AS STRING), ',',
                                CAST(ROUND(tone.neutral,  2) AS STRING), ',',
                                CAST(ROUND(tone.positive, 2) AS STRING), ']')            AS signal_value,
                    tone.rationale                                                        AS rationale,
                    CURRENT_TIMESTAMP()                                                   AS processed_at
                FROM parsed
                WHERE tone.negative  IS NOT NULL
                  AND tone.neutral   IS NOT NULL
                  AND tone.positive  IS NOT NULL
                  AND tone.rationale IS NOT NULL

            ) AS src
            ON tgt.signal_id = src.signal_id
            WHEN MATCHED THEN UPDATE SET
                tgt.sentiment = src.sentiment, tgt.severity_score = src.severity_score,
                tgt.advisor_action_needed = src.advisor_action_needed, tgt.signal_type = src.signal_type,
                tgt.signal = src.signal, tgt.signal_value = src.signal_value,
                tgt.rationale = src.rationale, tgt.processed_at = src.processed_at
            WHEN NOT MATCHED THEN INSERT (
                signal_id, symbol, signal_date, source_type, source_description,
                sentiment, severity_score, advisor_action_needed, signal_type, signal, signal_value, rationale, processed_at
            ) VALUES (
                src.signal_id, src.symbol, src.signal_date, src.source_type,
                src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
                src.signal_type, src.signal, src.signal_value, src.rationale, src.processed_at
            )
        """)

        m = result.collect()[0]
        n_written      = m["num_inserted_rows"] + m["num_updated_rows"]
        total_written += n_written
        pct            = 100 * done // transcript_total
        print(f"         ✓  {n_written:>4} signals written  ({pct}% of calls complete)")

    print(f"\nPart 1 complete: {transcript_total} calls processed, {total_written} signals written.")

# COMMAND ----------

# ── PART 2: SEC 10-K, 10-Q, and 8-K management tone ──────────────────────────

if sec_total == 0:
    print("No new SEC filing management tone signals to process.")
else:
    n_batches    = math.ceil(sec_total / BATCH_SIZE)
    total_written = 0

    print(f"{'─'*60}")
    print(f"Part 2: SEC Filings  ({sec_total} filings, {n_batches} batches)")
    print(f"{'─'*60}")

    for i in range(0, sec_total, BATCH_SIZE):
        batch     = pending_sec[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        done      = min(i + len(batch), sec_total)
        symbols   = ", ".join(sorted({r.symbol for r in batch}))
        forms     = ", ".join(sorted({r.form_type for r in batch}))
        print(f"\n  Batch {batch_num}/{n_batches}  [{done}/{sec_total}]  {symbols}  ({forms})")

        spark.createDataFrame(batch, schema=pending_sec_df.schema)\
             .createOrReplaceTempView("_sec_batch")

        result = spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
            USING (

                WITH new_filings AS (
                    SELECT * FROM _sec_batch
                ),

                -- ── Per-section aggregation — 20 000 char cap per section ──────────
                section_texts AS (
                    SELECT
                        c.symbol, c.form_type, c.filing_date, c.accession,
                        c.section_name AS section_label,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(c.chunk_index AS idx, c.chunk_text AS txt))),
                                x -> x.txt
                            )),
                            20000
                        ) AS section_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                    JOIN new_filings nf USING (symbol, accession)
                    WHERE c.is_latest = true
                    GROUP BY c.symbol, c.form_type, c.filing_date, c.accession, c.section_name
                ),

                -- ── Overall: all sections combined — 60 000 char cap ────────────
                overall_texts AS (
                    SELECT
                        c.symbol, c.form_type, c.filing_date, c.accession,
                        'overall' AS section_label,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(
                                    CASE c.section_name
                                        WHEN 'mda'                  THEN 0
                                        WHEN 'risk_factors'         THEN 1
                                        WHEN 'financial_statements' THEN 2
                                        WHEN 'full_text'            THEN 3
                                        ELSE 4
                                    END AS sort_key,
                                    c.chunk_index AS chunk_idx,
                                    c.chunk_text  AS chunk_text
                                ))),
                                x -> x.chunk_text
                            )),
                            60000
                        ) AS section_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                    JOIN new_filings nf USING (symbol, accession)
                    WHERE c.is_latest = true
                    GROUP BY c.symbol, c.form_type, c.filing_date, c.accession
                ),

                -- ── Combine, filter out already-scored units ─────────────────────
                all_units AS (
                    SELECT * FROM section_texts
                    UNION ALL
                    SELECT * FROM overall_texts
                ),

                new_units AS (
                    SELECT u.*
                    FROM all_units u
                    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                        ON g.signal_id = CASE
                            WHEN u.section_label = 'overall'
                            THEN md5(CONCAT(u.symbol, '|', u.accession, '|', 'management_tone_overall'))
                            ELSE md5(CONCAT(u.symbol, '|', u.accession, '|', u.section_label, '|management_tone'))
                        END
                    WHERE u.section_text IS NOT NULL AND LENGTH(TRIM(u.section_text)) > 100
                ),

                context AS (
                    SELECT symbol, form_type, filing_date, accession, section_label,
                        CONCAT(
                            'SEC Filing\\n',
                            'Issuer: ', symbol, '\\n',
                            'Form type: ', form_type, '\\n',
                            'Filing date: ', filing_date, '\\n',
                            CASE section_label
                                WHEN 'overall'              THEN 'Scope: Full filing (all sections combined)\\n'
                                WHEN 'mda'                  THEN 'Scope: MD&A (Management Discussion & Analysis)\\n'
                                WHEN 'risk_factors'         THEN 'Scope: Risk Factors section\\n'
                                WHEN 'financial_statements' THEN 'Scope: Financial Statements section\\n'
                                WHEN 'full_text'            THEN 'Scope: Full filing text\\n'
                                ELSE CONCAT('Scope: ', section_label, '\\n')
                            END,
                            '---\\n', section_text
                        ) AS context_text
                    FROM new_units
                ),

                extracted AS (
                    SELECT symbol, form_type, filing_date, accession, section_label,
                        TRIM(REGEXP_REPLACE(
                            ai_query(
                                '{LLM_ENDPOINT}',
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst assessing management communication quality.\\n\\n',
                                    'Estimate the tone distribution in this SEC filing content — what proportion of the management ',
                                    'narrative is negative, neutral, and positive. The three values must sum to 1.0 ',
                                    'and use two decimal places.\\n\\n',
                                    'Negative tone: defensive language, heavy hedging, walking back expectations, ',
                                    'evasiveness, alarming disclosures, excessive boilerplate risk language beyond ',
                                    'standard legal disclaimers.\\n',
                                    'Neutral tone: balanced factual reporting, standard forward-looking disclaimers, ',
                                    'routine disclosures with neither strong confidence nor concern.\\n',
                                    'Positive tone: confident and specific guidance, strong results framing, credible ',
                                    'forward-looking statements, specific financial targets with timelines, ',
                                    'transparency on upside.\\n\\n',
                                    'Focus on the content provided; the scope is identified in the document header above.\\n\\n',
                                    'Assess based on: language confidence and specificity, forward-looking guidance ',
                                    'quality, transparency about risks vs. deflection, frequency of boilerplate ',
                                    'hedging language, specificity of financial targets and timelines.\\n\\n',
                                    'Return JSON only — no markdown, no surrounding text:\\n',
                                    '{{"negative": <0.00-1.00>, "neutral": <0.00-1.00>, "positive": <0.00-1.00>, "rationale": "<one concise sentence>"}}\\n\\n',
                                    context_text
                                )
                            ),
                            '```json|```', ''
                        )) AS tone_json
                    FROM context
                ),

                parsed AS (
                    SELECT symbol, form_type, filing_date, accession, section_label,
                        from_json(tone_json, 'STRUCT<negative:DOUBLE, neutral:DOUBLE, positive:DOUBLE, rationale:STRING>') AS tone
                    FROM extracted
                )

                SELECT
                    CASE
                        WHEN section_label = 'overall'
                        THEN md5(CONCAT(symbol, '|', accession, '|', 'management_tone_overall'))
                        ELSE md5(CONCAT(symbol, '|', accession, '|', section_label, '|management_tone'))
                    END                                                                   AS signal_id,
                    symbol,
                    TRY_CAST(filing_date AS DATE)                                         AS signal_date,
                    CONCAT('sec_filing_', form_type)                                      AS source_type,
                    CONCAT(form_type, ' ', filing_date, ' — Management Tone - ',
                        CASE section_label
                            WHEN 'overall'              THEN 'Overall'
                            WHEN 'mda'                  THEN 'MD&A'
                            WHEN 'risk_factors'         THEN 'Risk Factors'
                            WHEN 'financial_statements' THEN 'Financial Statements'
                            WHEN 'full_text'            THEN 'Full Text'
                            ELSE initcap(replace(section_label, '_', ' '))
                        END)                                                              AS source_description,
                    CASE
                        WHEN tone.positive >= tone.negative AND tone.positive >= tone.neutral THEN 'Positive'
                        WHEN tone.negative >= tone.positive AND tone.negative >= tone.neutral THEN 'Negative'
                        ELSE                                                                       'Neutral'
                    END                                                                   AS sentiment,
                    CASE
                        WHEN tone.negative >= 0.4  THEN 0.9
                        WHEN tone.negative >= 0.25 THEN 0.5
                        ELSE                            0.2
                    END                                                                   AS severity_score,
                    tone.negative >= 0.4                                                  AS advisor_action_needed,
                    CASE section_label
                        WHEN 'overall' THEN 'Management Tone - Overall'
                        ELSE CONCAT('Management Tone - ', section_label)
                    END                                                                   AS signal_type,
                    CONCAT(form_type, ' ',
                        CASE section_label
                            WHEN 'overall'              THEN 'Overall'
                            WHEN 'mda'                  THEN 'MD&A'
                            WHEN 'risk_factors'         THEN 'Risk Factors'
                            WHEN 'financial_statements' THEN 'Financial Statements'
                            WHEN 'full_text'            THEN 'Full Text'
                            ELSE initcap(replace(section_label, '_', ' '))
                        END,
                        ' Management Tone')                                               AS signal,
                    CONCAT('[', CAST(ROUND(tone.negative, 2) AS STRING), ',',
                                CAST(ROUND(tone.neutral,  2) AS STRING), ',',
                                CAST(ROUND(tone.positive, 2) AS STRING), ']')            AS signal_value,
                    tone.rationale                                                        AS rationale,
                    CURRENT_TIMESTAMP()                                                   AS processed_at
                FROM parsed
                WHERE tone.negative  IS NOT NULL
                  AND tone.neutral   IS NOT NULL
                  AND tone.positive  IS NOT NULL
                  AND tone.rationale IS NOT NULL

            ) AS src
            ON tgt.signal_id = src.signal_id
            WHEN MATCHED THEN UPDATE SET
                tgt.sentiment = src.sentiment, tgt.severity_score = src.severity_score,
                tgt.advisor_action_needed = src.advisor_action_needed, tgt.signal_type = src.signal_type,
                tgt.signal = src.signal, tgt.signal_value = src.signal_value,
                tgt.rationale = src.rationale, tgt.processed_at = src.processed_at
            WHEN NOT MATCHED THEN INSERT (
                signal_id, symbol, signal_date, source_type, source_description,
                sentiment, severity_score, advisor_action_needed, signal_type, signal, signal_value, rationale, processed_at
            ) VALUES (
                src.signal_id, src.symbol, src.signal_date, src.source_type,
                src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
                src.signal_type, src.signal, src.signal_value, src.rationale, src.processed_at
            )
        """)

        m = result.collect()[0]
        n_written      = m["num_inserted_rows"] + m["num_updated_rows"]
        total_written += n_written
        pct            = 100 * done // sec_total
        print(f"         ✓  {n_written:>4} signals written  ({pct}% of filings complete)")

    print(f"\nPart 2 complete: {sec_total} filings processed, {total_written} signals written.")

# COMMAND ----------

# ── PART 3: Management Tone Delta — current vs. prior earnings call ────────────

if delta_total == 0:
    print("No new management tone delta signals to process.")
else:
    n_batches    = math.ceil(delta_total / BATCH_SIZE)
    total_written = 0

    print(f"{'─'*60}")
    print(f"Part 3: Tone Deltas  ({delta_total} call pairs, {n_batches} batches)")
    print(f"{'─'*60}")

    for i in range(0, delta_total, BATCH_SIZE):
        batch     = pending_deltas[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        done      = min(i + len(batch), delta_total)
        symbols   = ", ".join(sorted({r.symbol for r in batch}))
        print(f"\n  Batch {batch_num}/{n_batches}  [{done}/{delta_total}]  {symbols}")

        spark.createDataFrame(batch, schema=pending_deltas_df.schema)\
             .createOrReplaceTempView("_delta_batch")

        result = spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
            USING (

                WITH calls_to_score AS (
                    SELECT * FROM _delta_batch
                ),

                -- ── Current call text — 12 000 char cap ───────────────────────────
                current_texts AS (
                    SELECT c.symbol, c.year, c.quarter,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(
                                    CASE c.call_section
                                        WHEN 'prepared_remarks' THEN 0
                                        WHEN 'qa'               THEN 1
                                        ELSE                         2
                                    END AS sort_key,
                                    c.chunk_index AS idx,
                                    c.chunk_text  AS txt
                                ))),
                                x -> x.txt
                            )),
                            12000
                        ) AS transcript_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                    JOIN calls_to_score cts USING (symbol, year, quarter)
                    WHERE c.chunk_text IS NOT NULL
                    GROUP BY c.symbol, c.year, c.quarter
                ),

                -- ── Prior call text — 12 000 char cap ─────────────────────────────
                prior_texts AS (
                    SELECT cts.symbol, cts.year, cts.quarter,
                        LEFT(
                            CONCAT_WS('\\n\\n', transform(
                                sort_array(collect_list(struct(
                                    CASE c.call_section
                                        WHEN 'prepared_remarks' THEN 0
                                        WHEN 'qa'               THEN 1
                                        ELSE                         2
                                    END AS sort_key,
                                    c.chunk_index AS idx,
                                    c.chunk_text  AS txt
                                ))),
                                x -> x.txt
                            )),
                            12000
                        ) AS prior_transcript_text
                    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                    JOIN calls_to_score cts
                        ON c.symbol   = cts.symbol
                        AND c.year    = cts.prior_year
                        AND c.quarter = cts.prior_quarter
                    WHERE c.chunk_text IS NOT NULL
                    GROUP BY cts.symbol, cts.year, cts.quarter
                ),

                context AS (
                    SELECT
                        cts.symbol, cts.year, cts.quarter, cts.call_date,
                        cts.prior_year, cts.prior_quarter, cts.prior_call_date,
                        CONCAT(
                            '=== PRIOR EARNINGS CALL ===\\n',
                            'Ticker: ', cts.symbol, '\\n',
                            'Period: Q', CAST(cts.prior_quarter AS STRING), ' ', CAST(cts.prior_year AS STRING),
                            ' (', CAST(cts.prior_call_date AS STRING), ')\\n',
                            '---\\n',
                            pt.prior_transcript_text, '\\n\\n',
                            '=== CURRENT EARNINGS CALL ===\\n',
                            'Ticker: ', cts.symbol, '\\n',
                            'Period: Q', CAST(cts.quarter AS STRING), ' ', CAST(cts.year AS STRING),
                            ' (', CAST(cts.call_date AS STRING), ')\\n',
                            '---\\n',
                            ct.transcript_text
                        ) AS context_text
                    FROM calls_to_score cts
                    JOIN current_texts ct USING (symbol, year, quarter)
                    JOIN prior_texts   pt USING (symbol, year, quarter)
                    WHERE ct.transcript_text       IS NOT NULL
                      AND pt.prior_transcript_text IS NOT NULL
                ),

                extracted AS (
                    SELECT symbol, year, quarter, call_date, prior_year, prior_quarter, prior_call_date,
                        TRIM(REGEXP_REPLACE(
                            ai_query(
                                '{LLM_ENDPOINT}',
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst comparing two consecutive earnings call transcripts ',
                                    'for the same company.\\n\\n',
                                    'Identify the key changes in management tone and content between the prior and current calls.\\n\\n',
                                    'Assess:\\n',
                                    '1. Direction of tone shift: has management become more positive (Improving), more negative ',
                                    '(Deteriorating), or stayed roughly the same (Stable)?\\n',
                                    '2. New topics or themes introduced in the current call that were absent or minor in the prior call.\\n',
                                    '3. Topics that disappeared or received noticeably less emphasis.\\n',
                                    '4. Notable language changes — new hedging, changed guidance specificity, shifts in confidence.\\n\\n',
                                    'Return JSON only — no markdown, no surrounding text:\\n',
                                    '{{"direction": "Improving|Deteriorating|Stable", ',
                                    '"summary": "<1-2 sentence high-level summary of the shift>", ',
                                    '"rationale": "<3-5 sentence analysis covering tone shift, new topics, dropped topics, and notable language changes>"}}\\n\\n',
                                    context_text
                                )
                            ),
                            '```json|```', ''
                        )) AS delta_json
                    FROM context
                ),

                parsed AS (
                    SELECT symbol, year, quarter, call_date, prior_year, prior_quarter, prior_call_date,
                        from_json(delta_json, 'STRUCT<direction:STRING, summary:STRING, rationale:STRING>') AS delta
                    FROM extracted
                )

                SELECT
                    md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|management_tone_delta')) AS signal_id,
                    symbol,
                    TRY_CAST(call_date AS DATE)                                           AS signal_date,
                    'earnings_transcript'                                                 AS source_type,
                    CONCAT(symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING),
                           ' vs Q', CAST(prior_quarter AS STRING), ' ', CAST(prior_year AS STRING),
                           ' — Management Tone Delta')                                    AS source_description,
                    delta.direction                                                       AS sentiment,
                    CASE delta.direction WHEN 'Deteriorating' THEN 0.9 ELSE 0.2 END      AS severity_score,
                    delta.direction = 'Deteriorating'                                     AS advisor_action_needed,
                    'Management Tone - Delta'                                             AS signal_type,
                    CONCAT('Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' Management Tone Delta') AS signal,
                    delta.summary                                                         AS signal_value,
                    delta.rationale                                                       AS rationale,
                    CURRENT_TIMESTAMP()                                                   AS processed_at
                FROM parsed
                WHERE delta.direction IS NOT NULL
                  AND delta.summary   IS NOT NULL
                  AND delta.rationale IS NOT NULL

            ) AS src
            ON tgt.signal_id = src.signal_id
            WHEN MATCHED THEN UPDATE SET
                tgt.sentiment = src.sentiment, tgt.severity_score = src.severity_score,
                tgt.advisor_action_needed = src.advisor_action_needed, tgt.signal_type = src.signal_type,
                tgt.signal = src.signal, tgt.signal_value = src.signal_value,
                tgt.rationale = src.rationale, tgt.processed_at = src.processed_at
            WHEN NOT MATCHED THEN INSERT (
                signal_id, symbol, signal_date, source_type, source_description,
                sentiment, severity_score, advisor_action_needed, signal_type, signal, signal_value, rationale, processed_at
            ) VALUES (
                src.signal_id, src.symbol, src.signal_date, src.source_type,
                src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
                src.signal_type, src.signal, src.signal_value, src.rationale, src.processed_at
            )
        """)

        m = result.collect()[0]
        n_written      = m["num_inserted_rows"] + m["num_updated_rows"]
        total_written += n_written
        pct            = 100 * done // delta_total
        print(f"         ✓  {n_written:>4} signals written  ({pct}% of pairs complete)")

    print(f"\nPart 3 complete: {delta_total} call pairs processed, {total_written} signals written.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_type, signal_type, source_description,
               signal_value,
               sentiment, advisor_action_needed, severity_score,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE signal_type LIKE 'Management Tone%'
        ORDER BY symbol, signal_date DESC, signal_type
    """)
)

# COMMAND ----------
