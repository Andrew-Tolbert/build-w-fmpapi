# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Generate Management Tone signals for earnings transcripts and SEC 10-K/8-K filings.
#
# Management Tone is a sentiment probability distribution stored as a JSON array string:
#   signal_value = "[negative, neutral, positive]"  e.g. "[0.1,0.5,0.4]"
# The three values sum to 1.0. The frontend maps them to display labels/charts.
#
# Sources:
#   earnings_transcript  — full call (all sections combined), one score per (symbol, year, quarter)
#   sec_filing_10-K      — annual report, MD&A-prioritized text, one score per filing
#   sec_filing_8-K       — current report narrative, one score per filing
#
# Signal ID construction (guaranteed non-colliding with existing position-based IDs):
#   transcripts: md5(symbol|year|quarter|management_tone)
#   SEC filings:  md5(symbol|accession|management_tone)
#
# Derived fields from the distribution:
#   sentiment             — whichever of negative/neutral/positive has the highest weight
#   severity_score        — driven by the negative weight (>=0.5 → 0.9, >=0.25 → 0.5, else 0.2)
#   advisor_action_needed — true when negative weight >= 0.5
#
# Idempotency: a call/filing is considered processed when its management_tone signal_id exists.
# Backfill: run once — reads from existing bronze tables, no FMP re-pull or HTML re-parse needed.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (signal_type = 'Management Tone')
# Run after: ingest_fmapi_10_transcripts, ingest_fmapi_13_sec_validation

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number to cap per source for testing

# COMMAND ----------

# # Uncomment to reset management tone signals only
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE signal_type = 'Management Tone'")

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

transcript_count = spark.sql(f"""
    SELECT COUNT(DISTINCT CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING)))
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', 'management_tone'))
    WHERE c.chunk_text IS NOT NULL
""").collect()[0][0]

sec_count = spark.sql(f"""
    SELECT COUNT(DISTINCT c.accession)
    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', 'management_tone'))
    WHERE c.is_latest = true
      AND c.form_type IN ('10-K', '8-K')
""").collect()[0][0]

print(f"Earnings calls to score: {transcript_count}")
print(f"SEC filings (10-K / 8-K) to score: {sec_count}")

# COMMAND ----------

# ── PART 1: Earnings transcript management tone ────────────────────────────────

if transcript_count == 0:
    print("No new earnings call management tone signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find calls not yet scored ──────────────────────────────────────
            WITH new_calls AS (
                SELECT c.symbol, c.year, c.quarter, MAX(c.call_date) AS call_date
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', 'management_tone'))
                WHERE c.chunk_text IS NOT NULL
                GROUP BY c.symbol, c.year, c.quarter
                {_limit_clause}
            ),

            -- ── Combine all sections of the full call — 24 000 char cap ───────
            -- Prepared remarks (scripted) sorted before Q&A (analyst-driven) to
            -- preserve natural call order. Tone is assessed holistically across both.
            full_text AS (
                SELECT
                    c.symbol, c.year, c.quarter, nc.call_date,
                    LEFT(
                        CONCAT_WS(
                            '\\n\\n',
                            transform(
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
                            )
                        ),
                        24000
                    ) AS full_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                JOIN new_calls nc USING (symbol, year, quarter)
                GROUP BY c.symbol, c.year, c.quarter, nc.call_date
            ),

            -- ── Build framed context ───────────────────────────────────────────
            context AS (
                SELECT
                    symbol, year, quarter, call_date,
                    CONCAT(
                        'Earnings Call Transcript\\n',
                        'Ticker: ', symbol, '\\n',
                        'Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                        '---\\n',
                        full_text
                    ) AS context_text
                FROM full_text
                WHERE full_text IS NOT NULL
                  AND LENGTH(TRIM(full_text)) > 200
            ),

            -- ── One ai_query per call — returns a single JSON object ───────────
            -- Returns a probability distribution: negative + neutral + positive = 1.0
            extracted AS (
                SELECT
                    symbol, year, quarter, call_date,
                    TRIM(REGEXP_REPLACE(
                        ai_query(
                            '{LLM_ENDPOINT}',
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst assessing management communication quality.\\n\\n',
                                'Estimate the tone distribution across this earnings call — what proportion of the ',
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

            -- ── Parse JSON response ────────────────────────────────────────────
            parsed AS (
                SELECT
                    symbol, year, quarter, call_date,
                    from_json(tone_json, 'STRUCT<negative:DOUBLE, neutral:DOUBLE, positive:DOUBLE, rationale:STRING>') AS tone
                FROM extracted
            )

            SELECT
                md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|', 'management_tone')) AS signal_id,
                symbol,
                TRY_CAST(call_date AS DATE)                                       AS signal_date,
                'earnings_transcript'                                             AS source_type,
                CONCAT(symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' — Management Tone') AS source_description,
                CASE
                    WHEN tone.positive >= tone.negative AND tone.positive >= tone.neutral THEN 'Positive'
                    WHEN tone.negative >= tone.positive AND tone.negative >= tone.neutral THEN 'Negative'
                    ELSE                                                                       'Neutral'
                END                                                               AS sentiment,
                CASE
                    WHEN tone.negative >= 0.4  THEN 0.9
                    WHEN tone.negative >= 0.25 THEN 0.5
                    ELSE                            0.2
                END                                                               AS severity_score,
                tone.negative >= 0.4                                              AS advisor_action_needed,
                'Management Tone'                                                 AS signal_type,
                CONCAT('Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' Management Tone') AS signal,
                CONCAT('[', CAST(ROUND(tone.negative, 2) AS STRING), ',',
                            CAST(ROUND(tone.neutral,  2) AS STRING), ',',
                            CAST(ROUND(tone.positive, 2) AS STRING), ']')         AS signal_value,
                tone.rationale                                                    AS rationale,
                CURRENT_TIMESTAMP()                                               AS processed_at
            FROM parsed
            WHERE tone.negative  IS NOT NULL
              AND tone.neutral   IS NOT NULL
              AND tone.positive  IS NOT NULL
              AND tone.rationale IS NOT NULL

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
    print(f"Merged management tone signals from {transcript_count} earnings calls into gold_unified_signals.")

# COMMAND ----------

# ── PART 2: SEC 10-K and 8-K management tone ──────────────────────────────────

if sec_count == 0:
    print("No new SEC filing management tone signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find filings not yet scored ────────────────────────────────────
            WITH new_filings AS (
                SELECT DISTINCT c.symbol, c.accession, c.form_type, c.filing_date
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', 'management_tone'))
                WHERE c.is_latest = true
                  AND c.form_type IN ('10-K', '8-K')
                {_limit_clause}
            ),

            -- ── Aggregate chunks with section priority — 60 000 char cap ───────
            -- MD&A carries the most management narrative for tone assessment.
            -- 60 000 chars covers the full MD&A without loading financial tables.
            aggregated AS (
                SELECT
                    c.symbol,
                    c.form_type,
                    c.filing_date,
                    c.accession,
                    LEFT(
                        CONCAT_WS(
                            '\\n\\n',
                            transform(
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
                            )
                        ),
                        60000
                    ) AS document_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                JOIN new_filings nf USING (symbol, accession)
                WHERE c.is_latest = true
                GROUP BY c.symbol, c.form_type, c.filing_date, c.accession
            ),

            -- ── Build framed context ───────────────────────────────────────────
            context AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    CONCAT(
                        'SEC Filing\\n',
                        'Issuer: ', symbol, '\\n',
                        'Form type: ', form_type, '\\n',
                        'Filing date: ', filing_date, '\\n',
                        '---\\n',
                        document_text
                    ) AS context_text
                FROM aggregated
                WHERE document_text IS NOT NULL
                  AND LENGTH(TRIM(document_text)) > 100
            ),

            -- ── One ai_query per filing — returns a single JSON object ─────────
            -- Returns a probability distribution: negative + neutral + positive = 1.0
            extracted AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    TRIM(REGEXP_REPLACE(
                        ai_query(
                            '{LLM_ENDPOINT}',
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst assessing management communication quality.\\n\\n',
                                'Estimate the tone distribution in this SEC filing — what proportion of the management ',
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
                                'For 10-K filings, focus on the MD&A section narrative. For 8-K filings, assess ',
                                'the tone of the disclosed event and any management commentary.\\n\\n',
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

            -- ── Parse JSON response ────────────────────────────────────────────
            parsed AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    from_json(tone_json, 'STRUCT<negative:DOUBLE, neutral:DOUBLE, positive:DOUBLE, rationale:STRING>') AS tone
                FROM extracted
            )

            SELECT
                md5(CONCAT(symbol, '|', accession, '|', 'management_tone'))      AS signal_id,
                symbol,
                TRY_CAST(filing_date AS DATE)                                     AS signal_date,
                CONCAT('sec_filing_', form_type)                                  AS source_type,
                CONCAT(form_type, ' ', filing_date, ' — Management Tone')         AS source_description,
                CASE
                    WHEN tone.positive >= tone.negative AND tone.positive >= tone.neutral THEN 'Positive'
                    WHEN tone.negative >= tone.positive AND tone.negative >= tone.neutral THEN 'Negative'
                    ELSE                                                                       'Neutral'
                END                                                               AS sentiment,
                CASE
                    WHEN tone.negative >= 0.4  THEN 0.9
                    WHEN tone.negative >= 0.25 THEN 0.5
                    ELSE                            0.2
                END                                                               AS severity_score,
                tone.negative >= 0.4                                              AS advisor_action_needed,
                'Management Tone'                                                 AS signal_type,
                CONCAT(form_type, ' Management Tone')                             AS signal,
                CONCAT('[', CAST(ROUND(tone.negative, 2) AS STRING), ',',
                            CAST(ROUND(tone.neutral,  2) AS STRING), ',',
                            CAST(ROUND(tone.positive, 2) AS STRING), ']')         AS signal_value,
                tone.rationale                                                    AS rationale,
                CURRENT_TIMESTAMP()                                               AS processed_at
            FROM parsed
            WHERE tone.negative  IS NOT NULL
              AND tone.neutral   IS NOT NULL
              AND tone.positive  IS NOT NULL
              AND tone.rationale IS NOT NULL

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
    print(f"Merged management tone signals from {sec_count} SEC filings into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_type, source_description,
               signal_value AS tone_distribution,
               sentiment, advisor_action_needed, severity_score,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE signal_type = 'Management Tone'
        ORDER BY symbol, signal_date DESC
    """)
)

# COMMAND ----------
