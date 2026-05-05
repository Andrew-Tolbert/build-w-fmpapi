# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Generate Management Tone signals for earnings transcripts and SEC 10-K/8-K filings.
# Produces one overall signal per document, one signal per section, and one delta
# signal comparing each earnings call to the immediately preceding call.
#
# Signal types:
#   Management Tone - Overall    — holistic score across all sections
#   Management Tone - <section>  — score for each individual section
#     Transcript sections: prepared_remarks, qa
#     SEC sections:        mda, risk_factors, financial_statements, full_text
#   Management Tone - Delta      — tone shift vs. prior earnings call (transcripts only)
#
# Tone distribution signals store signal_value as a JSON array string:
#   signal_value = "[negative, neutral, positive]"  e.g. "[0.1,0.5,0.4]"
# The three values sum to 1.0. The frontend maps them to display labels/charts.
#
# Delta signals store signal_value as a plain-text 1-2 sentence summary of the shift.
# sentiment = Improving | Deteriorating | Stable; rationale = detailed narrative.
#
# Sources:
#   earnings_transcript  — overall + per-section + delta per (symbol, year, quarter)
#   sec_filing_10-K      — overall + per-section per filing
#   sec_filing_8-K       — overall + per-section per filing
#
# Signal ID construction (guaranteed non-colliding):
#   transcript overall:  md5(symbol|year|quarter|management_tone_overall)
#   transcript section:  md5(symbol|year|quarter|section|management_tone)
#   transcript delta:    md5(symbol|year|quarter|management_tone_delta)
#   SEC overall:         md5(symbol|accession|management_tone_overall)
#   SEC section:         md5(symbol|accession|section|management_tone)
#
# Idempotency: each (document, section) unit is scored independently; re-runs are safe.
# Backfill: reads from existing bronze tables, no FMP re-pull or HTML re-parse needed.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (signal_type LIKE 'Management Tone%')
# Run after: ingest_fmapi_10_transcripts, ingest_fmapi_13_sec_validation

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number to cap per source for testing

# COMMAND ----------

# # Uncomment to reset ALL management tone signals (overall + all sections)
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

_test_limit = dbutils.widgets.get("test_limit").strip()
_limit_clause = f"LIMIT {_test_limit}" if _test_limit else ""

# Count calls/filings missing the overall signal (used as processing anchor)
transcript_count = spark.sql(f"""
    SELECT COUNT(DISTINCT CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING)))
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', 'management_tone_overall'))
    WHERE c.chunk_text IS NOT NULL
""").collect()[0][0]

sec_count = spark.sql(f"""
    SELECT COUNT(DISTINCT c.accession)
    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', 'management_tone_overall'))
    WHERE c.is_latest = true
      AND c.form_type IN ('10-K', '8-K')
""").collect()[0][0]

print(f"Earnings calls to score: {transcript_count} (overall + per-section signals each)")
print(f"SEC filings (10-K / 8-K) to score: {sec_count} (overall + per-section signals each)")

# COMMAND ----------

# ── PART 1: Earnings transcript management tone ────────────────────────────────

if transcript_count == 0:
    print("No new earnings call management tone signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find calls missing the overall signal (anchor for this run) ──────
            WITH calls_to_score AS (
                SELECT c.symbol, c.year, c.quarter, MAX(c.call_date) AS call_date
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', 'management_tone_overall'))
                WHERE c.chunk_text IS NOT NULL
                GROUP BY c.symbol, c.year, c.quarter
                {_limit_clause}
            ),

            -- ── Per-section aggregation — 12 000 char cap per section ────────────
            section_texts AS (
                SELECT
                    c.symbol, c.year, c.quarter, cts.call_date,
                    c.call_section AS section_label,
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
                JOIN calls_to_score cts USING (symbol, year, quarter)
                WHERE c.chunk_text IS NOT NULL
                GROUP BY c.symbol, c.year, c.quarter, cts.call_date, c.call_section
            ),

            -- ── Overall: all sections combined — 24 000 char cap ────────────────
            -- Prepared remarks sorted before Q&A to preserve natural call order.
            overall_texts AS (
                SELECT
                    c.symbol, c.year, c.quarter, cts.call_date,
                    'overall' AS section_label,
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
                    ) AS section_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                JOIN calls_to_score cts USING (symbol, year, quarter)
                WHERE c.chunk_text IS NOT NULL
                GROUP BY c.symbol, c.year, c.quarter, cts.call_date
            ),

            -- ── Combine all units, filter out already-scored ─────────────────────
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
                        THEN md5(CONCAT(u.symbol, '|', CAST(u.year AS STRING), '|', CAST(u.quarter AS STRING), '|', 'management_tone_overall'))
                        ELSE md5(CONCAT(u.symbol, '|', CAST(u.year AS STRING), '|', CAST(u.quarter AS STRING), '|', u.section_label, '|management_tone'))
                    END
                WHERE u.section_text IS NOT NULL AND LENGTH(TRIM(u.section_text)) > 100
            ),

            -- ── Build framed context with section scope header ───────────────────
            context AS (
                SELECT
                    symbol, year, quarter, call_date, section_label,
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
                        '---\\n',
                        section_text
                    ) AS context_text
                FROM new_units
            ),

            -- ── One ai_query per scoring unit ────────────────────────────────────
            extracted AS (
                SELECT
                    symbol, year, quarter, call_date, section_label,
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

            -- ── Parse JSON response ──────────────────────────────────────────────
            parsed AS (
                SELECT
                    symbol, year, quarter, call_date, section_label,
                    from_json(tone_json, 'STRUCT<negative:DOUBLE, neutral:DOUBLE, positive:DOUBLE, rationale:STRING>') AS tone
                FROM extracted
            )

            SELECT
                CASE
                    WHEN section_label = 'overall'
                    THEN md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|', 'management_tone_overall'))
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
    print(f"Merged management tone signals from {transcript_count} earnings calls (overall + per-section) into gold_unified_signals.")

# COMMAND ----------

# ── PART 2: SEC 10-K and 8-K management tone ──────────────────────────────────

if sec_count == 0:
    print("No new SEC filing management tone signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find filings missing the overall signal (anchor for this run) ────
            WITH new_filings AS (
                SELECT DISTINCT c.symbol, c.accession, c.form_type, c.filing_date
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', 'management_tone_overall'))
                WHERE c.is_latest = true
                  AND c.form_type IN ('10-K', '8-K')
                {_limit_clause}
            ),

            -- ── Per-section aggregation — 20 000 char cap per section ────────────
            section_texts AS (
                SELECT
                    c.symbol, c.form_type, c.filing_date, c.accession,
                    c.section_name AS section_label,
                    LEFT(
                        CONCAT_WS(
                            '\\n\\n',
                            transform(
                                sort_array(collect_list(struct(c.chunk_index AS idx, c.chunk_text AS txt))),
                                x -> x.txt
                            )
                        ),
                        20000
                    ) AS section_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                JOIN new_filings nf USING (symbol, accession)
                WHERE c.is_latest = true
                GROUP BY c.symbol, c.form_type, c.filing_date, c.accession, c.section_name
            ),

            -- ── Overall: all sections combined — 60 000 char cap ────────────────
            -- MD&A carries the most management narrative; sorted first for tone.
            overall_texts AS (
                SELECT
                    c.symbol, c.form_type, c.filing_date, c.accession,
                    'overall' AS section_label,
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
                    ) AS section_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                JOIN new_filings nf USING (symbol, accession)
                WHERE c.is_latest = true
                GROUP BY c.symbol, c.form_type, c.filing_date, c.accession
            ),

            -- ── Combine all units, filter out already-scored ─────────────────────
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

            -- ── Build framed context with section scope header ───────────────────
            context AS (
                SELECT
                    symbol, form_type, filing_date, accession, section_label,
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
                        '---\\n',
                        section_text
                    ) AS context_text
                FROM new_units
            ),

            -- ── One ai_query per scoring unit ────────────────────────────────────
            extracted AS (
                SELECT
                    symbol, form_type, filing_date, accession, section_label,
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

            -- ── Parse JSON response ──────────────────────────────────────────────
            parsed AS (
                SELECT
                    symbol, form_type, filing_date, accession, section_label,
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
    print(f"Merged management tone signals from {sec_count} SEC filings (overall + per-section) into gold_unified_signals.")

# COMMAND ----------

# ── PART 3: Management Tone Delta — current vs. prior earnings call ────────────
# Compares each earnings call to the immediately preceding call for the same
# symbol (ordered by call_date), identifying tone shifts and new/dropped topics.

delta_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM (
        SELECT symbol, year, quarter
        FROM (
            SELECT symbol, year, quarter,
                   LAG(call_date) OVER (PARTITION BY symbol ORDER BY call_date) AS prior_call_date
            FROM (
                SELECT symbol, year, quarter, MAX(call_date) AS call_date
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
                WHERE chunk_text IS NOT NULL
                GROUP BY symbol, year, quarter
            )
        )
        WHERE prior_call_date IS NOT NULL
    ) all_deltable
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(all_deltable.symbol, '|', CAST(all_deltable.year AS STRING), '|', CAST(all_deltable.quarter AS STRING), '|management_tone_delta'))
""").collect()[0][0]

print(f"Earnings call delta signals to process: {delta_count}")

# COMMAND ----------

if delta_count == 0:
    print("No new management tone delta signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Rank calls by call_date per symbol; LAG gives the prior call ───────
            WITH call_sequence AS (
                SELECT symbol, year, quarter, call_date,
                       LAG(call_date) OVER (PARTITION BY symbol ORDER BY call_date) AS prior_call_date,
                       LAG(year)      OVER (PARTITION BY symbol ORDER BY call_date) AS prior_year,
                       LAG(quarter)   OVER (PARTITION BY symbol ORDER BY call_date) AS prior_quarter
                FROM (
                    SELECT symbol, year, quarter, MAX(call_date) AS call_date
                    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
                    WHERE chunk_text IS NOT NULL
                    GROUP BY symbol, year, quarter
                )
            ),

            -- ── Calls that have a prior call and no delta signal yet ─────────────
            calls_to_score AS (
                SELECT cs.symbol, cs.year, cs.quarter, cs.call_date,
                       cs.prior_year, cs.prior_quarter, cs.prior_call_date
                FROM call_sequence cs
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(cs.symbol, '|', CAST(cs.year AS STRING), '|', CAST(cs.quarter AS STRING), '|management_tone_delta'))
                WHERE cs.prior_call_date IS NOT NULL
                {_limit_clause}
            ),

            -- ── Current call text — 12 000 char cap ─────────────────────────────
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

            -- ── Prior call text — 12 000 char cap ───────────────────────────────
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
                    ON c.symbol = cts.symbol
                    AND c.year  = cts.prior_year
                    AND c.quarter = cts.prior_quarter
                WHERE c.chunk_text IS NOT NULL
                GROUP BY cts.symbol, cts.year, cts.quarter
            ),

            -- ── Build comparison context ─────────────────────────────────────────
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

            -- ── One ai_query per call pair ───────────────────────────────────────
            extracted AS (
                SELECT
                    symbol, year, quarter, call_date, prior_year, prior_quarter, prior_call_date,
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

            -- ── Parse JSON response ──────────────────────────────────────────────
            parsed AS (
                SELECT
                    symbol, year, quarter, call_date, prior_year, prior_quarter, prior_call_date,
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
                CASE delta.direction
                    WHEN 'Deteriorating' THEN 0.9
                    ELSE                      0.2
                END                                                                   AS severity_score,
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
    print(f"Merged management tone delta signals from {delta_count} earnings call pairs into gold_unified_signals.")

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
