# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from earnings call transcripts — section-aware.
#
# Reads bronze_transcript_chunks (populated by 3_ingest_data/1_FMAPI/10_transcripts.py).
# Processes prepared_remarks and qa sections independently: each section has its own
# classification vocabulary, severity rubric, and rationale prompt tuned to what that
# section actually reveals.
#
# PREPARED REMARKS — the scripted message management chose to deliver:
#   Signal types: Earnings, Guidance, Credit Event, Operational Update, Management Change
#   Focus: results vs expectations, guidance direction, BDC NAV/coverage/portfolio quality
#
# Q&A — the unscripted session that reveals what analysts are actually worried about:
#   Signal types: Credit Stress Probe, Guidance Pressure, Surprise Disclosure,
#                 Analyst Concern, Macro Risk
#   Focus: what analysts probed, whether management was evasive, new information
#          surfaced only under questioning (absent from prepared remarks)
#
# Signal ID: md5(symbol|year|quarter|call_section) — one row per section per call.
# NOTE: if re-running after the old single-row-per-transcript schema, clear first:
#   DELETE FROM gold_unified_signals WHERE source_type = 'earnings_transcript'
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'earnings_transcript')
# Run after: 3_ingest_data/1_FMAPI/10_transcripts.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number (e.g. "10") to cap sections for testing

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

# Count distinct (symbol, year, quarter, call_section) combos not yet in gold.
# Each section is independently idempotent — a prepared_remarks signal can exist
# without its qa counterpart if the notebook was interrupted mid-run.
new_count = spark.sql(f"""
    SELECT COUNT(DISTINCT CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section))
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section))
    WHERE c.chunk_text IS NOT NULL
""").collect()[0][0]

print(f"Transcript sections to map into gold_unified_signals: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new transcript section signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find sections not yet in gold ────────────────────────────────────
            WITH new_sections AS (
                SELECT DISTINCT c.symbol, c.year, c.quarter, c.call_section, c.call_date, c.title
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', CAST(c.year AS STRING), '|', CAST(c.quarter AS STRING), '|', c.call_section))
                WHERE c.chunk_text IS NOT NULL
                {_limit_clause}
            ),

            -- ── Aggregate chunks per section in order ─────────────────────────
            -- 6 000 char cap leaves room for the system header in context_text.
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
                        6000
                    ) AS section_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks c
                JOIN new_sections ns USING (symbol, year, quarter, call_section)
                GROUP BY c.symbol, c.year, c.quarter, c.call_section, c.call_date, c.title
            ),

            -- ── Build framed context string ───────────────────────────────────
            -- Label the section explicitly so the model knows what it is reading.
            context AS (
                SELECT
                    symbol, year, quarter, call_section, call_date, title,
                    CONCAT(
                        'Earnings Call Transcript — ',
                        CASE call_section
                            WHEN 'prepared_remarks' THEN 'Prepared Remarks (management-scripted presentation)'
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

            -- ── AI signal extraction ──────────────────────────────────────────
            -- Signal type, severity, and rationale prompts differ by section.
            -- prepared_remarks / full_text → what management disclosed
            -- qa                           → what analysts uncovered
            signals AS (
                SELECT
                    symbol, year, quarter, call_section, call_date, title,

                    ai_analyze_sentiment(context_text) AS sentiment_raw,

                    -- ── Signal type — different vocabulary per section ──────────
                    CASE
                    WHEN call_section IN ('prepared_remarks', 'full_text') THEN
                        ai_classify(
                            context_text,
                            '{{"Earnings":          "Financial results discussion — EPS beat or miss vs consensus, revenue growth or contraction, margin expansion or compression, balance sheet changes reported in the call",
                              "Guidance":          "Management raises, lowers, withdraws, or maintains forward guidance on revenue, EPS, NAV per share, or dividends; any explicit outlook for the next quarter or year",
                              "Credit Event":      "BDC: non-accrual portfolio company disclosed, PIK interest toggle, NAV per share decline, covenant headroom compression, dividend coverage deterioration. Equity: debt downgrade risk, liquidity stress, restructuring announced in prepared remarks",
                              "Operational Update":"Strategic decisions disclosed in prepared remarks — acquisitions, divestitures, new products or markets, capital allocation change, cost restructuring, JV or partnership",
                              "Management Change": "CEO, CFO, CIO, or board-level executive departure, retirement, or incoming appointment announced during prepared remarks",
                              "Other":             "General industry or macro framing, routine housekeeping, or topics without a direct investment signal"}}',
                            MAP(
                                'instructions',
                                'You are classifying the PREPARED REMARKS section of an earnings call for a Goldman Sachs UHNW wealth management platform. This is the scripted, management-controlled portion. Classify by the single most material financial event management disclosed. For BDCs, Credit Event is highest priority — any non-accrual, PIK toggle, NAV decline, or dividend coverage compression trumps Earnings. For equities, distinguish between backward-looking results (Earnings) and forward-looking outlook changes (Guidance).'
                            )
                        )
                    WHEN call_section = 'qa' THEN
                        ai_classify(
                            context_text,
                            '{{"Credit Stress Probe":    "Analysts asking specifically about portfolio company health — non-accruals by name, PIK interest on borrowers, covenant headroom on specific credits, BDC NAV trajectory, or equity credit quality deterioration. Signals analysts suspect undisclosed credit stress.",
                              "Guidance Pressure":      "Analysts challenging management forward guidance — pushing back on revenue targets, margin assumptions, dividend sustainability, or NAV projections. Management hedges, qualifies, or reduces expectations under questioning.",
                              "Surprise Disclosure":    "Material information surfaced only under analyst questioning, absent from prepared remarks — a new non-accrual named, guidance reduced, write-down hinted, regulatory issue surfaced, or acquisition confirmed.",
                              "Analyst Concern":        "Concentrated analyst skepticism around a specific risk that management did not address proactively — competitive threat, operational execution, rate sensitivity, borrower concentration, or sector deterioration.",
                              "Macro Risk":             "Analysts probing sensitivity to interest rate changes, economic slowdown, regulatory shifts, or sector headwinds; management responses reveal material exposure or hedging gaps.",
                              "Other":                  "Confirmatory follow-up questions on already-disclosed topics; no new risk signals or information emerged from Q&A"}}',
                            MAP(
                                'instructions',
                                'You are classifying the Q&A section of an earnings call for a Goldman Sachs UHNW wealth management platform. Q&A reveals what analysts are actually worried about — management has already delivered the scripted message in prepared remarks. Classify by what the Q&A exchange reveals about hidden risk or new information. Credit Stress Probe and Surprise Disclosure are the highest-priority categories for BDC calls. Guidance Pressure is critical for any call where management walked back or qualified statements under questioning. If the Q&A was fully confirmatory with no new signal, use Other.'
                            )
                        )
                    END AS signal_type_raw,

                    -- ── Severity — rubric differs by section ───────────────────
                    CASE
                    WHEN call_section IN ('prepared_remarks', 'full_text') THEN
                        ai_classify(
                            context_text,
                            '{{"High":   "Material surprise requiring immediate advisor action — significant EPS miss or beat vs consensus, guidance cut or meaningful raise, BDC non-accrual or NAV decline disclosed, key executive departure, covenant breach, or dividend cut announced in prepared remarks",
                              "Medium": "Informative but no major surprise — broadly in-line results, modest guidance revision, routine credit quality update, normal capital allocation commentary. Monitor but no immediate client action needed.",
                              "Low":    "Reaffirming prepared remarks — results meet expectations, guidance maintained, no new risks identified. Confirms the existing investment thesis without requiring advisor action."}}',
                            MAP(
                                'instructions',
                                'You are assessing the materiality of prepared remarks for Goldman Sachs UHNW wealth management. BDC calls disclosing non-accruals, PIK toggles, or NAV per share decline are always High. Earnings misses combined with guidance cuts are High. Guidance raises above consensus are High. Beats with maintained guidance are Medium. In-line results with maintained guidance are Low.'
                            )
                        )
                    WHEN call_section = 'qa' THEN
                        ai_classify(
                            context_text,
                            '{{"High":   "Q&A revealed material concerns not in prepared remarks — analysts named specific credit stress (non-accruals, PIK toggles, covenant names), management hedged or walked back guidance under pressure, a Surprise Disclosure changed the investment picture, or management evasion on direct questions suggests undisclosed risk. Requires immediate advisor review.",
                              "Medium": "Analysts probed known or expected risks but management answered satisfactorily — follow-up on existing concerns, rate sensitivity questions with coherent responses, normal capital structure questions. Worth monitoring but no immediate action.",
                              "Low":    "Confirmatory Q&A — analysts sought additional color but no new concerns surfaced, management answered confidently and completely, investment thesis unchanged after Q&A."}}',
                            MAP(
                                'instructions',
                                'You are assessing the materiality of Q&A for Goldman Sachs UHNW wealth management. High if analysts surfaced a Credit Stress Probe, Surprise Disclosure, or if management clearly hedged guidance under questioning. Medium if analysts probed but were satisfied. Low if the Q&A was routine and added nothing new. BDC Q&A naming non-accruals or PIK borrowers is always High.'
                            )
                        )
                    END AS severity_raw,

                    -- ── Rationale — section-specific framing ───────────────────
                    TRIM(ai_query(
                        '{LLM_ENDPOINT}',
                        CASE
                        WHEN call_section IN ('prepared_remarks', 'full_text') THEN
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst reviewing PREPARED REMARKS. ',
                                'This is the scripted section — management controls this message. ',
                                'Write exactly 2-3 sentences for a UHNW advisor covering: ',
                                '(1) financial results vs expectations (EPS/revenue beat, miss, or in-line), ',
                                '(2) guidance direction (raised/lowered/withdrawn/maintained), ',
                                '(3) for BDCs: NAV per share direction, dividend coverage, and portfolio quality tone.\\n',
                                'Ticker: ', symbol, ' | Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                                'Be specific about numbers and direction. Do not start with "I".\\n\\n',
                                LEFT(context_text, 3000)
                            )
                        WHEN call_section = 'qa' THEN
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst reviewing the Q&A SESSION. ',
                                'Q&A reveals what analysts are actually worried about — the prepared message has already been delivered. ',
                                'Write exactly 2-3 sentences for a UHNW advisor covering: ',
                                '(1) the primary risk or concern analysts were probing, ',
                                '(2) whether management was forthcoming or evasive on that concern, ',
                                '(3) any new material information that surfaced only under questioning (absent from prepared remarks). ',
                                'For BDC Q&A: explicitly flag any analyst questions about non-accrual names, PIK borrowers, or covenant headroom.\\n',
                                'Ticker: ', symbol, ' | Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                                'Do not start with "I". Do not summarize the prepared remarks — focus only on what Q&A revealed.\\n\\n',
                                LEFT(context_text, 3000)
                            )
                        END
                    )) AS rationale

                FROM context
            )

            -- ── Final output — one row per (symbol, year, quarter, call_section) ─
            SELECT
                md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING), '|', call_section)) AS signal_id,
                symbol,
                TRY_CAST(call_date AS DATE)                                      AS signal_date,
                'earnings_transcript'                                            AS source_type,
                CONCAT(
                    symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' — ',
                    CASE call_section
                        WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                        WHEN 'qa'               THEN 'Q&A Session'
                        ELSE                         'Full Transcript'
                    END
                )                                                                AS source_description,
                INITCAP(COALESCE(sentiment_raw, 'neutral'))                      AS sentiment,
                CASE severity_raw:response[0]::STRING
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                                              AS severity_score,
                CASE
                    -- Prepared remarks: act on material financial disclosures
                    WHEN call_section IN ('prepared_remarks', 'full_text')
                      AND severity_raw:response[0]::STRING = 'High'
                      AND signal_type_raw:response[0]::STRING IN ('Credit Event', 'Earnings', 'Guidance', 'Management Change')
                    THEN true
                    -- Q&A: act when analysts surfaced something management didn't volunteer
                    WHEN call_section = 'qa'
                      AND severity_raw:response[0]::STRING = 'High'
                      AND signal_type_raw:response[0]::STRING IN ('Credit Stress Probe', 'Surprise Disclosure', 'Analyst Concern', 'Guidance Pressure')
                    THEN true
                    ELSE false
                END                                                              AS advisor_action_needed,
                COALESCE(signal_type_raw:response[0]::STRING, 'Other')          AS signal_type,
                CONCAT(
                    'Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' ',
                    CASE call_section
                        WHEN 'prepared_remarks' THEN 'Prepared Remarks'
                        WHEN 'qa'               THEN 'Q&A'
                        ELSE                         'Full Transcript'
                    END
                )                                                                AS signal,
                severity_raw:response[0]::STRING                                AS signal_value,
                rationale,
                CURRENT_TIMESTAMP()                                              AS processed_at
            FROM signals

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
    print(f"Merged {new_count} transcript section signals into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, signal, source_description, sentiment, severity_score,
               advisor_action_needed, signal_type, signal_value,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'earnings_transcript'
        ORDER BY symbol, signal_date DESC, signal
    """)
)

# COMMAND ----------
