# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from earnings call transcripts.
#
# Reads bronze_transcripts (populated by 3_ingest_data/1_FMAPI/10_transcripts.py)
# and uses Databricks AI Functions to extract earnings-specific signals for each call.
#
# For earnings transcripts, the key signals are:
#   - EPS / revenue performance vs expectations (beat, miss, in-line)
#   - Forward guidance changes (raised, lowered, maintained, withdrawn)
#   - Credit quality commentary (especially critical for BDCs: NAV, coverage, non-accruals)
#   - Management tone and macro outlook statements
#   - Unusual language around liquidity, leverage, or covenant headroom
#
# Processing logic:
#   1. Anti-join against gold_unified_signals to find un-processed transcripts
#   2. Build context from the full `content` field (truncated to 8000 chars)
#   3. Three-function AI pipeline:
#      - ai_analyze_sentiment  → overall call sentiment
#      - ai_classify           → primary signal type
#      - ai_classify           → severity (High/Medium/Low)
#      - ai_query              → 2-3 sentence advisor-facing rationale
#   4. MERGE into gold_unified_signals on signal_id = md5(symbol|year|quarter)
#
# Idempotency: anti-join on (source_type='earnings_transcript', symbol, source_id).
# Only new transcripts are processed on re-runs.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'earnings_transcript')
# Run after: 3_ingest_data/1_FMAPI/10_transcripts.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number to cap for testing

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

new_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts t
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(t.symbol, '|', CAST(t.year AS STRING), '|', CAST(t.quarter AS STRING)))
    WHERE t.content IS NOT NULL
      AND LENGTH(TRIM(t.content)) > 200
""").collect()[0][0]

print(f"Earnings transcripts to map into gold_unified_signals: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new earnings transcript signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            WITH new_transcripts AS (
                SELECT t.symbol, t.year, t.quarter, t.date, t.title, t.content
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts t
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(t.symbol, '|', CAST(t.year AS STRING), '|', CAST(t.quarter AS STRING)))
                WHERE t.content IS NOT NULL
                  AND LENGTH(TRIM(t.content)) > 200
                {_limit_clause}
            ),

            -- Build context for AI analysis
            -- Truncate transcript content to 8000 chars; prepared remarks are in the first half
            -- which typically contains the most structured financial commentary
            context AS (
                SELECT
                    symbol, year, quarter, date, title,
                    CONCAT(
                        'Earnings Call Transcript\\n',
                        'Ticker: ', symbol, '\\n',
                        'Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                        CASE WHEN title IS NOT NULL THEN CONCAT('Title: ', title, '\\n') ELSE '' END,
                        '---\\n',
                        LEFT(content, 8000)
                    ) AS context_text
                FROM new_transcripts
            ),

            -- AI signal extraction
            signals AS (
                SELECT
                    symbol, year, quarter, date, title,
                    ai_analyze_sentiment(context_text) AS sentiment_raw,
                    ai_classify(
                        context_text,
                        '{{"Earnings":          "Quarterly results discussion — EPS beat or miss, revenue growth or contraction, margin commentary, balance sheet changes",
                          "Guidance":          "Management raises, lowers, or withdraws forward guidance on revenue, EPS, NAV, or dividends; explicit outlook statements for next quarter or year",
                          "Credit Event":      "BDC-specific: non-accrual portfolio company mentions, PIK interest toggle, NAV decline discussion, covenant headroom compression, credit quality deterioration; or for equities: debt downgrade risk, liquidity stress",
                          "Management Change": "CEO, CFO, CIO, or key executive departure, retirement, or transition announced on the call",
                          "Other":             "General macro commentary, product updates, or other topics without a primary financial signal"}}',
                        MAP(
                            'instructions',
                            'You are classifying earnings call transcripts for a Goldman Sachs wealth management platform. The portfolio includes public equities, ETFs, and BDC private credit vehicles. For BDC calls, Credit Event is the highest-priority category — flag any mention of non-accrual borrowers, PIK toggles, NAV per share decline, or coverage ratio compression. For equity calls, assess whether the primary theme is results performance (Earnings), outlook changes (Guidance), or something else. Choose the single most impactful category.'
                        )
                    ) AS signal_type_raw,
                    ai_classify(
                        context_text,
                        '{{"High":   "Call contains material negative or positive surprise — significant EPS miss/beat, guidance cut/raise, BDC non-accrual disclosure, NAV deterioration, executive departure, or covenant breach discussion. Requires immediate advisor review.",
                          "Medium": "Call is informational with some investment relevance — in-line results, minor guidance revision, sector commentary, management color on macro environment. Worth monitoring.",
                          "Low":    "Routine earnings call with no surprises, no guidance change, and no credit stress. Generally reaffirms existing thesis."}}',
                        MAP(
                            'instructions',
                            'You are assessing earnings call materiality for Goldman Sachs UHNW wealth management. BDC calls mentioning non-accruals, PIK toggles, or NAV decline are always High severity. Earnings misses combined with guidance cuts are High. Beats without guidance raise are typically Medium. Purely reaffirming calls with no new information are Low.'
                        )
                    ) AS severity_raw,
                    TRIM(ai_query(
                        '{LLM_ENDPOINT}',
                        CONCAT(
                            'You are a Goldman Sachs wealth advisor assistant. Write exactly 2-3 sentences ',
                            'summarizing the key investment takeaway from this earnings call for a UHNW advisor.\\n',
                            'Company: ', symbol, ' | Period: Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), '\\n',
                            'Focus on: (1) EPS/revenue performance vs expectations, (2) guidance changes, ',
                            '(3) credit quality signals if a BDC, (4) any surprise that changes the investment thesis.\\n',
                            'Transcript excerpt:\\n',
                            LEFT(context_text, 3000), '\\n\\n',
                            'Be specific and direct. Do not start with "I" or list everything — focus on the most actionable insight.'
                        )
                    )) AS rationale
                FROM context
            )

            SELECT
                md5(CONCAT(symbol, '|', CAST(year AS STRING), '|', CAST(quarter AS STRING))) AS signal_id,
                symbol,
                TRY_CAST(date AS DATE)                               AS signal_date,
                'earnings_transcript'                                AS source_type,
                COALESCE(title, CONCAT(symbol, ' Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' Earnings Call')) AS source_description,
                INITCAP(COALESCE(sentiment_raw, 'neutral'))          AS sentiment,
                CASE severity_raw:response[0]::STRING
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                                  AS severity_score,
                CASE WHEN severity_raw:response[0]::STRING = 'High'
                      AND signal_type_raw:response[0]::STRING
                          IN ('Credit Event', 'Guidance', 'Earnings', 'Management Change')
                     THEN true
                     ELSE false
                END                                                  AS advisor_action_needed,
                COALESCE(signal_type_raw:response[0]::STRING, 'Earnings') AS signal_type,
                CONCAT('Q', CAST(quarter AS STRING), ' ', CAST(year AS STRING), ' Earnings Call') AS signal,
                severity_raw:response[0]::STRING                     AS signal_value,
                rationale,
                CURRENT_TIMESTAMP()                                  AS processed_at
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
    print(f"Merged {new_count} earnings transcript signals into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, sentiment, severity_score,
               advisor_action_needed, signal_type, LEFT(rationale, 200) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'earnings_transcript'
        ORDER BY processed_at DESC, severity_score DESC
    """)
)

# COMMAND ----------
