# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from SEC filings (10-K, 10-Q, 8-K, etc.).
#
# Reads sec_filing_chunks (populated by 3_ingest_data/1_FMAPI/12_sec_parsing.py),
# aggregates text chunks per filing document (accession), then uses Databricks AI
# Functions to extract sentiment, classify the filing event, assess severity, and
# generate an advisor-facing rationale.
#
# Processing logic:
#   1. Group chunks by (symbol, accession, form_type, filing_date) — one signal per filing
#   2. Only process is_latest = true filings (most recent per ticker + form type)
#   3. Concatenate chunks in order up to 8000 chars for AI context
#   4. Three-function AI pipeline:
#      - ai_analyze_sentiment  → sentiment direction
#      - ai_classify           → signal type (Earnings/Credit Event/M&A/Guidance/Regulatory/Other)
#      - ai_classify           → severity (High/Medium/Low)
#      - ai_query              → 2-3 sentence rationale
#   5. MERGE into gold_unified_signals on signal_id = md5(symbol | accession)
#
# Idempotency: anti-join on (source_type='sec_filing', source_id=accession, symbol).
# Only accessions not yet in gold are processed on re-runs.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'sec_filing')
# Run after: 3_ingest_data/1_FMAPI/12_sec_parsing.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number to cap for testing

# COMMAND ----------

# # Uncomment to reset SEC filing signals only
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE source_type = 'sec_filing'")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals (
        signal_id             STRING,
        symbol                STRING,
        signal_date           DATE,
        source_type           STRING,
        source_id             STRING,
        source_description    STRING,
        sentiment             STRING,
        severity_score        DOUBLE,
        advisor_action_needed BOOLEAN,
        signal_type           STRING,
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
    SELECT COUNT(DISTINCT c.accession)
    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.source_type = 'sec_filing' AND g.source_id = c.accession AND g.symbol = c.symbol
    WHERE c.is_latest = true
""").collect()[0][0]

print(f"SEC filings to map into gold_unified_signals: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new SEC filing signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Step 1: Aggregate text chunks per filing ─────────────────────────
            WITH new_filings AS (
                SELECT DISTINCT symbol, accession
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.source_type = 'sec_filing' AND g.source_id = c.accession AND g.symbol = c.symbol
                WHERE c.is_latest = true
                {_limit_clause}
            ),

            -- Prioritise informative sections when concatenating:
            -- 10-K: mda first, then risk_factors, then rest
            -- 8-K / other: all chunks in chunk_index order
            aggregated AS (
                SELECT
                    c.symbol,
                    c.form_type,
                    c.filing_date,
                    c.accession,
                    -- Concatenate chunks ordered by section priority then chunk_index.
                    -- LEFT() truncates to 8000 chars so the AI context stays within limits.
                    LEFT(
                        CONCAT_WS(
                            '\n\n',
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
                        8000
                    ) AS document_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                JOIN new_filings nf USING (symbol, accession)
                WHERE c.is_latest = true
                GROUP BY c.symbol, c.form_type, c.filing_date, c.accession
            ),

            -- ── Step 2: Build context string for AI functions ─────────────────
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

            -- ── Step 3: AI signal extraction ──────────────────────────────────
            signals AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    ai_analyze_sentiment(context_text) AS sentiment_raw,
                    ai_classify(
                        context_text,
                        '{{"Earnings":          "Quarterly or annual financial results, EPS beats/misses, revenue surprises, profit warnings, or financial condition disclosures",
                          "Credit Event":      "Covenant breach, credit rating change, debt restructuring, non-accrual designation, NAV deterioration, liquidity issues, or default risk",
                          "M&A":               "Merger, acquisition, divestiture, takeover, spin-off, or major strategic transaction",
                          "Guidance":          "Forward revenue or earnings guidance raised, lowered, or withdrawn; management outlook changes",
                          "Management Change": "CEO, CFO, CIO, or board-level executive departure, appointment, or transition",
                          "Regulatory":        "SEC enforcement, CFTC action, government investigation, fine, sanction, or material regulatory development",
                          "Other":             "Other material disclosures not covered above"}}',
                        MAP(
                            'instructions',
                            'You are classifying SEC filings for a Goldman Sachs wealth management platform. Assign the PRIMARY financial significance category. For 10-K filings, assess the most material theme in the document. For 8-K filings, classify the specific triggering event. Credit Events are highest priority — flag any BDC non-accrual, covenant breach, or NAV deterioration. For 424B filings (prospectus supplements), classify as Guidance if they describe capital raises or dividend policy changes.'
                        )
                    ) AS signal_type_raw,
                    ai_classify(
                        context_text,
                        '{{"High":   "Filing discloses material negative developments requiring immediate client notification — earnings miss, credit deterioration, covenant breach, regulatory penalty, M&A surprise, CEO departure, guidance cut",
                          "Medium": "Filing provides important context that should be monitored — analyst day, routine guidance update, minor regulatory matter, management commentary",
                          "Low":    "Routine administrative filing with minimal investment relevance — exhibit amendments, insider transactions, boilerplate disclosures"}}',
                        MAP(
                            'instructions',
                            'You are assessing the financial materiality of SEC filings for Goldman Sachs UHNW wealth management. BDC credit events (NAV declines, non-accruals, PIK toggles) are always High materiality. 10-K annual reports are at least Medium. 8-K filings about earnings results: High if miss, Medium if beat. Prospectus supplements for BDCs can be High if they indicate capital stress.'
                        )
                    ) AS severity_raw,
                    TRIM(ai_query(
                        '{LLM_ENDPOINT}',
                        CONCAT(
                            'You are a Goldman Sachs wealth advisor assistant. Write exactly 2-3 sentences explaining ',
                            'the key investment implication of this SEC filing for a UHNW wealth advisor.\\n',
                            'Issuer: ', symbol, ' | Form: ', form_type, ' | Filed: ', filing_date, '\\n',
                            'Document excerpt:\\n',
                            LEFT(context_text, 3000), '\\n\\n',
                            'Be specific about financial impact. Do not start with "I" or summarize all details — focus on what the advisor needs to act on.'
                        )
                    )) AS rationale
                FROM context
            )

            SELECT
                md5(CONCAT(symbol, '|', accession))       AS signal_id,
                symbol,
                TRY_CAST(filing_date AS DATE)              AS signal_date,
                'sec_filing'                               AS source_type,
                accession                                  AS source_id,
                CONCAT(form_type, ' — ', filing_date)      AS source_description,
                INITCAP(COALESCE(sentiment_raw, 'neutral')) AS sentiment,
                CASE severity_raw:response[0]::STRING
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                        AS severity_score,
                CASE WHEN severity_raw:response[0]::STRING = 'High'
                      AND signal_type_raw:response[0]::STRING
                          IN ('Credit Event', 'Earnings', 'M&A', 'Management Change', 'Guidance')
                     THEN true
                     ELSE false
                END                                        AS advisor_action_needed,
                COALESCE(signal_type_raw:response[0]::STRING, 'Other') AS signal_type,
                rationale,
                CURRENT_TIMESTAMP()                        AS processed_at
            FROM signals

        ) AS src
        ON tgt.signal_id = src.signal_id
        WHEN MATCHED THEN UPDATE SET
            tgt.sentiment             = src.sentiment,
            tgt.severity_score        = src.severity_score,
            tgt.advisor_action_needed = src.advisor_action_needed,
            tgt.signal_type           = src.signal_type,
            tgt.rationale             = src.rationale,
            tgt.processed_at          = src.processed_at
        WHEN NOT MATCHED THEN INSERT (
            signal_id, symbol, signal_date, source_type, source_id, source_description,
            sentiment, severity_score, advisor_action_needed, signal_type, rationale, processed_at
        ) VALUES (
            src.signal_id, src.symbol, src.signal_date, src.source_type, src.source_id,
            src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
            src.signal_type, src.rationale, src.processed_at
        )
    """)
    print(f"Merged {new_count} SEC filing signals into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, sentiment, severity_score,
               advisor_action_needed, signal_type, LEFT(rationale, 200) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'sec_filing'
        ORDER BY processed_at DESC, severity_score DESC
    """)
)

# COMMAND ----------
