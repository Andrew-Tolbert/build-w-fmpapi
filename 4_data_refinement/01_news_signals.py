# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from news articles.
#
# Two-pass pipeline from bronze_stock_news → gold_unified_signals.
#
# PASS 1 — Relevance filter  (ai_classify — fast binary decision, runs on ALL new articles):
#   1. Anti-join bronze_stock_news against gold_unified_signals to find new articles
#   2. JOIN bronze_company_profiles + bronze_etf_info to resolve full entity name
#   3. ai_classify(relevance_input, Relevant / Not Relevant)
#   4. Write only Relevant articles to a staging table
#
# PASS 2 — Signal enrichment  (ai_query — one Claude call per relevant article only):
#   5. Read from relevance staging table
#   6. Single ai_query call per article — returns JSON with all signal fields:
#        sentiment, signal_type (free-form), signal_value (High/Medium/Low),
#        advisor_action_needed, rationale
#   7. Parse JSON fields with get_json_object
#   8. MERGE into gold_unified_signals on signal_id = md5(symbol|url)
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'news')

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

# COMMAND ----------

# # Uncomment to fully reset the gold signals table (all source types)
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals")

# # Uncomment to reset only news signals
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE source_type = 'news'")

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

unprocessed_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(n.symbol, '|', n.url))
    WHERE n.title IS NOT NULL
      AND LENGTH(TRIM(COALESCE(n.full_text, n.summary, n.title, ''))) > 0
""").collect()[0][0]

print(f"Unprocessed articles: {unprocessed_count}")

# COMMAND ----------

# ── PASS 1: Relevance filter ───────────────────────────────────────────────────
# ai_classify is fast and cheap for binary decisions — runs on every unprocessed
# article and filters down to the relevant subset before hitting the LLM endpoint.

RELEVANCE_STAGING = f"{UC_CATALOG}.{UC_SCHEMA}._news_relevance_staging"

if unprocessed_count == 0:
    print("No new articles — gold_unified_signals is up to date.")
else:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {RELEVANCE_STAGING} AS

        WITH unprocessed AS (
            SELECT n.*
            FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
            LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                ON g.signal_id = md5(CONCAT(n.symbol, '|', n.url))
            WHERE n.title IS NOT NULL
              AND LENGTH(TRIM(COALESCE(n.full_text, n.summary, n.title, ''))) > 0
        ),

        context AS (
            SELECT
                u.symbol, u.url, u.publishedDate, u.title, u.site,
                u.full_text, u.summary,
                COALESCE(cp.companyName, u.symbol) AS companyName,
                COALESCE(cp.sector,      'Unknown') AS sector,
                COALESCE(cp.industry,    'Unknown') AS industry,
                CONCAT(
                    'Security being assessed: ',
                    COALESCE(ei.name, cp.companyName, u.symbol),
                    ' (ticker: ', u.symbol,
                    ', ', COALESCE(cp.sector, 'Unknown'),
                    CASE WHEN cp.industry IS NOT NULL AND cp.industry != 'Unknown'
                         THEN ', ' || cp.industry ELSE '' END,
                    CASE WHEN ei.assetClass IS NOT NULL
                         THEN ', ' || ei.assetClass ELSE '' END,
                    ').', CHAR(10),
                    'Article title: ', u.title, '.', CHAR(10),
                    'Article body: ', LEFT(COALESCE(u.full_text, u.summary, ''), 8000)
                ) AS relevance_input
            FROM unprocessed u
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp ON u.symbol = cp.symbol
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info ei         ON u.symbol = ei.symbol
        ),

        relevance AS (
            SELECT *,
                ai_classify(
                    relevance_input,
                    '{{"Relevant":     "The article is primarily about this specific company, ETF, or fund — discusses their earnings, price action, strategy, credit quality, management, or developments that would directly affect an investor holding this security.",
                      "Not Relevant": "A general market roundup, broad economic commentary, or article where this security is mentioned only tangentially or not at all. Would not provide actionable intelligence about this specific holding."}}',
                    MAP(
                        'instructions',
                        'You are a relevance filter for a Goldman Sachs wealth management platform. A news data provider has associated this article with a specific security — your job is to determine whether the article is genuinely about that security or a generic market article incorrectly tagged to this symbol. The security name and ticker are stated explicitly at the top of the input. Consider the full article body, not just the headline. An article about gold prices is relevant to GLD. An article that only mentions the ticker in passing within a broad market roundup is Not Relevant.'
                    )
                ) AS _rel_raw
            FROM context
        )

        SELECT symbol, url, publishedDate, title, site,
               full_text, summary, companyName, sector, industry
        FROM relevance
        WHERE _rel_raw:response[0]::STRING = 'Relevant'
    """)

    relevant_count = spark.sql(f"SELECT COUNT(*) FROM {RELEVANCE_STAGING}").collect()[0][0]
    print(f"Pass 1 complete — {relevant_count} relevant out of {unprocessed_count} articles.")

# COMMAND ----------

# ── PASS 2: Signal enrichment ──────────────────────────────────────────────────
# One ai_query call per relevant article. Claude returns all signal fields as JSON
# so signal_type is free-form and specific (e.g. "Non-Accrual Designation") rather
# than forced into a fixed vocabulary.

if unprocessed_count == 0:
    print("Skipping Pass 2.")
else:
    relevant_count = spark.sql(f"SELECT COUNT(*) FROM {RELEVANCE_STAGING}").collect()[0][0]

    if relevant_count == 0:
        print("No relevant articles to enrich.")
    else:
        spark.sql(f"""
            MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
            USING (

                WITH context AS (
                    SELECT
                        symbol, url, publishedDate, title, site,
                        companyName, sector, industry,
                        CONCAT(
                            COALESCE(companyName, symbol), ' (', symbol, ')',
                            CASE WHEN sector   != 'Unknown' THEN ', ' || sector   ELSE '' END,
                            CASE WHEN industry != 'Unknown' THEN ', ' || industry ELSE '' END,
                            ': ', title, '. ',
                            LEFT(COALESCE(full_text, summary, ''), 6000)
                        ) AS context_text
                    FROM {RELEVANCE_STAGING}
                ),

                analyzed AS (
                    SELECT
                        symbol, url, publishedDate, title, site,
                        TRIM(REGEXP_REPLACE(
                            ai_query(
                                '{LLM_ENDPOINT}',
                                CONCAT(
                                    'You are a Goldman Sachs wealth advisor analyst. ',
                                    'Analyze this financial news article and return ONLY a JSON object ',
                                    'with these exact fields:\\n',
                                    '  "sentiment": "Positive" | "Negative" | "Mixed" | "Neutral"\\n',
                                    '  "signal_type": specific event label in your own words — be precise ',
                                    '(e.g. "Non-Accrual Designation", "Dividend Cut Warning", ',
                                    '"Q3 Earnings Miss", "CEO Departure", "Covenant Breach Risk", ',
                                    '"Credit Rating Downgrade"). Do NOT use generic labels.\\n',
                                    '  "signal_value": "High" | "Medium" | "Low"  ',
                                    '(materiality to a UHNW portfolio)\\n',
                                    '  "advisor_action_needed": true | false\\n',
                                    '  "rationale": exactly 2-3 sentences on the investment implication ',
                                    'for a UHNW advisor — be specific, do not start with "I"\\n\\n',
                                    'Return ONLY the JSON. No markdown, no text outside the JSON.\\n\\n',
                                    context_text
                                )
                            ),
                            '```json|```', ''
                        )) AS llm_response
                    FROM context
                ),

                parsed AS (
                    SELECT
                        symbol, url, publishedDate, title,
                        get_json_object(llm_response, '$.sentiment')                          AS sentiment_raw,
                        get_json_object(llm_response, '$.signal_type')                        AS signal_type_val,
                        get_json_object(llm_response, '$.signal_value')                       AS signal_value_val,
                        CAST(get_json_object(llm_response, '$.advisor_action_needed') AS BOOLEAN) AS advisor_action_val,
                        get_json_object(llm_response, '$.rationale')                          AS rationale_val
                    FROM analyzed
                )

                SELECT
                    md5(CONCAT(symbol, '|', url))                       AS signal_id,
                    symbol,
                    TRY_CAST(publishedDate AS DATE)                      AS signal_date,
                    'news'                                               AS source_type,
                    title                                                AS source_description,
                    INITCAP(COALESCE(sentiment_raw, 'Neutral'))          AS sentiment,
                    CASE COALESCE(signal_value_val, 'Medium')
                        WHEN 'High'   THEN 0.9
                        WHEN 'Medium' THEN 0.5
                        WHEN 'Low'    THEN 0.2
                        ELSE 0.5
                    END                                                  AS severity_score,
                    COALESCE(advisor_action_val, false)                  AS advisor_action_needed,
                    COALESCE(signal_type_val, 'Other')                   AS signal_type,
                    COALESCE(signal_type_val, 'Other')                   AS signal,
                    COALESCE(signal_value_val, 'Medium')                 AS signal_value,
                    rationale_val                                        AS rationale,
                    CURRENT_TIMESTAMP()                                  AS processed_at
                FROM parsed

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
        print(f"Pass 2 complete — merged {relevant_count} relevant articles into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, sentiment, severity_score,
               advisor_action_needed, signal_type, signal_value,
               LEFT(rationale, 200) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'news'
        ORDER BY processed_at DESC, severity_score DESC
    """)
)

# COMMAND ----------
