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
# Single-pass pipeline: one ai_query call per article handles relevance judgment
# AND signal extraction simultaneously.
#
# Pipeline:
#   1. Anti-join bronze_stock_news against gold_unified_signals to find new articles
#   2. JOIN bronze_company_profiles + bronze_etf_info to resolve full entity name
#   3. One ai_query call per article — Claude decides relevance and, if relevant,
#      generates all signal fields in the same response:
#        - relevant        → filter; irrelevant articles return {"relevant": false} and are discarded
#        - sentiment       → Positive | Negative | Mixed | Neutral
#        - signal_type     → free-form specific label (e.g. "Non-Accrual Designation")
#        - signal_value    → High | Medium | Low  (materiality)
#        - advisor_action_needed → true | false
#        - rationale       → 2-3 sentence advisor-facing explanation
#   4. Parse JSON response with get_json_object; strip code fences
#   5. MERGE relevant rows into gold_unified_signals on signal_id = md5(symbol|url)
#
# Advantages over the two-pass ai_classify approach:
#   - 1 AI call per article instead of 4-5
#   - No staging table
#   - signal_type is free-form — Claude writes "Covenant Breach Warning" not just "Credit Event"
#   - All fields generated from the same reasoning pass (internally consistent)
#
# Idempotency: anti-join on signal_id — articles already in gold are never re-processed.
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

if unprocessed_count == 0:
    print("No new articles — gold_unified_signals is up to date.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Step 1: Unprocessed articles with entity context ─────────────────
            WITH unprocessed AS (
                SELECT n.*
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(n.symbol, '|', n.url))
                WHERE n.title IS NOT NULL
                  AND LENGTH(TRIM(COALESCE(n.full_text, n.summary, n.title, ''))) > 0
            ),

            -- Join company profile + ETF info so Claude knows exactly what security
            -- it's assessing relevance for — full name, sector, asset class
            context AS (
                SELECT
                    u.symbol, u.url, u.publishedDate, u.title, u.site,
                    COALESCE(cp.companyName, u.symbol)  AS companyName,
                    COALESCE(cp.sector,      'Unknown')  AS sector,
                    COALESCE(cp.industry,    'Unknown')  AS industry,
                    CONCAT(
                        'Security: ',
                        COALESCE(ei.name, cp.companyName, u.symbol),
                        ' (ticker: ', u.symbol,
                        ', ', COALESCE(cp.sector, 'Unknown'),
                        CASE WHEN cp.industry IS NOT NULL AND cp.industry != 'Unknown'
                             THEN ', ' || cp.industry ELSE '' END,
                        CASE WHEN ei.assetClass IS NOT NULL
                             THEN ', ' || ei.assetClass ELSE '' END,
                        ')\\nArticle title: ', u.title,
                        '\\nArticle body: ',
                        LEFT(COALESCE(u.full_text, u.summary, ''), 6000)
                    ) AS article_context
                FROM unprocessed u
                LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
                    ON u.symbol = cp.symbol
                LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info ei
                    ON u.symbol = ei.symbol
            ),

            -- ── Step 2: Single ai_query call per article ─────────────────────────
            -- Claude decides relevance and generates all signal fields in one shot.
            -- Irrelevant articles return {{"relevant": false}} — short, cheap response.
            -- Relevant articles return the full JSON with all signal fields.
            analyzed AS (
                SELECT
                    symbol, url, publishedDate, title, site,
                    companyName, sector, industry,
                    TRIM(REGEXP_REPLACE(
                        ai_query(
                            '{LLM_ENDPOINT}',
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst. ',
                                'Analyze this news article and determine if it is genuinely about the ',
                                'specified security or a generic market article incorrectly tagged to it.\\n\\n',
                                'If NOT genuinely about this specific security, return ONLY:\\n',
                                '{{"relevant": false}}\\n\\n',
                                'If the article IS about this security, return a JSON object with:\\n',
                                '  "relevant": true\\n',
                                '  "sentiment": "Positive" | "Negative" | "Mixed" | "Neutral"\\n',
                                '  "signal_type": precise event label in your own words — be specific ',
                                '(e.g. "Non-Accrual Designation", "Dividend Cut Warning", "Q3 Earnings Miss", ',
                                '"CEO Departure", "Covenant Breach Risk", "Credit Rating Downgrade"). ',
                                'Do NOT default to generic labels.\\n',
                                '  "signal_value": "High" | "Medium" | "Low"  (materiality to a UHNW portfolio)\\n',
                                '  "advisor_action_needed": true | false\\n',
                                '  "rationale": exactly 2-3 sentences explaining the investment implication ',
                                'for a UHNW advisor — be specific, do not start with "I"\\n\\n',
                                'Return ONLY the JSON object. No markdown, no explanation outside the JSON.\\n\\n',
                                article_context
                            )
                        ),
                        '```json|```', ''
                    )) AS llm_response
                FROM context
            ),

            -- ── Step 3: Filter to relevant articles and parse JSON fields ────────
            relevant AS (
                SELECT
                    symbol, url, publishedDate, title, site,
                    companyName, sector, industry,
                    get_json_object(llm_response, '$.sentiment')               AS sentiment_raw,
                    get_json_object(llm_response, '$.signal_type')             AS signal_type_val,
                    get_json_object(llm_response, '$.signal_value')            AS signal_value_val,
                    CAST(get_json_object(llm_response, '$.advisor_action_needed') AS BOOLEAN) AS advisor_action_val,
                    get_json_object(llm_response, '$.rationale')               AS rationale_val
                FROM analyzed
                WHERE get_json_object(llm_response, '$.relevant') = 'true'
            )

            SELECT
                md5(CONCAT(symbol, '|', url))                          AS signal_id,
                symbol,
                TRY_CAST(publishedDate AS DATE)                         AS signal_date,
                'news'                                                  AS source_type,
                title                                                   AS source_description,
                INITCAP(COALESCE(sentiment_raw, 'Neutral'))             AS sentiment,
                CASE COALESCE(signal_value_val, 'Medium')
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.5
                END                                                     AS severity_score,
                COALESCE(advisor_action_val, false)                     AS advisor_action_needed,
                COALESCE(signal_type_val, 'Other')                      AS signal_type,
                COALESCE(signal_type_val, 'Other')                      AS signal,
                COALESCE(signal_value_val, 'Medium')                    AS signal_value,
                rationale_val                                           AS rationale,
                CURRENT_TIMESTAMP()                                     AS processed_at
            FROM relevant

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
    print(f"Merged news signals into gold_unified_signals.")

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
