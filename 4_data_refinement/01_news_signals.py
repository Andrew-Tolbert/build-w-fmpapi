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
# Reads silver_news_signals (already AI-enriched by 3_ingest_data/1_FMAPI/08b_news_signals.py)
# and maps relevant, fully-enriched articles into gold_unified_signals.
# Adds an AI-generated rationale explaining why the signal warrants advisor attention.
#
# Idempotency: anti-join on (source_type='news', symbol, source_id=url) — only new articles
# are processed on re-runs, so each ai_query call runs at most once per article.
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'news')
# Run after: 3_ingest_data/1_FMAPI/08b_news_signals.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number to cap for testing

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
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.source_type = 'news' AND g.source_id = s.url AND g.symbol = s.symbol
    WHERE s.is_relevant = true
      AND s.signal_type IS NOT NULL
      AND s.sentiment   IS NOT NULL
""").collect()[0][0]

print(f"News articles to map into gold_unified_signals: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new news signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            WITH new_articles AS (
                SELECT
                    s.symbol,
                    s.url,
                    s.publishedDate,
                    s.title,
                    s.site,
                    s.companyName,
                    s.sector,
                    s.industry,
                    s.sentiment,
                    s.signal_type,
                    s.materiality,
                    s.advisor_action
                FROM {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.source_type = 'news' AND g.source_id = s.url AND g.symbol = s.symbol
                WHERE s.is_relevant = true
                  AND s.signal_type IS NOT NULL
                  AND s.sentiment   IS NOT NULL
                {_limit_clause}
            ),

            enriched AS (
                SELECT
                    symbol, url, publishedDate, title, site,
                    companyName, sector, industry,
                    sentiment, signal_type, materiality, advisor_action,
                    TRIM(ai_query(
                        '{LLM_ENDPOINT}',
                        CONCAT(
                            'You are a Goldman Sachs wealth advisor assistant. ',
                            'Write exactly 2-3 sentences explaining why this news about ',
                            COALESCE(companyName, symbol), ' (', symbol, ') warrants attention ',
                            'and what action a UHNW wealth advisor should consider.\\n',
                            'Article: "', title, '" — published by ', site, ' on ', publishedDate, '\\n',
                            'AI classification: ', signal_type, ' event | sentiment: ', sentiment,
                            ' | materiality: ', materiality, '\\n',
                            'Sector context: ', COALESCE(sector, 'Unknown'),
                            ' / ', COALESCE(industry, 'Unknown'), '\\n',
                            'Be specific and direct. Do not start with "I" or repeat the classification labels.'
                        )
                    )) AS rationale
                FROM new_articles
            )

            SELECT
                md5(CONCAT(symbol, '|', url))                         AS signal_id,
                symbol,
                TRY_CAST(publishedDate AS DATE)                        AS signal_date,
                'news'                                                 AS source_type,
                url                                                    AS source_id,
                title                                                  AS source_description,
                INITCAP(COALESCE(sentiment, 'neutral'))                AS sentiment,
                CASE materiality
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                                    AS severity_score,
                COALESCE(advisor_action, false)                        AS advisor_action_needed,
                signal_type,
                rationale,
                CURRENT_TIMESTAMP()                                    AS processed_at
            FROM enriched

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
    print(f"Merged {new_count} news signals into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, sentiment, severity_score,
               advisor_action_needed, signal_type, LEFT(rationale, 200) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'news'
        ORDER BY processed_at DESC, severity_score DESC
    """)
)

# COMMAND ----------
