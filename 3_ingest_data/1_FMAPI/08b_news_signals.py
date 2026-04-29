# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Enrich bronze_stock_news with AI-generated signals using Databricks built-in SQL functions.
# Reads only articles not yet present in silver_news_signals (anti-join idempotency).
# AI functions are called exactly once per article — never re-processed on re-runs.
#
# Enrichment pipeline per article:
#   1. JOIN bronze_company_profiles + bronze_etf_info to build a context-rich input string
#   2. ai_sentiment(context_text)                      → sentiment label
#   3. ai_classify(context_text, signal_type_labels)   → signal_type
#   4. ai_classify(context_text, materiality_labels)   → materiality
#   5. SQL logic: High materiality + actionable signal → advisor_action boolean
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals
# Run after: 3_ingest_data/1_FMAPI/08_news.py

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# # Uncomment to fully reset — drops the silver table so all articles are re-processed
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals (
        symbol         STRING,
        url            STRING,
        publishedDate  STRING,
        title          STRING,
        site           STRING,
        companyName    STRING,
        sector         STRING,
        industry       STRING,
        sentiment      STRING,
        signal_type    STRING,
        materiality    STRING,
        advisor_action BOOLEAN,
        processed_at   TIMESTAMP
    )
    USING DELTA
""")

# COMMAND ----------

# Count articles in bronze that have not yet been enriched in silver.
# This gates the expensive AI function calls — skipped entirely if nothing is new.
unprocessed_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
        ON n.symbol = s.symbol AND n.url = s.url
    WHERE n.title IS NOT NULL
      AND LENGTH(TRIM(COALESCE(n.summary, n.title, ''))) > 0
""").collect()[0][0]

print(f"Unprocessed articles: {unprocessed_count}")

# COMMAND ----------

if unprocessed_count == 0:
    print("No new articles — silver_news_signals is up to date.")
else:
    spark.sql(f"""
        INSERT INTO {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals

        -- Step 1: isolate unprocessed articles
        WITH unprocessed AS (
            SELECT n.*
            FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
            LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
                ON n.symbol = s.symbol AND n.url = s.url
            WHERE n.title IS NOT NULL
              AND LENGTH(TRIM(COALESCE(n.summary, n.title, ''))) > 0
        ),

        -- Step 2: join company + ETF metadata and build context string
        -- Context gives the AI functions sector/industry background so classifications
        -- are grounded in the company's business — not just the headline alone.
        context AS (
            SELECT
                u.symbol,
                u.url,
                u.publishedDate,
                u.title,
                u.site,
                COALESCE(cp.companyName, u.symbol)  AS companyName,
                COALESCE(cp.sector,      'Unknown')  AS sector,
                COALESCE(cp.industry,    'Unknown')  AS industry,
                CONCAT(
                    COALESCE(cp.companyName, u.symbol),
                    ' (', u.symbol,
                    ', ', COALESCE(cp.sector,   'Unknown'),
                    ', ', COALESCE(cp.industry, 'Unknown'),
                    CASE WHEN ei.assetClass IS NOT NULL
                         THEN ', ' || ei.assetClass
                         ELSE ''
                    END,
                    '): ',
                    u.title,
                    '. ',
                    COALESCE(u.summary, '')
                ) AS context_text
            FROM unprocessed u
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
                ON u.symbol = cp.symbol
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info ei
                ON u.symbol = ei.symbol
        ),

        -- Step 3: call each AI function exactly once per article
        signals AS (
            SELECT
                symbol,
                url,
                publishedDate,
                title,
                site,
                companyName,
                sector,
                industry,
                ai_sentiment(context_text) AS sentiment,
                ai_classify(
                    context_text,
                    array('Earnings', 'M&A', 'Credit Event',
                          'Management Change', 'Guidance', 'Regulatory', 'Other')
                )                          AS signal_type,
                ai_classify(
                    context_text,
                    array('High', 'Medium', 'Low')
                )                          AS materiality
            FROM context
        )

        -- Step 4: derive advisor_action in pure SQL — no extra AI call
        SELECT
            symbol,
            url,
            publishedDate,
            title,
            site,
            companyName,
            sector,
            industry,
            sentiment,
            signal_type,
            materiality,
            CASE
                WHEN materiality = 'High'
                 AND signal_type IN ('Credit Event', 'M&A', 'Guidance', 'Management Change')
                THEN true
                ELSE false
            END          AS advisor_action,
            CURRENT_TIMESTAMP() AS processed_at
        FROM signals
    """)
    print(f"Enriched and inserted {unprocessed_count} articles into silver_news_signals.")

# COMMAND ----------

display(
    spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.silver_news_signals")
        .orderBy("processed_at", ascending=False)
)
