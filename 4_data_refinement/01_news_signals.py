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
# Full two-pass pipeline directly from bronze_stock_news → gold_unified_signals.
# Incorporates and deprecates 3_ingest_data/1_FMAPI/08b_news_signals.py.
#
# PASS 1 — Relevance filter (runs on ALL unprocessed articles, one AI call each):
#   1. Anti-join bronze_stock_news against gold_unified_signals to find new articles
#   2. JOIN bronze_company_profiles + bronze_etf_info to resolve full entity name
#   3. ai_classify(relevance_input, Relevant / Not Relevant)
#   4. Write relevant articles to a staging table (irrelevant ones are discarded)
#
# PASS 2 — Signal enrichment (runs ONLY on relevant articles from staging):
#   5. ai_analyze_sentiment(context_text)              → sentiment
#   6. ai_classify(context_text, signal_type_labels)   → signal_type / signal
#   7. ai_classify(context_text, materiality_labels)   → materiality / signal_value
#   8. ai_query(context_text)                          → rationale (2-3 sentences)
#   9. Derive advisor_action_needed from signal_type + materiality
#  10. MERGE into gold_unified_signals on signal_id = md5(symbol|url)
#
# Idempotency: anti-join on signal_id — articles already in gold are never re-processed.
# Irrelevant articles are not tracked; they will be re-evaluated on subsequent runs
# (they fail relevance and are never inserted, so this is safe for a demo workload).
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'news')

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
# Calls ai_classify once per article to determine relevance.
# Only articles classified as Relevant proceed to Pass 2.
# Results written to a staging table so Pass 2 can reference them cleanly.

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
            {_limit_clause}
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
                    ').',
                    CHAR(10), 'Article title: ', u.title, '.',
                    CHAR(10), 'Article body: ',
                    LEFT(COALESCE(u.full_text, u.summary, ''), 8000)
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

        SELECT
            symbol, url, publishedDate, title, site,
            full_text, summary, companyName, sector, industry
        FROM relevance
        WHERE _rel_raw:response[0]::STRING = 'Relevant'
    """)

    relevant_count = spark.sql(f"SELECT COUNT(*) FROM {RELEVANCE_STAGING}").collect()[0][0]
    print(f"Pass 1 complete — {relevant_count} relevant out of {unprocessed_count} articles.")

# COMMAND ----------

# ── PASS 2: Signal enrichment + MERGE ─────────────────────────────────────────
# Runs only on articles in the relevance staging table.
# Three AI classify calls + one ai_query for the rationale.

if unprocessed_count == 0:
    print("Skipping Pass 2 — nothing to process.")
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
                            COALESCE(companyName, symbol),
                            ' (', symbol,
                            ', ', COALESCE(sector,   'Unknown'),
                            ', ', COALESCE(industry, 'Unknown'),
                            '): ',
                            title, '. ',
                            LEFT(COALESCE(full_text, summary, ''), 8000)
                        ) AS context_text
                    FROM {RELEVANCE_STAGING}
                ),

                signals AS (
                    SELECT
                        symbol, url, publishedDate, title, site,
                        companyName, sector, industry,
                        ai_analyze_sentiment(context_text) AS sentiment_raw,
                        ai_classify(
                            context_text,
                            '{{"Earnings":          "Quarterly or annual earnings results, EPS beats or misses, revenue surprises, profit warnings",
                              "M&A":               "Mergers, acquisitions, divestitures, takeover bids, spin-offs, or strategic combinations",
                              "Credit Event":      "Covenant breaches, credit rating downgrades, non-accrual designations, PIK interest toggles, debt restructurings, or NAV deterioration in BDCs",
                              "Management Change": "CEO, CFO, CIO, or board-level departures, appointments, or leadership transitions",
                              "Guidance":          "Forward revenue or earnings guidance raised, lowered, or withdrawn by company management",
                              "Regulatory":        "Government investigations, SEC enforcement actions, fines, sanctions, compliance failures, or new regulations materially affecting the company",
                              "Other":             "News that does not fit the above categories"}}',
                            MAP(
                                'instructions',
                                'You are classifying financial news articles for a Goldman Sachs wealth management platform serving ultra-high-net-worth clients. Securities include large-cap equities, sector ETFs, and Business Development Companies (BDCs) that lend to private companies. Classify each article by its PRIMARY financial significance to an investor holding the security. Credit Events are the highest-priority category — prioritize this label for any BDC non-accrual designation, PIK interest toggle, covenant headroom compression, or NAV deterioration. Classify based on financial impact to the position, not the tone of the article.'
                            )
                        ) AS _signal_type_raw,
                        ai_classify(
                            context_text,
                            '{{"High":   "Likely to move the position price more than 5%, or requires an immediate client conversation — earnings misses, M&A announcements, credit downgrades, CEO departures, covenant breaches",
                              "Medium": "Relevant context that should be monitored but does not require immediate action — analyst rating changes, minor guidance revisions, sector commentary, management commentary",
                              "Low":    "Informational or background news with minimal near-term price impact — product updates, routine filings, general industry news"}}',
                            MAP(
                                'instructions',
                                'You are assessing the financial materiality of news articles for a Goldman Sachs wealth management platform. Consider the potential impact on portfolio positions held by ultra-high-net-worth clients. BDC credit events and private credit stress signals should be rated High even when described in neutral language, as they directly affect client income and capital.'
                            )
                        ) AS _materiality_raw,
                        context_text
                    FROM context
                ),

                enriched AS (
                    SELECT
                        symbol, url, publishedDate, title, site,
                        companyName, sector, industry,
                        sentiment_raw,
                        _signal_type_raw:response[0]::STRING AS signal_type_val,
                        _materiality_raw:response[0]::STRING AS materiality_val,
                        TRIM(ai_query(
                            '{LLM_ENDPOINT}',
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor assistant. ',
                                'Write exactly 2-3 sentences explaining why this news about ',
                                COALESCE(companyName, symbol), ' (', symbol, ') warrants attention ',
                                'and what action a UHNW wealth advisor should consider.\\n',
                                'Article: "', title, '" — published by ', site, ' on ', publishedDate, '\\n',
                                'Classification: ', _signal_type_raw:response[0]::STRING,
                                ' | materiality: ', _materiality_raw:response[0]::STRING,
                                ' | sentiment: ', sentiment_raw, '\\n',
                                'Sector: ', COALESCE(sector, 'Unknown'), ' / ', COALESCE(industry, 'Unknown'), '\\n',
                                'Be specific and direct. Do not start with "I" or repeat the labels.'
                            )
                        )) AS rationale
                    FROM signals
                )

                SELECT
                    md5(CONCAT(symbol, '|', url))                        AS signal_id,
                    symbol,
                    TRY_CAST(publishedDate AS DATE)                       AS signal_date,
                    'news'                                                AS source_type,
                    title                                                 AS source_description,
                    INITCAP(COALESCE(sentiment_raw, 'neutral'))           AS sentiment,
                    CASE materiality_val
                        WHEN 'High'   THEN 0.9
                        WHEN 'Medium' THEN 0.5
                        WHEN 'Low'    THEN 0.2
                        ELSE 0.3
                    END                                                   AS severity_score,
                    CASE
                        WHEN materiality_val = 'High'
                         AND signal_type_val IN ('Credit Event', 'M&A', 'Guidance', 'Management Change')
                        THEN true
                        ELSE false
                    END                                                   AS advisor_action_needed,
                    signal_type_val                                       AS signal_type,
                    signal_type_val                                       AS signal,
                    materiality_val                                       AS signal_value,
                    rationale,
                    CURRENT_TIMESTAMP()                                   AS processed_at
                FROM enriched

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
