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
#
# Two-pass pipeline per article:
#
#   PASS 1 — Relevance check (runs on ALL unprocessed articles, one AI call each):
#     1. JOIN bronze_company_profiles + bronze_etf_info to resolve full entity name
#     2. Build relevance_input: "Security: [full name] (ticker: [SYMBOL]) ..." + article body
#     3. ai_classify(relevance_input, Relevant / Not Relevant)  → is_relevant
#     4. INSERT all articles into silver with is_relevant flag; signal columns left NULL
#
#   PASS 2 — Signal enrichment (runs ONLY on is_relevant = true articles, three AI calls each):
#     5. Read silver WHERE is_relevant = true AND signal_type IS NULL
#     6. Rebuild context_text with company/sector framing + article body
#     7. ai_analyze_sentiment(context_text)                    → sentiment
#     8. ai_classify(context_text, signal_type_labels)         → signal_type  (V2)
#     9. ai_classify(context_text, materiality_labels)         → materiality  (V2)
#    10. Derive advisor_action in SQL; MERGE signals back into silver
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals
# Run after: 3_ingest_data/1_FMAPI/08_news.py

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number (e.g. "10") to cap rows for testing

# COMMAND ----------

# # Uncomment to fully reset — drops the silver table so all articles are re-processed
#spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals")

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
        is_relevant    BOOLEAN,
        sentiment      STRING,
        signal_type    STRING,
        materiality    STRING,
        advisor_action BOOLEAN,
        processed_at   TIMESTAMP
    )
    USING DELTA
""")

# Add is_relevant column to any table created before this column was introduced
try:
    spark.sql(f"ALTER TABLE {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals ADD COLUMN is_relevant BOOLEAN")
    print("Added is_relevant column.")
except Exception:
    pass  # column already exists

# COMMAND ----------

# Build an optional LIMIT clause for testing — empty widget = no limit in production.
_test_limit = dbutils.widgets.get("test_limit").strip()
_limit_clause = f"LIMIT {_test_limit}" if _test_limit else ""

# Count articles in bronze not yet in silver — gates both passes.
unprocessed_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
        ON n.symbol = s.symbol AND n.url = s.url
    WHERE n.title IS NOT NULL
      AND LENGTH(TRIM(COALESCE(n.full_text, n.summary, n.title, ''))) > 0
""").collect()[0][0]

print(f"Unprocessed articles: {unprocessed_count}")

# COMMAND ----------

# ── PASS 1: Relevance check ────────────────────────────────────────────────────
# Inserts ALL unprocessed articles with is_relevant flag and NULL signal columns.
# This permanently marks every article as seen (via anti-join) regardless of relevance,
# so irrelevant articles are never re-evaluated on future runs.
#
# relevance_input explicitly names the security so the model knows exactly
# what to assess relevance for — full company/ETF name AND ticker symbol.

if unprocessed_count == 0:
    print("No new articles — silver_news_signals is up to date.")
else:
    spark.sql(f"""
        INSERT INTO {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals

        WITH unprocessed AS (
            SELECT n.*
            FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
            LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals s
                ON n.symbol = s.symbol AND n.url = s.url
            WHERE n.title IS NOT NULL
              AND LENGTH(TRIM(COALESCE(n.full_text, n.summary, n.title, ''))) > 0
            {_limit_clause}
        ),

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
                -- relevance_input: lead with the full entity name AND ticker so the model
                -- knows exactly which security to assess relevance for, then provide the article.
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
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
                ON u.symbol = cp.symbol
            LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info ei
                ON u.symbol = ei.symbol
        ),

        relevance AS (
            SELECT
                symbol, url, publishedDate, title, site,
                companyName, sector, industry,
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
            symbol,
            url,
            publishedDate,
            title,
            site,
            companyName,
            sector,
            industry,
            _rel_raw:response[0]::STRING = 'Relevant' AS is_relevant,
            NULL                                       AS sentiment,
            NULL                                       AS signal_type,
            NULL                                       AS materiality,
            false                                      AS advisor_action,
            CURRENT_TIMESTAMP()                        AS processed_at
        FROM relevance
    """)
    print(f"Pass 1 complete — inserted {unprocessed_count} articles with relevance flags.")

# COMMAND ----------

# ── PASS 2: Signal enrichment ──────────────────────────────────────────────────
# Runs only on articles where is_relevant = true AND signal_type IS NULL.
# Rebuilds context_text with company/sector framing, then calls the three AI functions.
# Results are merged back into silver — no new rows, only column updates.

relevant_count = spark.sql(f"""
    SELECT COUNT(*)
    FROM {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals
    WHERE is_relevant = true AND signal_type IS NULL
""").collect()[0][0]

print(f"Relevant articles awaiting signal enrichment: {relevant_count}")

# COMMAND ----------

if relevant_count == 0:
    print("No relevant articles to enrich.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals AS tgt
        USING (

            WITH to_enrich AS (
                SELECT symbol, url
                FROM {UC_CATALOG}.{UC_SCHEMA}.silver_news_signals
                WHERE is_relevant = true AND signal_type IS NULL
            ),

            bronze AS (
                SELECT n.symbol, n.url, n.title, n.full_text, n.summary
                FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news n
                JOIN to_enrich e ON n.symbol = e.symbol AND n.url = e.url
            ),

            context AS (
                SELECT
                    b.symbol,
                    b.url,
                    CONCAT(
                        COALESCE(cp.companyName, b.symbol),
                        ' (', b.symbol,
                        ', ', COALESCE(cp.sector,   'Unknown'),
                        ', ', COALESCE(cp.industry, 'Unknown'),
                        CASE WHEN ei.assetClass IS NOT NULL
                             THEN ', ' || ei.assetClass ELSE '' END,
                        '): ',
                        b.title,
                        '. ',
                        LEFT(COALESCE(b.full_text, b.summary, ''), 8000)
                    ) AS context_text
                FROM bronze b
                LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles cp
                    ON b.symbol = cp.symbol
                LEFT JOIN {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info ei
                    ON b.symbol = ei.symbol
            ),

            signals AS (
                SELECT
                    symbol,
                    url,
                    ai_analyze_sentiment(context_text) AS sentiment,
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
                    ) AS _materiality_raw
                FROM context
            )

            SELECT
                symbol,
                url,
                sentiment,
                _signal_type_raw:response[0]::STRING AS signal_type,
                _materiality_raw:response[0]::STRING AS materiality,
                CASE
                    WHEN _materiality_raw:response[0]::STRING = 'High'
                     AND _signal_type_raw:response[0]::STRING
                         IN ('Credit Event', 'M&A', 'Guidance', 'Management Change')
                    THEN true
                    ELSE false
                END AS advisor_action
            FROM signals

        ) AS src
        ON tgt.symbol = src.symbol AND tgt.url = src.url
        WHEN MATCHED THEN UPDATE SET
            tgt.sentiment      = src.sentiment,
            tgt.signal_type    = src.signal_type,
            tgt.materiality    = src.materiality,
            tgt.advisor_action = src.advisor_action
    """)
    print(f"Pass 2 complete — enriched {relevant_count} relevant articles.")

# COMMAND ----------

display(
    spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.silver_news_signals")
        .orderBy("processed_at", ascending=False).filter("is_relevant = True")
)

# COMMAND ----------


