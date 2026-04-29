# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# News Signal Queries — Dashboard + Genie Use
#
# Four Lakeview dashboard queries and three Genie context queries sourced from
# silver_news_signals (AI-enriched) joined to holdings/accounts/clients for
# advisor-level scoping.
#
# Lakeview Parameters (named params for filter widgets):
#   :date.min     — period start  (date range widget)
#   :date.max     — period end    (date range widget)
#   :advisor_id   — optional; leave blank to include all advisors
#   :sector       — optional; leave blank to include all sectors
#
# ─────────────────────────────────────────────────────────────────────────────
# LAKEVIEW QUERIES
#   1. SENTIMENT TREND         — daily avg sentiment by sector, line chart
#   2. SIGNAL TYPE MIX         — article count by signal_type, bar chart
#   3. HIGH-MATERIALITY FEED   — table of High materiality articles, advisor-scoped
#   4. ADVISOR ACTION QUEUE    — table of articles requiring advisor action
#
# GENIE QUERIES
#   5. NEGATIVE NEWS FOR HOLDINGS  — natural language: "negative news for my BDC holdings"
#   6. M&A AND CREDIT EVENTS       — natural language: "M&A or credit event news this month"
#   7. SECTOR SENTIMENT SUMMARY    — natural language: "which sectors have the worst news?"
# ─────────────────────────────────────────────────────────────────────────────

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG ahtsa;
# MAGIC USE SCHEMA awm;

# COMMAND ----------

# DBTITLE 1,1. SENTIMENT TREND
# MAGIC %sql
# MAGIC -- Daily average sentiment score by sector over the selected date range.
# MAGIC -- Sentiment is encoded: positive=1, neutral/mixed=0, negative=-1.
# MAGIC -- Filtered to sectors represented in the advisor's current book.
# MAGIC --
# MAGIC -- Chart type: line chart
# MAGIC -- X axis: date | Y axis: avg_sentiment | Series: sector
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC -- ── Sentiment as numeric ─────────────────────────────────────────────────────
# MAGIC news_scored AS (
# MAGIC   SELECT
# MAGIC     TO_DATE(publishedDate) AS date,
# MAGIC     sector,
# MAGIC     CASE sentiment
# MAGIC       WHEN 'positive' THEN  1
# MAGIC       WHEN 'negative' THEN -1
# MAGIC       ELSE 0
# MAGIC     END AS sentiment_score
# MAGIC   FROM silver_news_signals
# MAGIC   WHERE TO_DATE(publishedDate) BETWEEN :date.min AND :date.max
# MAGIC     AND sector != 'Unknown'
# MAGIC     AND (sector = :sector OR :sector IS NULL)
# MAGIC ),
# MAGIC
# MAGIC -- ── Limit to sectors held by the filtered advisor's clients ──────────────────
# MAGIC held_sectors AS (
# MAGIC   SELECT DISTINCT cp.sector
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a  ON h.account_id = a.account_id
# MAGIC   JOIN clients  c  ON a.client_id  = c.client_id
# MAGIC   JOIN bronze_company_profiles cp ON h.ticker = cp.symbol
# MAGIC   WHERE (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC     AND cp.sector IS NOT NULL
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   n.date,
# MAGIC   n.sector,
# MAGIC   ROUND(AVG(n.sentiment_score), 3) AS avg_sentiment,
# MAGIC   COUNT(*)                          AS article_count
# MAGIC FROM news_scored n
# MAGIC JOIN held_sectors hs ON n.sector = hs.sector
# MAGIC GROUP BY n.date, n.sector
# MAGIC ORDER BY n.date, n.sector

# COMMAND ----------

# DBTITLE 1,2. SIGNAL TYPE MIX
# MAGIC %sql
# MAGIC -- Count of articles by signal type in the selected date window.
# MAGIC -- Scoped to tickers held by the filtered advisor's book.
# MAGIC --
# MAGIC -- Chart type: bar chart
# MAGIC -- X axis: signal_type | Y axis: article_count | Color: materiality
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   s.signal_type,
# MAGIC   s.materiality,
# MAGIC   COUNT(*) AS article_count
# MAGIC FROM silver_news_signals s
# MAGIC JOIN held_tickers ht ON s.symbol = ht.ticker
# MAGIC WHERE TO_DATE(s.publishedDate) BETWEEN :date.min AND :date.max
# MAGIC   AND (s.sector = :sector OR :sector IS NULL)
# MAGIC GROUP BY s.signal_type, s.materiality
# MAGIC ORDER BY article_count DESC

# COMMAND ----------

# DBTITLE 1,3. HIGH-MATERIALITY FEED
# MAGIC %sql
# MAGIC -- Table of High materiality articles for holdings in the advisor's book.
# MAGIC -- Shows company context alongside the headline so advisors can act immediately.
# MAGIC --
# MAGIC -- Chart type: table
# MAGIC -- Sort: publishedDate DESC
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC held_tickers AS (
# MAGIC   SELECT DISTINCT h.ticker
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   TO_DATE(s.publishedDate) AS date,
# MAGIC   s.symbol,
# MAGIC   s.companyName,
# MAGIC   s.sector,
# MAGIC   s.signal_type,
# MAGIC   s.sentiment,
# MAGIC   s.title,
# MAGIC   s.site,
# MAGIC   s.url
# MAGIC FROM silver_news_signals s
# MAGIC JOIN held_tickers ht ON s.symbol = ht.ticker
# MAGIC WHERE s.materiality = 'High'
# MAGIC   AND TO_DATE(s.publishedDate) BETWEEN :date.min AND :date.max
# MAGIC   AND (s.sector = :sector OR :sector IS NULL)
# MAGIC ORDER BY s.publishedDate DESC

# COMMAND ----------

# DBTITLE 1,4. ADVISOR ACTION QUEUE
# MAGIC %sql
# MAGIC -- Articles flagged for proactive advisor outreach:
# MAGIC -- High materiality + actionable signal type (Credit Event, M&A, Guidance, Management Change).
# MAGIC -- Joined to clients so the advisor sees exactly which clients hold the affected ticker.
# MAGIC --
# MAGIC -- Chart type: table
# MAGIC -- Sort: publishedDate DESC
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC -- One row per (article, client) so the advisor sees every affected client
# MAGIC affected_clients AS (
# MAGIC   SELECT
# MAGIC     h.ticker,
# MAGIC     c.client_id,
# MAGIC     c.client_name,
# MAGIC     c.advisor_id,
# MAGIC     SUM(h.market_value) AS position_value
# MAGIC   FROM holdings h
# MAGIC   JOIN accounts a ON h.account_id = a.account_id
# MAGIC   JOIN clients  c ON a.client_id  = c.client_id
# MAGIC   WHERE (array_contains(:advisor_id, c.advisor_id) OR :advisor_id IS NULL)
# MAGIC   GROUP BY h.ticker, c.client_id, c.client_name, c.advisor_id
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   TO_DATE(s.publishedDate) AS date,
# MAGIC   s.symbol,
# MAGIC   s.companyName,
# MAGIC   s.sector,
# MAGIC   s.signal_type,
# MAGIC   s.sentiment,
# MAGIC   ac.client_name,
# MAGIC   ROUND(ac.position_value, 0) AS position_value,
# MAGIC   s.title,
# MAGIC   s.site,
# MAGIC   s.url
# MAGIC FROM silver_news_signals s
# MAGIC JOIN affected_clients ac ON s.symbol = ac.ticker
# MAGIC WHERE s.advisor_action = true
# MAGIC   AND TO_DATE(s.publishedDate) BETWEEN :date.min AND :date.max
# MAGIC ORDER BY s.publishedDate DESC, ac.position_value DESC

# COMMAND ----------

# DBTITLE 1,5. GENIE — Negative news for BDC / private credit holdings
# MAGIC %sql
# MAGIC -- Genie context query: "Show me negative news for my BDC holdings"
# MAGIC -- Returns recent negative-sentiment articles for private credit tickers only.
# MAGIC
# MAGIC SELECT
# MAGIC   TO_DATE(s.publishedDate) AS date,
# MAGIC   s.symbol,
# MAGIC   s.companyName,
# MAGIC   s.signal_type,
# MAGIC   s.materiality,
# MAGIC   s.title,
# MAGIC   s.site
# MAGIC FROM silver_news_signals s
# MAGIC JOIN bronze_company_profiles cp ON s.symbol = cp.symbol
# MAGIC WHERE s.sentiment   = 'negative'
# MAGIC   AND cp.industry LIKE '%Business Development%'
# MAGIC   AND TO_DATE(s.publishedDate) >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC ORDER BY s.publishedDate DESC

# COMMAND ----------

# DBTITLE 1,6. GENIE — M&A and credit event news this month
# MAGIC %sql
# MAGIC -- Genie context query: "What M&A or credit event activity has been reported recently?"
# MAGIC -- Returns articles with signal_type M&A or Credit Event from the current month.
# MAGIC
# MAGIC SELECT
# MAGIC   TO_DATE(s.publishedDate) AS date,
# MAGIC   s.symbol,
# MAGIC   s.companyName,
# MAGIC   s.sector,
# MAGIC   s.signal_type,
# MAGIC   s.materiality,
# MAGIC   s.sentiment,
# MAGIC   s.title,
# MAGIC   s.site
# MAGIC FROM silver_news_signals s
# MAGIC WHERE s.signal_type IN ('M&A', 'Credit Event')
# MAGIC   AND TO_DATE(s.publishedDate) >= DATE_TRUNC('month', CURRENT_DATE())
# MAGIC ORDER BY s.materiality DESC, s.publishedDate DESC

# COMMAND ----------

# DBTITLE 1,7. GENIE — Sector sentiment summary
# MAGIC %sql
# MAGIC -- Genie context query: "Which sectors are getting the worst news coverage?"
# MAGIC -- Aggregates sentiment counts by sector over the last 30 days,
# MAGIC -- sorted by negative article share descending.
# MAGIC
# MAGIC SELECT
# MAGIC   sector,
# MAGIC   COUNT(*)                                                          AS total_articles,
# MAGIC   SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END)          AS positive,
# MAGIC   SUM(CASE WHEN sentiment = 'neutral'
# MAGIC            OR  sentiment = 'mixed'   THEN 1 ELSE 0 END)            AS neutral,
# MAGIC   SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)          AS negative,
# MAGIC   ROUND(
# MAGIC     SUM(CASE WHEN sentiment = 'negative' THEN 1 ELSE 0 END)
# MAGIC     / NULLIF(COUNT(*), 0) * 100, 1
# MAGIC   )                                                                 AS pct_negative
# MAGIC FROM silver_news_signals
# MAGIC WHERE sector != 'Unknown'
# MAGIC   AND TO_DATE(publishedDate) >= DATE_SUB(CURRENT_DATE(), 30)
# MAGIC GROUP BY sector
# MAGIC ORDER BY pct_negative DESC
