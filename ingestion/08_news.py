# Databricks notebook source
# COMMAND ----------

# Ingest recent stock news for all watchlist tickers.
# Target: uc.wealth.stock_news
# FMP Source: F15 — /stable/stock-news

# COMMAND ----------

# MAGIC %pip install requests python-dotenv

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

NEWS_LIMIT = 50  # articles per ticker

news_frames = []
for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_stock_news(ticker, limit=NEWS_LIMIT)
        df = pd.DataFrame(rows)
        df["symbol"] = ticker
        news_frames.append(df)
        print(f"  {ticker}: {len(df)} articles")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

news_df = pd.concat(news_frames, ignore_index=True) if news_frames else pd.DataFrame()
print(f"\nTotal articles: {len(news_df)}")

# COMMAND ----------

target = uc_table("stock_news")
sdf = spark.createDataFrame(news_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
