# Databricks notebook source
# COMMAND ----------

# Pull recent stock news for all watchlist tickers.
# Output: UC_VOLUME_PATH/stock_news/stock_news.json
# FMP Source: F15 — /stable/stock-news

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

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
news_df["ingested_at"] = pd.Timestamp.now().isoformat()
print(f"\nTotal articles: {len(news_df)}")

# COMMAND ----------

out_dir = volume_subdir("stock_news")
os.makedirs(out_dir, exist_ok=True)
out_path = f"{out_dir}/stock_news.json"

news_df.to_json(out_path, orient="records", indent=2)
print(f"Written {len(news_df)} articles to {out_path}")
