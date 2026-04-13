# Databricks notebook source
# COMMAND ----------

# Ingest historical daily price data for all watchlist tickers.
# Target: uc.wealth.historical_prices
# FMP Source: F2 — /stable/historical-price-eod/{symbol}

# COMMAND ----------

# MAGIC %pip install requests python-dotenv

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

spark = SparkSession.builder.getOrCreate()
client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

# Parameters — override with Databricks widgets if needed
from_date = HISTORY_START_DATE
to_date   = pd.Timestamp.today().strftime("%Y-%m-%d")

print(f"Date range: {from_date} → {to_date}")
print(f"Fetching price history for {len(EQUITY_TICKERS)} tickers...")

# COMMAND ----------

all_frames = []
for ticker in EQUITY_TICKERS:
    try:
        rows = client.get_historical_prices(ticker, from_date, to_date)
        df = pd.DataFrame(rows)
        df["symbol"] = ticker
        all_frames.append(df)
        print(f"  {ticker}: {len(df)} rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

combined = pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame()
print(f"\nTotal rows: {len(combined)}")

# COMMAND ----------

target = uc_table("historical_prices")

sdf = spark.createDataFrame(combined).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)

print(f"Written {sdf.count()} rows to {target}")
