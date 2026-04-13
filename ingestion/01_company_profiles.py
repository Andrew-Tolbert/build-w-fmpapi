# Databricks notebook source
# COMMAND ----------

# Ingest FMP company profiles for all tickers in the watchlist.
# Target: uc.wealth.company_profiles
# FMP Source: F1 — /stable/profile

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

# Fetch profiles for all tickers (batched in a single request)
print(f"Fetching profiles for {len(EQUITY_TICKERS)} tickers...")
raw = client.get_profiles(EQUITY_TICKERS)

df = pd.DataFrame(raw) if isinstance(raw, list) else pd.DataFrame([raw])
print(f"  Received {len(df)} records")

# COMMAND ----------

# Write to Unity Catalog
target = uc_table("company_profiles")

sdf = spark.createDataFrame(df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)

print(f"Written {sdf.count()} rows to {target}")
