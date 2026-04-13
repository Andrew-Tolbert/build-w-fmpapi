# Databricks notebook source
# COMMAND ----------

# Ingest analyst estimates, price target consensus, and analyst ratings.
# Targets: uc.wealth.analyst_estimates, uc.wealth.price_targets, uc.wealth.analyst_ratings
# FMP Sources: F12/F13/F14

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

estimates_frames = []
targets_frames   = []
ratings_frames   = []

for ticker in EQUITY_TICKERS:
    try:
        est = client.get_analyst_estimates(ticker, limit=8)
        edf = pd.DataFrame(est); edf["symbol"] = ticker; estimates_frames.append(edf)

        pt = client.get_price_target_consensus(ticker)
        tdf = pd.DataFrame([pt] if isinstance(pt, dict) else pt)
        tdf["symbol"] = ticker; targets_frames.append(tdf)

        gr = client.get_grades_summary(ticker)
        gdf = pd.DataFrame([gr] if isinstance(gr, dict) else gr)
        gdf["symbol"] = ticker; ratings_frames.append(gdf)

        print(f"  {ticker}: {len(edf)} estimate rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

# COMMAND ----------

for df, table_name in [
    (pd.concat(estimates_frames, ignore_index=True) if estimates_frames else pd.DataFrame(), "analyst_estimates"),
    (pd.concat(targets_frames,   ignore_index=True) if targets_frames   else pd.DataFrame(), "price_targets"),
    (pd.concat(ratings_frames,   ignore_index=True) if ratings_frames   else pd.DataFrame(), "analyst_ratings"),
]:
    target = uc_table(table_name)
    sdf = spark.createDataFrame(df).withColumn("ingested_at", current_timestamp())
    sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
    print(f"Written {sdf.count()} rows to {target}")
