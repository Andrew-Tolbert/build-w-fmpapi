# Databricks notebook source
# COMMAND ----------

# Ingest key metrics and financial ratios for all watchlist tickers.
# These provide pre-computed covenant proxies: netDebtToEBITDA, interestCoverage.
# Targets: uc.wealth.key_metrics, uc.wealth.financial_ratios
# FMP Sources: F6/F7

# COMMAND ----------

# MAGIC %pip install requests

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

metrics_frames = []
ratios_frames  = []

for ticker in EQUITY_TICKERS:
    try:
        m = client.get_key_metrics(ticker, period="quarterly", limit=12)
        r = client.get_ratios(ticker, period="quarterly", limit=12)
        mdf = pd.DataFrame(m); mdf["symbol"] = ticker; metrics_frames.append(mdf)
        rdf = pd.DataFrame(r); rdf["symbol"] = ticker; ratios_frames.append(rdf)
        print(f"  {ticker}: {len(mdf)} metrics rows, {len(rdf)} ratio rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

metrics_combined = pd.concat(metrics_frames, ignore_index=True) if metrics_frames else pd.DataFrame()
ratios_combined  = pd.concat(ratios_frames,  ignore_index=True) if ratios_frames  else pd.DataFrame()

# COMMAND ----------

for df, table_name in [
    (metrics_combined, "key_metrics"),
    (ratios_combined,  "financial_ratios"),
]:
    target = uc_table(table_name)
    sdf = spark.createDataFrame(df).withColumn("ingested_at", current_timestamp())
    sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
    print(f"Written {sdf.count()} rows to {target}")
