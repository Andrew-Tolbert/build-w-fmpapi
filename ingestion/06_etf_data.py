# Databricks notebook source
# COMMAND ----------

# Ingest ETF information, holdings, and sector weightings.
# Targets: uc.wealth.etf_info, uc.wealth.etf_holdings, uc.wealth.etf_sectors
# FMP Sources: F9/F10/F11

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

info_frames    = []
holdings_frames = []
sector_frames  = []

for etf in ETF_TICKERS:
    print(f"Fetching ETF data for {etf}...")
    try:
        # ETF info
        info = client.get_etf_info(etf)
        idf = pd.DataFrame([info] if isinstance(info, dict) else info)
        idf["symbol"] = etf
        info_frames.append(idf)

        # ETF holdings
        holdings = client.get_etf_holdings(etf)
        hdf = pd.DataFrame(holdings)
        hdf["etf_symbol"] = etf
        holdings_frames.append(hdf)
        print(f"  {etf}: {len(hdf)} holdings")

        # Sector weightings
        sectors = client.get_etf_sector_weightings(etf)
        sdf = pd.DataFrame(sectors)
        sdf["etf_symbol"] = etf
        sector_frames.append(sdf)
        print(f"  {etf}: {len(sdf)} sector buckets")

    except Exception as e:
        print(f"  {etf}: ERROR — {e}")

# COMMAND ----------

for df, table_name in [
    (pd.concat(info_frames,     ignore_index=True) if info_frames     else pd.DataFrame(), "etf_info"),
    (pd.concat(holdings_frames, ignore_index=True) if holdings_frames else pd.DataFrame(), "etf_holdings"),
    (pd.concat(sector_frames,   ignore_index=True) if sector_frames   else pd.DataFrame(), "etf_sectors"),
]:
    target = uc_table(table_name)
    sdf = spark.createDataFrame(df).withColumn("ingested_at", current_timestamp())
    sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
    print(f"Written {sdf.count()} rows to {target}")
