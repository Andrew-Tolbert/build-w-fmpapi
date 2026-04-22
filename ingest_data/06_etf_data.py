# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest ETF info, holdings, and sector weighting JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/etf_data/{TICKER}/{ts}_{etf_info,etf_holdings,etf_sectors}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_holdings
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_sectors

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_etf_info", "bronze_etf_holdings", "bronze_etf_sectors"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_info (
        symbol          STRING,
        name            STRING,
        description     STRING,
        aum             DOUBLE,
        expenseRatio    DOUBLE,
        ytdReturn       DOUBLE,
        oneYearReturn   DOUBLE,
        threeYearReturn DOUBLE,
        fiveYearReturn  DOUBLE,
        holdingsCount   LONG,
        ingested_at     STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_holdings (
        etf_symbol       STRING,
        asset            STRING,
        name             STRING,
        isin             STRING,
        cusip            STRING,
        weightPercentage DOUBLE,
        marketValue      DOUBLE,
        exchange         STRING,
        ingested_at      STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_etf_sectors (
        etf_symbol       STRING,
        sector           STRING,
        weightPercentage DOUBLE,
        ingested_at      STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/etf_data"

for suffix, table in [
    ("etf_info",     "bronze_etf_info"),
    ("etf_holdings", "bronze_etf_holdings"),
    ("etf_sectors",  "bronze_etf_sectors"),
]:
    path = f"{base}/*/*_{suffix}.json"
    df = spark.read.option("multiline", "true").json(path)
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_etf_holdings").orderBy("etf_symbol", "weightPercentage").limit(100))
