# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest analyst estimates, price targets, and ratings JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/analyst_data/{TICKER}/{ts}_{analyst_estimates,price_targets,analyst_ratings}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_estimates
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_price_targets
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_ratings

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_analyst_estimates", "bronze_price_targets", "bronze_analyst_ratings"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_estimates (
        symbol                          STRING,
        date                            DATE,
        period                          STRING,
        estimatedRevenueAvg             DOUBLE,
        estimatedRevenueLow             DOUBLE,
        estimatedRevenueHigh            DOUBLE,
        estimatedEbitdaAvg              DOUBLE,
        estimatedEbitdaLow              DOUBLE,
        estimatedEbitdaHigh             DOUBLE,
        estimatedEpsAvg                 DOUBLE,
        estimatedEpsLow                 DOUBLE,
        estimatedEpsHigh                DOUBLE,
        numberAnalystEstimatedRevenue   LONG,
        ingested_at                     STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_price_targets (
        symbol          STRING,
        targetConsensus DOUBLE,
        targetMedian    DOUBLE,
        targetHigh      DOUBLE,
        targetLow       DOUBLE,
        analystCount    LONG,
        lastUpdate      STRING,
        ingested_at     STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_ratings (
        symbol     STRING,
        strongBuy  LONG,
        buy        LONG,
        hold       LONG,
        sell       LONG,
        strongSell LONG,
        consensus  STRING,
        ingested_at STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/analyst_data"

for suffix, table, date_col in [
    ("analyst_estimates", "bronze_analyst_estimates", True),
    ("price_targets",     "bronze_price_targets",     False),
    ("analyst_ratings",   "bronze_analyst_ratings",   False),
]:
    path = f"{base}/*/*_{suffix}.json"
    df = spark.read.option("multiline", "true").json(path)
    if date_col:
        df = df.withColumn("date", col("date").cast("date"))
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_estimates").orderBy("symbol", "date"))
