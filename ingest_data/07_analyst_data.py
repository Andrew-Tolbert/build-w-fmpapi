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

# # Uncomment to wipe tables before re-running
# for t in ["bronze_analyst_estimates", "bronze_price_targets", "bronze_analyst_ratings"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_estimates (
        symbol             STRING,
        date               DATE,
        revenueLow         DOUBLE,
        revenueHigh        DOUBLE,
        revenueAvg         DOUBLE,
        ebitdaLow          DOUBLE,
        ebitdaHigh         DOUBLE,
        ebitdaAvg          DOUBLE,
        ebitLow            DOUBLE,
        ebitHigh           DOUBLE,
        ebitAvg            DOUBLE,
        netIncomeLow       DOUBLE,
        netIncomeHigh      DOUBLE,
        netIncomeAvg       DOUBLE,
        sgaExpenseLow      DOUBLE,
        sgaExpenseHigh     DOUBLE,
        sgaExpenseAvg      DOUBLE,
        epsAvg             DOUBLE,
        epsHigh            DOUBLE,
        epsLow             DOUBLE,
        numAnalystsRevenue LONG,
        numAnalystsEps     LONG,
        ingested_at        STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_price_targets (
        symbol          STRING,
        targetHigh      DOUBLE,
        targetLow       DOUBLE,
        targetConsensus DOUBLE,
        targetMedian    DOUBLE,
        ingested_at     STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_ratings (
        symbol      STRING,
        strongBuy   LONG,
        buy         LONG,
        hold        LONG,
        sell        LONG,
        strongSell  LONG,
        consensus   STRING,
        ingested_at STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/analyst_data"

for suffix, table in [
    ("analyst_estimates", "bronze_analyst_estimates"),
    ("price_targets",     "bronze_price_targets"),
    ("analyst_ratings",   "bronze_analyst_ratings"),
]:
    path = f"{base}/*/*_{suffix}.json"
    target_schema = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{table}").schema
    df = spark.read.option("multiline", "true").schema(target_schema).json(path)
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_analyst_estimates").orderBy("symbol", "date"))
