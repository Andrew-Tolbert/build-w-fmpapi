# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest index and VIX historical price JSON files from UC Volume into a bronze Delta table.
# Source: UC_VOLUME_PATH/indexes/{SYMBOL}/{ts}_history.json
# Symbols: GSPC (^GSPC), DJI (^DJI), IXIC (^IXIC), VIX (^VIX)
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# # Uncomment to wipe the Delta table before re-running
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix (
        symbol        STRING,
        index         STRING,
        date          DATE,
        open          DOUBLE,
        high          DOUBLE,
        low           DOUBLE,
        close         DOUBLE,
        volume        LONG,
        change        DOUBLE,
        changePercent DOUBLE,
        vwap          DOUBLE,
        ingested_at   STRING
    )
    USING DELTA
""")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, when, col

wildcard_path   = f"{UC_VOLUME_PATH}/indexes/*/*_history.json"
target_schema   = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix").schema

df = spark.read.option("multiline", "true").schema(target_schema).json(wildcard_path)

# Remove ^ from symbol and add Index column
df = df.withColumn(
    "symbol", regexp_replace(col("symbol"), r"^\^", "")
).withColumn(
    "index",
    when(col("symbol") == "DJI", "Dow Jones Industrial Average")
    .when(col("symbol") == "IXIC", "Nasdaq Composite")
    .when(col("symbol") == "GSPC", "S&P 500")
    .when(col("symbol") == "VIX", "VIX")
    .otherwise(None)
)

df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix").orderBy("symbol", "date"))
