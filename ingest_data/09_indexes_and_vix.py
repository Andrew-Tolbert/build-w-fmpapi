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

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe the Delta table before re-running
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix (
        symbol        STRING,
        date          DATE,
        open          DOUBLE,
        high          DOUBLE,
        low           DOUBLE,
        close         DOUBLE,
        volume        LONG,
        change        DOUBLE,
        changePercent DOUBLE,
        ingested_at   STRING
    )
    USING DELTA
""")

# COMMAND ----------

wildcard_path = f"{UC_VOLUME_PATH}/indexes/*/*_history.json"

df = (
    spark.read
        .option("multiline", "true")
        .json(wildcard_path)
        .withColumn("date", col("date").cast("date"))
)

df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_indexes_and_vix").orderBy("symbol", "date"))
