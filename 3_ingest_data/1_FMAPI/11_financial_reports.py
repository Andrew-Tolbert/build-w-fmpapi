# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest structured financial report JSON files from UC Volume into a bronze Delta table using Autoloader.
# Each file is a complete FMP financial report document (one per ticker x fiscal period).
# Section names (e.g. "Cover Page", "CONDENSED CONSOLIDATED STATEMEN") are dynamic keys with
# array values — schema is inferred rather than declared.
# New reports are appended on each run; already-processed files are skipped via checkpoint.
# Source: UC_VOLUME_PATH/financial_reports/{TICKER}/{year}_{period}.json
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# # Uncomment to fully reset — drops the table and clears the Autoloader checkpoint/schema
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports")
# dbutils.fs.rm(f"{UC_VOLUME_PATH}/_checkpoints/bronze_financial_reports", recurse=True)
# dbutils.fs.rm(f"{UC_VOLUME_PATH}/_schemas/bronze_financial_reports", recurse=True)

# COMMAND ----------

checkpoint_path = f"{UC_VOLUME_PATH}/_checkpoints/bronze_financial_reports"
schema_path     = f"{UC_VOLUME_PATH}/_schemas/bronze_financial_reports"
source_path     = f"{UC_VOLUME_PATH}/financial_reports/*/*.json"

query = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("multiLine", "true")
        .load(source_path)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports")
)

query.awaitTermination()
print(f"bronze_financial_reports row count: {spark.table(f'{UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports').count()}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports").select("symbol", "year", "period").distinct().orderBy("symbol", "year", "period"))
