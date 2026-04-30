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

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports (
        _rescued_data STRING
    )
""")

# COMMAND ----------

from pyspark.sql import functions as F

checkpoint_path = f"{UC_VOLUME_PATH}/_checkpoints/bronze_financial_reports"
schema_path     = f"{UC_VOLUME_PATH}/_schemas/bronze_financial_reports"
source_path     = f"{UC_VOLUME_PATH}/financial_reports/*/*.json"

# Reset stale state from previous JSON-format run
spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports")
dbutils.fs.rm(checkpoint_path, recurse=True)
dbutils.fs.rm(schema_path, recurse=True)

raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .option("wholetext", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .load(source_path)
)

cleaned = (
    raw
    .withColumn("doc", F.parse_json(F.col("value")))
    .withColumn("symbol", F.regexp_extract(F.col("_metadata.file_path"), r"financial_reports/([^/]+)/", 1))
    .withColumn("year", F.regexp_extract(F.col("_metadata.file_path"), r"/(\d{4})_", 1).cast("int"))
    .withColumn("period", F.regexp_extract(F.col("_metadata.file_path"), r"_(\w+)\.json$", 1))
    .drop("value")
)

query = (
    cleaned.writeStream
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

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_financial_reports").orderBy("symbol", "year", "period"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ahtsa.awm.bronze_financial_reports
# MAGIC  
