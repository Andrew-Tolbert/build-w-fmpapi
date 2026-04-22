# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest earnings call transcript JSON files from UC Volume into a bronze Delta table using Autoloader.
# New transcripts are appended on each run; already-processed files are skipped via checkpoint.
# Source: UC_VOLUME_PATH/transcripts/{TICKER}/Q{q}_{year}.json
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# # Uncomment to fully reset — drops the table and clears the Autoloader checkpoint
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts")
# dbutils.fs.rm(f"{UC_VOLUME_PATH}/_checkpoints/bronze_transcripts", recurse=True)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts (
        symbol      STRING,
        year        INT,
        quarter     INT,
        date        STRING,
        title       STRING,
        content     STRING
    )
    USING DELTA
""")

# COMMAND ----------

checkpoint_path = f"{UC_VOLUME_PATH}/_checkpoints/bronze_transcripts"
source_path     = f"{UC_VOLUME_PATH}/transcripts/*/*.json"

query = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("multiLine", "true")
        .load(source_path)
        .writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts")
)

query.awaitTermination()
print(f"bronze_transcripts row count: {spark.table(f'{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts').count()}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts").orderBy("symbol", "year", "quarter"))
