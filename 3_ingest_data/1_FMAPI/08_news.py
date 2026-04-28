# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest stock news JSON files from UC Volume into a bronze Delta table using Autoloader.
# New articles are appended on each run; already-processed files are skipped via checkpoint.
# Source: UC_VOLUME_PATH/stock_news/{TICKER}/{date}_{hash}.json
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# # Uncomment to fully reset — drops the table and clears the Autoloader checkpoint
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news")
# dbutils.fs.rm(f"{UC_VOLUME_PATH}/_checkpoints/bronze_stock_news", recurse=True)

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news (
        symbol        STRING,
        url           STRING,
        publishedDate STRING,
        publisher     STRING,
        title         STRING,
        summary       STRING,
        full_text     STRING,
        site          STRING,
        sentiment     STRING,
        ingested_at   STRING
    )
    USING DELTA
""")

# COMMAND ----------

checkpoint_path = f"{UC_VOLUME_PATH}/_checkpoints/bronze_stock_news"
schema_path     = f"{UC_VOLUME_PATH}/_schemas/bronze_stock_news"
source_path     = f"{UC_VOLUME_PATH}/stock_news/*/*.json"

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
        .toTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news")
)

query.awaitTermination()
print(f"bronze_stock_news row count: {spark.table(f'{UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news').count()}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_stock_news").orderBy("symbol", "publishedDate"))
