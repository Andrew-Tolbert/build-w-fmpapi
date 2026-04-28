# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest historical price JSON files from UC Volume into a bronze Delta table.
# Source: UC_VOLUME_PATH/historical_prices/{TICKER}/{ts}_prices.json
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe the Delta table before re-running
# spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices (
        symbol           STRING,
        date             DATE,
        adjOpen          DOUBLE,
        adjHigh          DOUBLE,
        adjLow           DOUBLE,
        adjClose         DOUBLE,
        volume           LONG,
        ingested_at      STRING
    )
    USING DELTA
""")

# COMMAND ----------

wildcard_path = f"{UC_VOLUME_PATH}/historical_prices/*/*.json"

df = (
    spark.read
        .option("multiline", "true")
        .json(wildcard_path)
        .withColumn("date", col("date").cast("date"))
)

df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices")

print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_historical_prices").orderBy("symbol", "date"))
