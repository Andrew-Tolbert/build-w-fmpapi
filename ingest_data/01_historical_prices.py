# Databricks notebook source
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
        date             DATE,
        open             DOUBLE,
        high             DOUBLE,
        low              DOUBLE,
        close            DOUBLE,
        adjClose         DOUBLE,
        volume           LONG,
        unadjustedVolume LONG,
        change           DOUBLE,
        changePercent    DOUBLE,
        vwap             DOUBLE,
        label            STRING,
        changeOverTime   DOUBLE,
        symbol           STRING,
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
