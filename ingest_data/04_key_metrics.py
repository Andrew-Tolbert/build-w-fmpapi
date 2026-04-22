# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest key metrics and financial ratios JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/key_metrics/{TICKER}/{ts}_{key_metrics,financial_ratios}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_key_metrics
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_ratios

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_key_metrics", "bronze_financial_ratios"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_key_metrics (
        symbol              STRING,
        date                DATE,
        period              STRING,
        netDebtToEBITDA     DOUBLE,
        interestCoverage    DOUBLE,
        debtToEquity        DOUBLE,
        currentRatio        DOUBLE,
        quickRatio          DOUBLE,
        peRatio             DOUBLE,
        priceToBooksRatio   DOUBLE,
        earningsYield       DOUBLE,
        freeCashFlowPerShare DOUBLE,
        dividendYield       DOUBLE,
        enterpriseValue     DOUBLE,
        marketCap           DOUBLE,
        ingested_at         STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_financial_ratios (
        symbol                    STRING,
        date                      DATE,
        period                    STRING,
        netProfitMargin           DOUBLE,
        grossProfitMargin         DOUBLE,
        operatingProfitMargin     DOUBLE,
        returnOnAssets            DOUBLE,
        returnOnEquity            DOUBLE,
        debtRatio                 DOUBLE,
        interestCoverage          DOUBLE,
        enterpriseValueMultiple   DOUBLE,
        operatingCashFlowRatio    DOUBLE,
        ingested_at               STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/key_metrics"

for suffix, table in [
    ("key_metrics",      "bronze_key_metrics"),
    ("financial_ratios", "bronze_financial_ratios"),
]:
    path = f"{base}/*/*_{suffix}.json"
    target_schema = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{table}").schema
    df = (
        spark.read
            .option("multiline", "true")
            .schema(target_schema)
            .json(path)
            .withColumn("date", col("date").cast("date"))
    )
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

schema(target_schema)
