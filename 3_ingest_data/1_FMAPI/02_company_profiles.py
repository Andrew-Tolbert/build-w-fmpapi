# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest company profile JSON files from UC Volume into a bronze Delta table.
# Source: UC_VOLUME_PATH/company_profiles/{TICKER}/{ts}_profile.json
# Output: {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# # Uncomment to wipe the Delta table before re-running
#spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles (
        symbol              STRING,
        price               DOUBLE,
        marketCap           LONG,
        beta                DOUBLE,
        lastDividend        DOUBLE,
        range               STRING,
        change              DOUBLE,
        changePercentage    DOUBLE,
        volume              DOUBLE,
        averageVolume       LONG,
        companyName         STRING,
        currency            STRING,
        cik                 STRING,
        isin                STRING,
        cusip               STRING,
        exchangeFullName    STRING,
        exchange            STRING,
        industry            STRING,
        website             STRING,
        description         STRING,
        ceo                 STRING,
        sector              STRING,
        country             STRING,
        fullTimeEmployees   STRING,
        phone               STRING,
        address             STRING,
        city                STRING,
        state               STRING,
        zip                 STRING,
        image               STRING,
        ipoDate             STRING,
        defaultImage        BOOLEAN,
        isEtf               BOOLEAN,
        isActivelyTrading   BOOLEAN,
        isAdr               BOOLEAN,
        isFund              BOOLEAN,
        ingested_at         STRING
    )
    USING DELTA
""")

# COMMAND ----------

wildcard_path = f"{UC_VOLUME_PATH}/company_profiles/*/*.json"

table = 'bronze_company_profiles'

target_schema = spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.{table}").schema

df = spark.read.option("multiline", "true").schema(target_schema).json(wildcard_path)

df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles")

print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_company_profiles").orderBy("symbol"))
