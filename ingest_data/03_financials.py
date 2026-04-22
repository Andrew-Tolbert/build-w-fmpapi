# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Ingest financial statement JSON files from UC Volume into bronze Delta tables.
# Source: UC_VOLUME_PATH/financials/{TICKER}/{ts}_{type}.json
# Outputs:
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_sheets
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_cash_flows
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_income_growth
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_growth
#   {UC_CATALOG}.{UC_SCHEMA}.bronze_cashflow_growth

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# # Uncomment to wipe tables before re-running
# for t in ["bronze_income_statements","bronze_balance_sheets","bronze_cash_flows",
#           "bronze_income_growth","bronze_balance_growth","bronze_cashflow_growth"]:
#     spark.sql(f"DROP TABLE IF EXISTS {UC_CATALOG}.{UC_SCHEMA}.{t}")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements (
        symbol              STRING,
        date                DATE,
        period              STRING,
        revenue             DOUBLE,
        ebitda              DOUBLE,
        operatingIncome     DOUBLE,
        netIncome           DOUBLE,
        eps                 DOUBLE,
        epsDiluted          DOUBLE,
        interestExpense     DOUBLE,
        grossProfit         DOUBLE,
        grossProfitRatio    DOUBLE,
        costOfRevenue       DOUBLE,
        operatingExpenses   DOUBLE,
        ingested_at         STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_balance_sheets (
        symbol                      STRING,
        date                        DATE,
        period                      STRING,
        totalAssets                 DOUBLE,
        totalLiabilities            DOUBLE,
        totalStockholdersEquity     DOUBLE,
        cashAndCashEquivalents      DOUBLE,
        totalDebt                   DOUBLE,
        longTermDebt                DOUBLE,
        shortTermDebt               DOUBLE,
        netDebt                     DOUBLE,
        goodwillAndIntangibleAssets DOUBLE,
        retainedEarnings            DOUBLE,
        ingested_at                 STRING
    )
    USING DELTA
""")

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {UC_CATALOG}.{UC_SCHEMA}.bronze_cash_flows (
        symbol                      STRING,
        date                        DATE,
        period                      STRING,
        operatingCashFlow           DOUBLE,
        capitalExpenditure          DOUBLE,
        freeCashFlow                DOUBLE,
        depreciationAndAmortization DOUBLE,
        dividendsPaid               DOUBLE,
        ingested_at                 STRING
    )
    USING DELTA
""")

# COMMAND ----------

base = f"{UC_VOLUME_PATH}/financials"

for suffix, table in [
    ("income_statements", "bronze_income_statements"),
    ("balance_sheets",    "bronze_balance_sheets"),
    ("cash_flows",        "bronze_cash_flows"),
    ("income_growth",     "bronze_income_growth"),
    ("balance_growth",    "bronze_balance_growth"),
    ("cashflow_growth",   "bronze_cashflow_growth"),
]:
    path = f"{base}/*/*_{suffix}.json"
    df = (
        spark.read
            .option("multiline", "true")
            .json(path)
            .withColumn("date", col("date").cast("date"))
    )
    df.write.mode("overwrite").saveAsTable(f"{UC_CATALOG}.{UC_SCHEMA}.{table}")
    print(f"Loaded {df.count()} rows into {UC_CATALOG}.{UC_SCHEMA}.{table}")

# COMMAND ----------

display(spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_income_statements").orderBy("symbol", "date"))
