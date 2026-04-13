# Databricks notebook source
# COMMAND ----------

# Ingest income statements, balance sheets, and cash flow statements.
# Targets: uc.wealth.income_statements, uc.wealth.balance_sheets, uc.wealth.cash_flows
# FMP Sources: F3/F4/F5

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

def fetch_for_all(fetch_fn, tickers, label):
    """Call fetch_fn(ticker) for each ticker, concatenate results."""
    frames = []
    for ticker in tickers:
        try:
            rows = fetch_fn(ticker)
            df = pd.DataFrame(rows)
            df["symbol"] = ticker
            frames.append(df)
            print(f"  {ticker}: {len(df)} rows")
        except Exception as e:
            print(f"  {ticker}: ERROR — {e}")
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"{label}: {len(result)} total rows")
    return result

# COMMAND ----------

print("=== Income Statements ===")
income_df = fetch_for_all(
    lambda t: client.get_income_statement(t, period="quarterly", limit=12),
    EQUITY_TICKERS, "Income statements"
)

# COMMAND ----------

print("\n=== Balance Sheets ===")
balance_df = fetch_for_all(
    lambda t: client.get_balance_sheet(t, period="quarterly", limit=12),
    EQUITY_TICKERS, "Balance sheets"
)

# COMMAND ----------

print("\n=== Cash Flow Statements ===")
cashflow_df = fetch_for_all(
    lambda t: client.get_cash_flow(t, period="quarterly", limit=12),
    EQUITY_TICKERS, "Cash flows"
)

# COMMAND ----------

for df, table_name in [
    (income_df,  "income_statements"),
    (balance_df, "balance_sheets"),
    (cashflow_df, "cash_flows"),
]:
    target = uc_table(table_name)
    sdf = spark.createDataFrame(df).withColumn("ingested_at", current_timestamp())
    sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
    print(f"Written {sdf.count()} rows to {target}")
