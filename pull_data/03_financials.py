# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull income statements, balance sheets, and cash flow statements.
# Output: UC_VOLUME_PATH/financials/{TICKER}/{income_statements,balance_sheets,cash_flows}.json
# FMP Sources: F3/F4/F5

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# COMMAND ----------

out_base = volume_subdir("financials")

for ticker in EQUITY_TICKERS:
    try:
        income   = client.get_income_statement(ticker, period="quarterly", limit=12)
        balance  = client.get_balance_sheet(ticker, period="quarterly", limit=12)
        cashflow = client.get_cash_flow(ticker, period="quarterly", limit=12)

        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        ingested_at = pd.Timestamp.now().isoformat()

        for data, filename in [
            (income,   "income_statements"),
            (balance,  "balance_sheets"),
            (cashflow, "cash_flows"),
        ]:
            df = pd.DataFrame(data)
            df["symbol"] = ticker
            df["ingested_at"] = ingested_at
            df.to_json(f"{ticker_dir}/{filename}.json", orient="records", indent=2)

        print(f"  {ticker}: {len(income)} income, {len(balance)} balance, {len(cashflow)} cashflow rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")
