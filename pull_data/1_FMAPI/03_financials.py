# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Pull income statements, balance sheets, cash flow statements, and their YoY growth rates.
# Output: UC_VOLUME_PATH/financials/{TICKER}/{ts}_{income_statements,balance_sheets,cash_flows,income_growth,balance_growth,cashflow_growth}.json
# FMP Sources: F3/F4/F5 + growth endpoints

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

client = FMPClient(api_key=FMP_API_KEY)

# Uncomment to wipe all data for this feed before re-ingesting:
clear_directory(volume_subdir("financials"))

# COMMAND ----------

out_base = volume_subdir("financials")
_ts = ts_prefix()

for ticker in EQUITY_TICKERS:
    try:
        income          = client.get_income_statement(ticker, period="quarterly", limit=24)
        balance         = client.get_balance_sheet(ticker, period="quarterly", limit=24)
        cashflow        = client.get_cash_flow(ticker, period="quarterly", limit=24)
        income_growth   = client.get_income_statement_growth(ticker, period="quarterly", limit=24)
        balance_growth  = client.get_balance_sheet_growth(ticker, period="quarterly", limit=24)
        cashflow_growth = client.get_cash_flow_growth(ticker, period="quarterly", limit=24)

        ticker_dir = f"{out_base}/{ticker}"
        os.makedirs(ticker_dir, exist_ok=True)
        ingested_at = pd.Timestamp.now().isoformat()

        for data, filename in [
            (income,          "income_statements"),
            (balance,         "balance_sheets"),
            (cashflow,        "cash_flows"),
            (income_growth,   "income_growth"),
            (balance_growth,  "balance_growth"),
            (cashflow_growth, "cashflow_growth"),
        ]:
            df = pd.DataFrame(data)
            df["symbol"] = ticker
            df["ingested_at"] = ingested_at
            df.to_json(f"{ticker_dir}/{_ts}_{filename}.json", orient="records", indent=2)

        print(f"  {ticker}: {len(income)} income, {len(balance)} balance, {len(cashflow)} cashflow, "
              f"{len(income_growth)} income_growth, {len(balance_growth)} balance_growth, {len(cashflow_growth)} cashflow_growth rows")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

# COMMAND ----------

display(dbutils.fs.ls(f"{out_base}/{ticker}"))
