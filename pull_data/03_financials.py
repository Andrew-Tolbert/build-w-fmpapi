# Databricks notebook source
# COMMAND ----------

# Pull income statements, balance sheets, and cash flow statements.
# Output: UC_VOLUME_PATH/financials/{income_statements,balance_sheets,cash_flows}.json
# FMP Sources: F3/F4/F5

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd

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

out_dir = volume_subdir("financials")
os.makedirs(out_dir, exist_ok=True)

for df, filename in [
    (income_df,   "income_statements"),
    (balance_df,  "balance_sheets"),
    (cashflow_df, "cash_flows"),
]:
    df["ingested_at"] = pd.Timestamp.now().isoformat()
    out_path = f"{out_dir}/{filename}.json"
    df.to_json(out_path, orient="records", indent=2)
    print(f"Written {len(df)} rows to {out_path}")
