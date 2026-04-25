# Databricks notebook source
# Validate and rebuild holdings from the transactions ledger.
#
# Transactions are the authoritative source of record. This notebook:
#   1. Reconstructs quantity and weighted-average cost basis from BUY transactions.
#   2. Compares those values against the current holdings table and reports discrepancies.
#   3. Overwrites holdings with the transaction-derived values, priced at the latest
#      date in bronze_historical_prices.
#
# Asset class is preserved from the existing holdings table — it is not derivable
# from transactions alone.
# DIVIDEND and FEE transactions do not affect position quantity or cost basis.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import datetime
import pandas as pd
from pyspark.sql.functions import current_timestamp

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# ── Load source data ───────────────────────────────────────────────────────────

txns_df     = spark.table(uc_table("transactions")).toPandas()
holdings_df = spark.table(uc_table("holdings")).toPandas()

prices_raw = spark.table(uc_table("bronze_historical_prices")) \
    .select("symbol", "date", "adjClose") \
    .toPandas()
prices_raw["date"] = pd.to_datetime(prices_raw["date"])

max_price_date = prices_raw["date"].max().date()
latest_prices  = (
    prices_raw[prices_raw["date"] == prices_raw["date"].max()]
    .set_index("symbol")["adjClose"]
    .to_dict()
)

print(f"Transactions: {len(txns_df):,} rows")
print(f"Holdings:     {len(holdings_df):,} rows")
print(f"Price date:   {max_price_date}")

# COMMAND ----------

# ── Reconstruct positions from BUY transactions ────────────────────────────────
# Net quantity  = SUM(quantity) for BUYs per (account_id, ticker)
# Weighted cost = SUM(gross_amount) / SUM(quantity) for BUYs — true average cost

buys = txns_df[txns_df["action"] == "BUY"].copy()

txn_positions = (
    buys.groupby(["account_id", "ticker"])
    .agg(
        txn_quantity  = ("quantity",     "sum"),
        txn_gross     = ("gross_amount", "sum"),
    )
    .reset_index()
)
txn_positions["txn_cost_basis_per_share"] = (
    txn_positions["txn_gross"] / txn_positions["txn_quantity"]
)

print(f"Distinct positions in transactions: {len(txn_positions):,}")

# COMMAND ----------

# ── Compare against current holdings ──────────────────────────────────────────

# Exclude CASH from the comparison — it has no BUY transaction
holdings_ex_cash = holdings_df[holdings_df["ticker"] != "CASH"].copy()

merged = holdings_ex_cash.merge(
    txn_positions,
    on=["account_id", "ticker"],
    how="outer",
    indicator=True,
)

in_holdings_only = merged[merged["_merge"] == "left_only"]
in_txns_only     = merged[merged["_merge"] == "right_only"]
in_both          = merged[merged["_merge"] == "both"].copy()

print(f"\n── Coverage ──────────────────────────────────────────────────────────")
print(f"  Positions in both:          {len(in_both):,}")
print(f"  In holdings but no BUYs:    {len(in_holdings_only):,}  ← should be 0")
print(f"  In BUYs but not holdings:   {len(in_txns_only):,}  ← should be 0")

if len(in_holdings_only) > 0:
    print("\n  Holdings with no BUY transactions:")
    print(in_holdings_only[["account_id", "ticker", "quantity"]].to_string(index=False))

if len(in_txns_only) > 0:
    print("\n  BUY transactions with no holdings row:")
    print(in_txns_only[["account_id", "ticker", "txn_quantity"]].to_string(index=False))

# COMMAND ----------

# ── Quantity and cost-basis discrepancies ──────────────────────────────────────

in_both["qty_diff"]  = (in_both["txn_quantity"] - in_both["quantity"]).abs()
in_both["cost_diff"] = (in_both["txn_cost_basis_per_share"] - in_both["cost_basis_per_share"]).abs()

qty_mismatches  = in_both[in_both["qty_diff"]  > 0.01]
cost_mismatches = in_both[in_both["cost_diff"] > 0.01]

print(f"\n── Discrepancies ─────────────────────────────────────────────────────")
print(f"  Quantity mismatches  (>0.01 shares):  {len(qty_mismatches):,}")
print(f"  Cost basis mismatches (>$0.01/share): {len(cost_mismatches):,}")
print(f"  (Cost basis differences are expected — holdings used inception-date")
print(f"   price; transactions use tranche-weighted average.)")

if len(qty_mismatches) > 0:
    print("\n  Sample quantity mismatches:")
    print(qty_mismatches[["account_id", "ticker", "quantity", "txn_quantity", "qty_diff"]]
          .head(10).to_string(index=False))

# COMMAND ----------

# ── Rebuild holdings from transactions ────────────────────────────────────────
# Use transaction-derived quantity and weighted-average cost as the ground truth.
# Asset class is carried over from existing holdings (not in transactions).
# CASH row is rebuilt from account_aum minus all non-cash market values.

asset_class_ref = holdings_df[holdings_df["ticker"] != "CASH"] \
    .set_index(["account_id", "ticker"])["asset_class"].to_dict()

accounts_df = spark.table(uc_table("accounts")).toPandas() \
    .set_index("account_id")["account_aum"].to_dict()

rebuilt_rows = []

for _, pos in txn_positions.iterrows():
    account_id = pos["account_id"]
    ticker     = pos["ticker"]
    qty        = pos["txn_quantity"]
    cb_per_sh  = pos["txn_cost_basis_per_share"]
    price      = latest_prices.get(ticker, None)

    if price is None:
        print(f"WARNING: no current price for {ticker} — skipping")
        continue

    asset_class = asset_class_ref.get((account_id, ticker), "Equity")

    rebuilt_rows.append({
        "date":                 max_price_date,
        "account_id":           account_id,
        "ticker":               ticker,
        "asset_class":          asset_class,
        "quantity":             qty,
        "price":                price,
        "market_value":         qty * price,
        "cost_basis_per_share": cb_per_sh,
        "total_cost_basis":     qty * cb_per_sh,
        "unrealized_gl":        qty * (price - cb_per_sh),
    })

rebuilt_df = pd.DataFrame(rebuilt_rows)

# Rebuild CASH row per account: account_aum minus sum of non-cash market values
mv_by_account = rebuilt_df.groupby("account_id")["market_value"].sum()

for account_id, account_aum in accounts_df.items():
    cash_amount = max(0.0, account_aum - mv_by_account.get(account_id, 0.0))
    rebuilt_df = pd.concat([rebuilt_df, pd.DataFrame([{
        "date":                 max_price_date,
        "account_id":           account_id,
        "ticker":               "CASH",
        "asset_class":          "Cash",
        "quantity":             cash_amount,
        "price":                1.0,
        "market_value":         cash_amount,
        "cost_basis_per_share": 1.0,
        "total_cost_basis":     cash_amount,
        "unrealized_gl":        0.0,
    }])], ignore_index=True)

# COMMAND ----------

# ── Validation summary before overwrite ───────────────────────────────────────

print(f"\n── Rebuilt holdings summary ──────────────────────────────────────────")
print(f"  Rows:              {len(rebuilt_df):,}")
print(f"  Accounts:          {rebuilt_df['account_id'].nunique():,}")
print(f"  Total market value: ${rebuilt_df['market_value'].sum() / 1e9:.2f}B")
print(f"  Total cost basis:   ${rebuilt_df['total_cost_basis'].sum() / 1e9:.2f}B")
print(f"  Total unrealized G/L: ${rebuilt_df['unrealized_gl'].sum() / 1e6:,.0f}M")
print(rebuilt_df.groupby("asset_class")["market_value"].sum()
      .sort_values(ascending=False)
      .apply(lambda x: f"${x/1e9:.2f}B")
      .reset_index()
      .to_string(index=False))

# COMMAND ----------

# ── Overwrite holdings ─────────────────────────────────────────────────────────

sdf = spark.createDataFrame(rebuilt_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("holdings"))
print(f"\nHoldings overwritten: {sdf.count():,} rows, priced as of {max_price_date}")

# COMMAND ----------

spark.table(uc_table("holdings")).orderBy("account_id", "asset_class", "ticker").display()
