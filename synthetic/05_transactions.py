# Databricks notebook source
# Generate synthetic transaction history consistent with the holdings table.
# Each holding is established via 3–5 initial BUY tranches spread over the first 90 days
# after account inception. Quarterly DIVIDEND entries are added for equity/ETF/BDC positions.
# Quarterly FEE entries (GS PWM advisory fee) are added per account.
# Prices are sourced from bronze_historical_prices; dates before price history use earliest
# available price as an approximation.
# Target: {catalog}.{schema}.transactions

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import uuid
import random
import numpy as np
import pandas as pd
import datetime
from pyspark.sql.functions import current_timestamp

rng = np.random.default_rng(45)
random.seed(45)

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# # Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('transactions')}")

# COMMAND ----------

# ── Load source tables ─────────────────────────────────────────────────────────
holdings_df = spark.table(uc_table("holdings")).toPandas()
accounts_df = spark.table(uc_table("accounts")).toPandas()

print(f"Holdings: {len(holdings_df)} | Accounts: {len(accounts_df)}")

# COMMAND ----------

# ── Load price history for transaction pricing ─────────────────────────────────
prices_raw = spark.table(uc_table("bronze_historical_prices")) \
    .select("symbol", "date", "adjClose") \
    .toPandas()
prices_raw["date"] = pd.to_datetime(prices_raw["date"])
prices_raw = prices_raw.sort_values(["symbol", "date"])

# Also load dividend yield from financial ratios for realistic dividend amounts
try:
    div_yields = spark.table(uc_table("bronze_financial_ratios")) \
        .select("symbol", "dividendYield") \
        .groupBy("symbol").avg("dividendYield") \
        .toPandas() \
        .set_index("symbol")["avg(dividendYield)"].to_dict()
except Exception:
    div_yields = {}

# Per-symbol sorted price series for date lookups
prices_by_symbol = {
    sym: grp.set_index("date")["adjClose"].sort_index()
    for sym, grp in prices_raw.groupby("symbol")
}
min_price_date = prices_raw["date"].min()

def get_price_on(ticker, target_date):
    """Return adjClose on or before target_date; falls back to earliest available."""
    if ticker == "CASH":
        return 1.0
    if ticker not in prices_by_symbol:
        return None
    ser = prices_raw[prices_raw["symbol"] == ticker].set_index("date")["adjClose"]
    dt  = pd.Timestamp(target_date)
    valid = ser[ser.index <= dt]
    return float(valid.iloc[-1]) if len(valid) > 0 else float(ser.iloc[0])

# COMMAND ----------

# ── Synthetic alt-fund price lookup (fixed NAV) ────────────────────────────────
ALT_FUND_NAVS = {
    "GS_VINTAGE_IX":  1000.0,
    "GS_GROWTH_EQ3":  2500.0,
    "GS_INFRA_II":    1500.0,
    "GS_REAL_ASST4":  1200.0,
    "GS_MACRO_OPP":    500.0,
}

def get_price(ticker, date):
    if ticker in ALT_FUND_NAVS:
        return ALT_FUND_NAVS[ticker]
    p = get_price_on(ticker, date)
    return p if p else None

# COMMAND ----------

# ── Fee rate per account (annualized) ─────────────────────────────────────────
account_fee_rates = {
    acc_id: float(rng.uniform(0.0075, 0.0125))
    for acc_id in accounts_df["account_id"]
}
account_lookup = accounts_df.set_index("account_id").to_dict(orient="index")

today = datetime.date.today()

# ── Quarter-start dates from price history start to today ─────────────────────
price_start = min_price_date.date()

def quarter_starts_between(start_date, end_date):
    """Return list of quarter-start dates (Jan, Apr, Jul, Oct) in [start_date, end_date]."""
    dates = []
    year, month = start_date.year, start_date.month
    first_q = ((month - 1) // 3) * 3 + 1
    current = datetime.date(year, first_q, 1)
    if current < start_date:
        current = datetime.date(year, first_q + 3 if first_q <= 9 else 1,
                                1 if first_q <= 9 else 1)
        if first_q > 9:
            current = datetime.date(year + 1, 1, 1)
    while current <= end_date:
        dates.append(current)
        m = current.month + 3
        y = current.year + (1 if m > 12 else 0)
        m = m if m <= 12 else m - 12
        current = datetime.date(y, m, 1)
    return dates

# COMMAND ----------

# ── Build transaction records ──────────────────────────────────────────────────
all_txns = []

for _, account in accounts_df.iterrows():
    account_id     = account["account_id"]
    account_aum    = account["account_aum"]
    inception_date = pd.Timestamp(account["inception_date"]).date()
    fee_rate       = account_fee_rates[account_id]

    acct_holdings = holdings_df[holdings_df["account_id"] == account_id]

    # ── Initial BUY tranches per holding ──────────────────────────────────────
    for _, holding in acct_holdings.iterrows():
        ticker    = holding["ticker"]
        total_qty = holding["quantity"]
        asset_cls = holding["asset_class"]

        if ticker == "CASH":
            # Model initial cash deposit as a single BUY on inception
            all_txns.append({
                "trade_id":    str(uuid.uuid4()),
                "date":        inception_date,
                "account_id":  account_id,
                "ticker":      "CASH",
                "action":      "BUY",
                "quantity":    total_qty,
                "price":       1.0,
                "gross_amount": total_qty,
                "fee_amount":   0.0,
                "net_amount":  -total_qty,
            })
            continue

        n_tranches = int(rng.integers(3, 6))

        # Split quantity across tranches using Dirichlet weights; ensure integer shares
        raw_weights = rng.dirichlet(np.full(n_tranches, 2.0))
        qtys = (raw_weights * total_qty).round().astype(int)
        diff = int(total_qty) - qtys.sum()
        qtys[0] += diff  # absorb rounding into first tranche

        for i, qty in enumerate(qtys):
            if qty <= 0:
                continue
            # Spread buys over first 90 days; earlier tranches come first
            day_offset = int(rng.integers(i * 5, min(90, (i + 1) * 20 + 5)))
            txn_date   = inception_date + datetime.timedelta(days=day_offset)
            price      = get_price(ticker, txn_date)
            if price is None:
                continue
            gross  = qty * price
            fee    = round(gross * 0.0005, 2)  # 0.05% commission
            all_txns.append({
                "trade_id":    str(uuid.uuid4()),
                "date":        txn_date,
                "account_id":  account_id,
                "ticker":      ticker,
                "action":      "BUY",
                "quantity":    float(qty),
                "price":       price,
                "gross_amount": gross,
                "fee_amount":   fee,
                "net_amount":  -(gross + fee),
            })

    # ── Quarterly DIVIDEND entries for income-generating positions ─────────────
    income_assets = {"Equity", "ETF", "Fixed Income", "Private Credit"}
    income_holdings = acct_holdings[acct_holdings["asset_class"].isin(income_assets)]

    div_quarter_starts = quarter_starts_between(
        max(inception_date, price_start),
        today
    )

    for _, holding in income_holdings.iterrows():
        ticker = holding["ticker"]
        if ticker == "CASH":
            continue

        # Estimate quarterly dividend yield
        annual_yield = div_yields.get(ticker, 0.015)
        if annual_yield is None or annual_yield <= 0:
            annual_yield = 0.015
        quarterly_rate = annual_yield / 4

        for q_date in div_quarter_starts:
            price = get_price(ticker, q_date)
            if price is None:
                continue
            qty       = holding["quantity"]
            div_amt   = round(qty * price * quarterly_rate, 2)
            if div_amt <= 0:
                continue
            all_txns.append({
                "trade_id":    str(uuid.uuid4()),
                "date":        q_date,
                "account_id":  account_id,
                "ticker":      ticker,
                "action":      "DIVIDEND",
                "quantity":    qty,
                "price":       price,
                "gross_amount": div_amt,
                "fee_amount":   0.0,
                "net_amount":   div_amt,
            })

    # ── Quarterly FEE entries (GS PWM advisory fee) ────────────────────────────
    fee_quarter_starts = quarter_starts_between(
        max(inception_date, price_start),
        today
    )
    quarterly_fee = round(account_aum * fee_rate / 4, 2)
    for q_date in fee_quarter_starts:
        all_txns.append({
            "trade_id":    str(uuid.uuid4()),
            "date":        q_date,
            "account_id":  account_id,
            "ticker":      "ADVISORY_FEE",
            "action":      "FEE",
            "quantity":    1.0,
            "price":       quarterly_fee,
            "gross_amount": quarterly_fee,
            "fee_amount":   0.0,
            "net_amount":  -quarterly_fee,
        })

txns_df = pd.DataFrame(all_txns)

# COMMAND ----------

# ── Validation ─────────────────────────────────────────────────────────────────

print(f"Transactions generated: {len(txns_df)}")
print(txns_df.groupby("action")["trade_id"].count().reset_index(name="count").to_string(index=False))

# Consistency check: net BUY qty should match holdings.quantity within 0.01
buy_qty = txns_df[txns_df["action"] == "BUY"].groupby(["account_id", "ticker"])["quantity"].sum()
holdings_qty = (
    holdings_df[~holdings_df["ticker"].isin(["CASH"])]
    .groupby(["account_id", "ticker"])["quantity"].sum()
)
check = (buy_qty - holdings_qty).abs().dropna()
mismatches = check[check > 0.01]
if len(mismatches) > 0:
    print(f"WARNING: {len(mismatches)} (account, ticker) pairs with qty mismatch > 0.01")
    print(mismatches.head(10))
else:
    print("Quantity consistency check PASSED: all BUY totals match holdings within 0.01 shares")

# COMMAND ----------

sdf = spark.createDataFrame(txns_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("transactions"))
print(f"Written {sdf.count()} rows to {uc_table('transactions')}")
