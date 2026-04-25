# Databricks notebook source
# Lightweight daily update — keeps holdings and transactions current without
# regenerating the full synthetic dataset.
#
# Step 1: Append new DIVIDEND and FEE transactions for any quarters that have
#         elapsed since the last recorded DIV/FEE date.
# Step 2: Mark holdings to market using the latest prices in bronze_historical_prices.
#         Quantities, cost basis, and position composition are unchanged.
#
# Transactions are updated first (ledger of record), then holdings reflect that state.
# Run after each daily bronze_historical_prices refresh.
# clients, accounts, ips_targets, and all BUY transactions are never touched.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import uuid
import hashlib
import datetime
import numpy as np
import pandas as pd
from pyspark.sql.functions import current_timestamp

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"
today = datetime.date.today()

# COMMAND ----------

# ── Step 1: Append new DIVIDEND and FEE transactions ──────────────────────────

# Load prices now — used by both steps
prices_raw = spark.table(uc_table("bronze_historical_prices")) \
    .select("symbol", "date", "adjClose") \
    .toPandas()
prices_raw["date"] = pd.to_datetime(prices_raw["date"])

max_price_date = prices_raw["date"].max().date()

def quarter_starts_between(start_date, end_date):
    """Return list of quarter-start dates (Jan, Apr, Jul, Oct) in [start_date, end_date]."""
    dates = []
    year, month = start_date.year, start_date.month
    first_q = ((month - 1) // 3) * 3 + 1
    current = datetime.date(year, first_q, 1)
    if current < start_date:
        current = datetime.date(year, first_q + 3 if first_q <= 9 else 1, 1)
        if first_q > 9:
            current = datetime.date(year + 1, 1, 1)
    while current <= end_date:
        dates.append(current)
        m = current.month + 3
        y = current.year + (1 if m > 12 else 0)
        m = m if m <= 12 else m - 12
        current = datetime.date(y, m, 1)
    return dates

# Find what quarters are already covered — only append genuinely new ones
last_div_fee_date = spark.sql(f"""
    SELECT MAX(date) FROM {uc_table('transactions')}
    WHERE action IN ('DIVIDEND', 'FEE')
""").collect()[0][0]

if last_div_fee_date is None:
    print("No existing DIV/FEE rows found — run 05_transactions.py first")
else:
    last_div_fee_date = pd.Timestamp(last_div_fee_date).date()
    new_quarters = quarter_starts_between(
        last_div_fee_date + datetime.timedelta(days=1),
        today
    )

    if not new_quarters:
        print(f"Transactions up to date (last quarter: {last_div_fee_date})")
    else:
        print(f"Appending transactions for {len(new_quarters)} new quarter(s): {new_quarters}")

        # ── Load what we need ──────────────────────────────────────────────────
        accounts_df = spark.table(uc_table("accounts")).toPandas()
        holdings_df = spark.table(uc_table("holdings")).toPandas()

        def fee_rate_for(account_id):
            """Deterministic fee rate derived from account_id — no stored state needed."""
            seed = int(hashlib.sha256(account_id.encode()).hexdigest()[:8], 16)
            return float(np.random.default_rng(seed).uniform(0.0075, 0.0125))

        # Price series for dividend pricing on historical quarter dates
        prices_by_symbol = {
            sym: grp.set_index("date")["adjClose"].sort_index()
            for sym, grp in prices_raw.groupby("symbol")
        }

        def get_price_on(ticker, target_date):
            if ticker == "CASH":
                return 1.0
            if ticker not in prices_by_symbol:
                return None
            ser   = prices_by_symbol[ticker]
            dt    = pd.Timestamp(target_date)
            valid = ser[ser.index <= dt]
            return float(valid.iloc[-1]) if len(valid) > 0 else float(ser.iloc[0])

        # Dividend yields from financial ratios
        try:
            div_yields = spark.table(uc_table("bronze_financial_ratios")) \
                .select("symbol", "dividendYield") \
                .groupBy("symbol").avg("dividendYield") \
                .toPandas() \
                .set_index("symbol")["avg(dividendYield)"].to_dict()
        except Exception:
            div_yields = {}

        income_assets = {"Equity", "ETF", "Fixed Income", "Private Credit"}
        new_txns = []

        for _, account in accounts_df.iterrows():
            account_id  = account["account_id"]
            account_aum = account["account_aum"]
            fee_rate    = fee_rate_for(account_id)
            acct_holdings = holdings_df[holdings_df["account_id"] == account_id]

            for q_date in new_quarters:
                # ── DIVIDEND entries ───────────────────────────────────────────
                income_h = acct_holdings[acct_holdings["asset_class"].isin(income_assets)]
                for _, holding in income_h.iterrows():
                    ticker = holding["ticker"]
                    if ticker == "CASH":
                        continue
                    annual_yield = div_yields.get(ticker)
                    if annual_yield is None:
                        annual_yield = 0.015   # ETF default
                    elif annual_yield <= 0:
                        continue               # non-dividend-paying equity
                    price = get_price_on(ticker, q_date)
                    if price is None:
                        continue
                    qty     = holding["quantity"]
                    div_amt = round(qty * price * (annual_yield / 4), 2)
                    if div_amt <= 0:
                        continue
                    new_txns.append({
                        "trade_id":     str(uuid.uuid4()),
                        "date":         q_date,
                        "account_id":   account_id,
                        "ticker":       ticker,
                        "action":       "DIVIDEND",
                        "quantity":     qty,
                        "price":        price,
                        "gross_amount": div_amt,
                        "fee_amount":   0.0,
                        "net_amount":   div_amt,
                    })
                    # ── DRIP: reinvest dividend as whole shares, no commission ──
                    drip_qty = int(div_amt // price)
                    if drip_qty >= 1:
                        new_txns.append({
                            "trade_id":     str(uuid.uuid4()),
                            "date":         q_date,
                            "account_id":   account_id,
                            "ticker":       ticker,
                            "action":       "DRIP",
                            "quantity":     float(drip_qty),
                            "price":        price,
                            "gross_amount": drip_qty * price,
                            "fee_amount":   0.0,
                            "net_amount":   -(drip_qty * price),
                        })

                # ── FEE entry ──────────────────────────────────────────────────
                quarterly_fee = round(account_aum * fee_rate / 4, 2)
                new_txns.append({
                    "trade_id":     str(uuid.uuid4()),
                    "date":         q_date,
                    "account_id":   account_id,
                    "ticker":       "ADVISORY_FEE",
                    "action":       "FEE",
                    "quantity":     1.0,
                    "price":        quarterly_fee,
                    "gross_amount": quarterly_fee,
                    "fee_amount":   0.0,
                    "net_amount":   -quarterly_fee,
                })

        new_txns_df = pd.DataFrame(new_txns)
        sdf = spark.createDataFrame(new_txns_df).withColumn("ingested_at", current_timestamp())
        sdf.write.format("delta").mode("append").saveAsTable(uc_table("transactions"))
        print(f"Appended {len(new_txns_df)} new transactions")
        print(new_txns_df.groupby("action")["trade_id"].count().reset_index(name="count").to_string(index=False))

# COMMAND ----------

# ── Step 2: Mark holdings to market ───────────────────────────────────────────

# Read static columns only — these never change after initial generation
holdings_static = spark.table(uc_table("holdings")) \
    .select("account_id", "ticker", "asset_class",
            "quantity", "cost_basis_per_share", "total_cost_basis") \
    .toPandas()

latest_prices = (
    prices_raw[prices_raw["date"] == prices_raw["date"].max()]
    .set_index("symbol")["adjClose"]
    .to_dict()
)

print(f"Marking holdings to market as of {max_price_date}")

# Recompute price-dependent columns; CASH is always $1.00
holdings_static["price"]         = holdings_static["ticker"].map(latest_prices).fillna(1.0)
holdings_static["market_value"]  = holdings_static["quantity"] * holdings_static["price"]
holdings_static["unrealized_gl"] = holdings_static["quantity"] * (
    holdings_static["price"] - holdings_static["cost_basis_per_share"]
)
holdings_static["date"] = max_price_date

# Overwrite — safe because holdings is always a single-date snapshot
sdf = spark.createDataFrame(holdings_static).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("holdings"))
print(f"Holdings updated: {sdf.count()} rows, date={max_price_date}")
print(f"  Total market value: ${holdings_static['market_value'].sum() / 1e9:.2f}B")

# COMMAND ----------

# ── Verification ───────────────────────────────────────────────────────────────
spark.sql(f"""
    SELECT action, COUNT(*) as count, MAX(date) as latest_date
    FROM {uc_table('transactions')}
    GROUP BY action ORDER BY action
""").display()
