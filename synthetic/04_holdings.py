# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Generate synthetic holdings as of the most recent price date in bronze_historical_prices.
# Ticker universe and metadata come from bronze_company_profiles.
# Current prices and cost basis come from bronze_historical_prices (adjClose).
# Alternatives exposure uses real-asset ETFs (GLD, SLV, DBC, VNQ) — all have daily prices.
# BDC exposure is capped at 4% of account AUM regardless of IPS target.
# IPS drift calibrated so ~10% of accounts show at least one asset class outside min/max.
# Target: {catalog}.{schema}.holdings

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import random
import numpy as np
import pandas as pd
from pyspark.sql.functions import current_timestamp

rng = np.random.default_rng(44)
random.seed(44)

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# # Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('holdings')}")

# COMMAND ----------

# ── Ticker universe from TICKER_CONFIG (ignores LIMITED_LOAD widget) ───────────
# Using TICKER_CONFIG directly ensures we get the full universe regardless of run mode.
FIXED_INCOME_ETFS = {"AGG", "TLT", "LQD", "HYG", "JNK", "EMB", "BIL", "SHY", "BKLN"}
ALT_ETFS          = {"GLD", "SLV", "DBC", "VNQ"}

equity_universe  = [s for s, c in TICKER_CONFIG.items() if c["type"] == "equity"]
bdc_universe     = [s for s, c in TICKER_CONFIG.items() if c["type"] == "private_credit"]
all_etfs         = [s for s, c in TICKER_CONFIG.items() if c["type"] == "etf"]
broad_etf_univ   = [s for s in all_etfs if s not in FIXED_INCOME_ETFS and s not in ALT_ETFS]
fi_etf_univ      = [s for s in all_etfs if s in FIXED_INCOME_ETFS]
alt_etf_univ     = [s for s in all_etfs if s in ALT_ETFS]

print(f"Equities: {len(equity_universe)} | ETFs: {len(broad_etf_univ)} broad, {len(fi_etf_univ)} fixed income, {len(alt_etf_univ)} alts | BDCs: {len(bdc_universe)}")

# COMMAND ----------

# ── Load reference tables ──────────────────────────────────────────────────────
clients_df  = spark.table(uc_table("clients")).toPandas()
accounts_df = spark.table(uc_table("accounts")).toPandas()
ips_df      = spark.table(uc_table("ips_targets")).toPandas()

print(f"Clients: {len(clients_df)} | Accounts: {len(accounts_df)}")

# COMMAND ----------

# ── Load prices: current prices and cost-basis history ────────────────────────
# Load full price history once into Pandas — ~60 tickers × ~2yr daily = ~30k rows.
prices_raw = spark.table(uc_table("bronze_historical_prices")) \
    .select("symbol", "date", "adjClose") \
    .toPandas()
prices_raw["date"] = pd.to_datetime(prices_raw["date"])
prices_raw = prices_raw.sort_values(["symbol", "date"])

max_price_date = prices_raw["date"].max().date()
print(f"Price history: {len(prices_raw)} rows | Latest date: {max_price_date}")

# ETF → true asset class via bronze_etf_info.assetClass
etf_asset_class_map = (
    spark.table(uc_table("bronze_etf_info"))
    .select("symbol", "assetClass")
    .toPandas()
    .set_index("symbol")["assetClass"]
    .to_dict()
)

def resolve_asset_class(ticker, stored_class):
    """Return the true IPS asset class. ETF vehicles are mapped via bronze_etf_info."""
    if stored_class != "ETF":
        return stored_class
    etf_class = etf_asset_class_map.get(ticker, "Equity")
    if etf_class in ("Fixed Income", "Bond"):
        return "Fixed Income"
    if etf_class in ("Commodity", "Commodities", "Real Estate", "Multi-Asset", "Alternative"):
        return "Alternatives"
    return "Equity"

# Latest price per symbol (for current market value)
latest_prices = (
    prices_raw[prices_raw["date"] == prices_raw["date"].max()]
    .set_index("symbol")["adjClose"]
    .to_dict()
)

# Per-symbol sorted time series for cost-basis lookups
prices_by_symbol = {
    sym: grp.set_index("date")["adjClose"].sort_index()
    for sym, grp in prices_raw.groupby("symbol")
}

def get_cost_basis(ticker, inception_date):
    """Return the adjClose on or before inception_date; falls back to earliest available."""
    if ticker not in prices_by_symbol:
        return latest_prices.get(ticker, 1.0)
    ser = prices_by_symbol[ticker]
    dt  = pd.Timestamp(inception_date)
    valid = ser[ser.index <= dt]
    return float(valid.iloc[-1]) if len(valid) > 0 else float(ser.iloc[0])

# COMMAND ----------

# ── IPS targets as nested dict ─────────────────────────────────────────────────
ips_dict = {}
for _, row in ips_df.iterrows():
    ips_dict.setdefault(row["risk_profile"], {})[row["asset_class"]] = {
        "target": row["target_allocation_pct"],
        "min":    row["min_allocation_pct"],
        "max":    row["max_allocation_pct"],
    }

# ── Client lookup ──────────────────────────────────────────────────────────────
client_lookup = clients_df.set_index("client_id").to_dict(orient="index")

# COMMAND ----------

# ── Holdings construction ──────────────────────────────────────────────────────

def make_positions(tickers, asset_class, budget, inception_date, account_id, concentration=2.0):
    """Sample tickers from a list, weight by Dirichlet, return holdings rows."""
    rows = []
    valid = [t for t in tickers if t in latest_prices and latest_prices[t] > 0]
    if not valid or budget <= 0:
        return rows
    weights = rng.dirichlet(np.full(len(valid), concentration))
    for ticker, w in zip(valid, weights):
        dollar_amt = budget * w
        price = latest_prices[ticker]
        qty   = max(1.0, round(dollar_amt / price))
        cb    = get_cost_basis(ticker, inception_date)
        rows.append({
            "account_id":           account_id,
            "ticker":               ticker,
            "asset_class":          asset_class,
            "quantity":             qty,
            "price":                price,
            "market_value":         qty * price,
            "cost_basis_per_share": cb,
            "total_cost_basis":     qty * cb,
            "unrealized_gl":        qty * (price - cb),
        })
    return rows

all_holdings = []

for _, account in accounts_df.iterrows():
    account_id    = account["account_id"]
    account_aum   = account["account_aum"]
    account_type  = account["account_type"]
    inception_date = pd.Timestamp(account["inception_date"]).date()

    client      = client_lookup[account["client_id"]]
    risk_profile = client["risk_profile"]
    tier         = client["tier"]
    bdc_eligible = client["bdc_eligible"]

    # ── Apply Gaussian drift to IPS targets ────────────────────────────────────
    # sigma = (max - min) / 8 → ~10% of accounts drift outside band
    profile = ips_dict[risk_profile]
    raw_pcts = {}
    for ac, bounds in profile.items():
        sigma = (bounds["max"] - bounds["min"]) / 8.0
        raw_pcts[ac] = max(0.0, bounds["target"] + rng.normal(0, sigma))
    total_pct = sum(raw_pcts.values())
    actual_pcts = {ac: v / total_pct * 100 for ac, v in raw_pcts.items()}

    buckets = {ac: account_aum * pct / 100 for ac, pct in actual_pcts.items()}

    # ── Set cash explicitly from IPS bucket, capped at IPS max ────────────────
    # Prevents cash absorbing excess drift. Non-cash budgets are renormalized
    # to fill the remaining AUM after cash is reserved.
    ips_cash_max  = profile["Cash"]["max"]
    cash_target   = min(buckets["Cash"], account_aum * ips_cash_max / 100.0)
    non_cash_budget = account_aum - cash_target

    # Build non-cash raw buckets, adjusting Private Credit before renormalization.
    # Non-BDC accounts: drop PC entirely so that budget redistributes to other classes.
    # BDC accounts: cap PC at 4% of AUM before scaling so the spend cap never creates
    # a gap that would be silently discarded when the cash ceiling is applied.
    non_cash_raw = {}
    for ac, v in buckets.items():
        if ac == "Cash":
            continue
        if ac == "Private Credit":
            if not bdc_eligible:
                continue                          # redistribute to other asset classes
            v = min(v, account_aum * 0.04)       # cap at 4% before scaling
        non_cash_raw[ac] = v

    non_cash_total  = sum(non_cash_raw.values()) or 1.0
    scaled_buckets  = {ac: v / non_cash_total * non_cash_budget for ac, v in non_cash_raw.items()}

    rows = []

    # ── Equity (direct stocks + broad/sector ETFs) ────────────────────────────
    # broad_etf_univ (SPY, QQQ, XL* sector ETFs, international ETFs) shares the
    # Equity IPS budget. "ETF" is passed so the post-loop resolver maps every
    # ticker through bronze_etf_info — stocks fall back to "Equity", equity ETFs
    # resolve to "Equity", and any outlier (e.g. XLRE) gets its correct class.
    eq_budget = scaled_buckets.get("Equity", 0)
    if eq_budget > 0:
        equity_pool = equity_universe + broad_etf_univ
        n = int(rng.integers(8, 25))
        sample = list(rng.choice(equity_pool, size=min(n, len(equity_pool)), replace=False))
        rows += make_positions(sample, "ETF", eq_budget, inception_date, account_id)

    # ── Fixed Income ───────────────────────────────────────────────────────────
    fi_budget = scaled_buckets.get("Fixed Income", 0)
    if fi_budget > 0:
        n = int(rng.integers(3, min(7, len(fi_etf_univ) + 1)))
        sample = list(rng.choice(fi_etf_univ, size=min(n, len(fi_etf_univ)), replace=False))
        rows += make_positions(sample, "Fixed Income", fi_budget, inception_date, account_id)

    # ── Alternatives (real-asset ETFs only — GLD, SLV, DBC, VNQ) ─────────────
    alt_budget = scaled_buckets.get("Alternatives", 0)
    if alt_budget > 0:
        n = int(rng.integers(2, len(alt_etf_univ) + 1))
        sample = list(rng.choice(alt_etf_univ, size=min(n, len(alt_etf_univ)), replace=False))
        rows += make_positions(sample, "Alternatives", alt_budget, inception_date, account_id)

    # ── Private Credit / BDC ───────────────────────────────────────────────────
    # Only for BDC-eligible clients; hard cap at 4% of account AUM.
    pc_budget = scaled_buckets.get("Private Credit", 0)
    if pc_budget > 0 and bdc_eligible and bdc_universe:
        bdc_budget = min(pc_budget, account_aum * 0.04)
        n = int(rng.integers(1, 4))
        sample = list(rng.choice(bdc_universe, size=min(n, len(bdc_universe)), replace=False))
        rows += make_positions(sample, "Private Credit", bdc_budget, inception_date, account_id, concentration=3.0)

    # ── Cash: IPS target + rounding remainder, hard-capped at IPS max ─────────
    allocated_mv = sum(r["market_value"] for r in rows)
    cash_amount  = max(0.0, cash_target + (non_cash_budget - allocated_mv))
    cash_amount  = min(cash_amount, account_aum * ips_cash_max / 100.0)
    rows.append({
        "account_id":           account_id,
        "ticker":               "CASH",
        "asset_class":          "Cash",
        "quantity":             cash_amount,
        "price":                1.0,
        "market_value":         cash_amount,
        "cost_basis_per_share": 1.0,
        "total_cost_basis":     cash_amount,
        "unrealized_gl":        0.0,
    })

    for r in rows:
        r["date"] = max_price_date
    all_holdings.extend(rows)

# Resolve true asset class for every non-cash position.
# Equity/FI/Alts/PC/Cash pass through unchanged; "ETF" is mapped via bronze_etf_info.
for row in all_holdings:
    if row["ticker"] != "CASH":
        row["asset_class"] = resolve_asset_class(row["ticker"], row["asset_class"])

holdings_df = pd.DataFrame(all_holdings)

# COMMAND ----------

# ── Validation ─────────────────────────────────────────────────────────────────

# AUM reconciliation
mv_by_account = holdings_df.groupby("account_id")["market_value"].sum().reset_index(name="total_mv")
mv_check = mv_by_account.merge(accounts_df[["account_id", "account_aum"]], on="account_id")
mv_check["diff_pct"] = (mv_check["total_mv"] - mv_check["account_aum"]).abs() / mv_check["account_aum"] * 100
print(f"Holdings generated: {len(holdings_df)}")
print(f"  Accounts covered: {holdings_df['account_id'].nunique()}")
print(f"  Total market value: ${holdings_df['market_value'].sum() / 1e9:.2f}B")
print(f"  Max AUM diff: {mv_check['diff_pct'].max():.2f}%  (cash absorbs rounding)")

# BDC exposure check
bdc_holdings = holdings_df[holdings_df["asset_class"] == "Private Credit"]
if len(bdc_holdings) > 0:
    bdc_pct_by_acct = (
        bdc_holdings.groupby("account_id")["market_value"].sum()
        / accounts_df.set_index("account_id")["account_aum"]
        * 100
    ).dropna()
    print(f"  BDC exposure max: {bdc_pct_by_acct.max():.2f}% (cap is 4%)")

# Drift check: how many accounts have ≥1 asset class outside IPS band
drift_accounts = 0
for _, account in accounts_df.iterrows():
    client    = client_lookup[account["client_id"]]
    profile   = ips_dict[client["risk_profile"]]
    acct_mv   = holdings_df[holdings_df["account_id"] == account["account_id"]].groupby("asset_class")["market_value"].sum()
    total_mv  = acct_mv.sum()
    if total_mv == 0:
        continue
    for ac, bounds in profile.items():
        pct = acct_mv.get(ac, 0) / total_mv * 100
        if pct < bounds["min"] or pct > bounds["max"]:
            drift_accounts += 1
            break
print(f"  Accounts with IPS drift: {drift_accounts} / {len(accounts_df)} ({drift_accounts/len(accounts_df)*100:.1f}%)")

# COMMAND ----------

sdf = spark.createDataFrame(holdings_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("holdings"))
print(f"Written {sdf.count()} rows to {uc_table('holdings')}")

# COMMAND ----------

display(spark.table(uc_table("holdings")).orderBy("account_id", "asset_class", "ticker"))
