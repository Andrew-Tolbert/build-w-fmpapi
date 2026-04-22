# Databricks notebook source
# Generate synthetic daily holdings for all clients.
# Asset pool is drawn directly from TICKER_CONFIG, with sector/industry metadata
# embedded for portfolio composition and exposure-limit views.
#
# Key demo constraints:
#   - Top-tier BDCs only in client portfolios; only bdc_eligible clients hold them
#   - OBDC is flagged near covenant breach — drives Agent 1 cross-account scan
#   - Deliberate allocation drift in Private Credit (over max_pct) for demo alert
#   - TLH opportunities seeded for ~30% of equity positions
#
# Target: {catalog}.{schema}.daily_holdings

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import random
import datetime
import pandas as pd
from pyspark.sql.functions import current_timestamp

random.seed(42)
uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('daily_holdings')}")

# COMMAND ----------

clients_df = spark.table(uc_table("clients")).toPandas()
ips_df     = spark.table(uc_table("client_ips_targets")).toPandas()
as_of_date = datetime.date.today().isoformat()

print(f"Clients: {len(clients_df)} | IPS rows: {len(ips_df)}")

# COMMAND ----------

# ── Equity pool — grouped by GICS sector ──────────────────────────────────────
# (sector, industry, geography) for every equity ticker in TICKER_CONFIG

EQUITY_META = {
    # Financials
    "GS":   ("Financials", "Investment Banking & Brokerage",        "US"),
    "MS":   ("Financials", "Investment Banking & Brokerage",        "US"),
    "JPM":  ("Financials", "Diversified Banks",                     "US"),
    "BAC":  ("Financials", "Diversified Banks",                     "US"),
    "C":    ("Financials", "Diversified Banks",                     "US"),
    "WFC":  ("Financials", "Diversified Banks",                     "US"),
    "BX":   ("Financials", "Asset Management & Custody Banks",      "US"),
    "APO":  ("Financials", "Asset Management & Custody Banks",      "US"),
    "KKR":  ("Financials", "Asset Management & Custody Banks",      "US"),
    "BLK":  ("Financials", "Asset Management & Custody Banks",      "US"),
    "SCHW": ("Financials", "Investment Brokerage",                  "US"),
    "ICE":  ("Financials", "Financial Exchanges & Data",            "US"),
    "CME":  ("Financials", "Financial Exchanges & Data",            "US"),
    # Technology
    "AAPL": ("Technology",              "Technology Hardware",       "US"),
    "MSFT": ("Technology",              "Systems Software",         "US"),
    "NVDA": ("Technology",              "Semiconductors",           "US"),
    "GOOGL":("Communication Services",  "Interactive Media",        "US"),
    "META": ("Communication Services",  "Interactive Media",        "US"),
    "AMZN": ("Consumer Discretionary",  "E-Commerce",               "US"),
    "TSLA": ("Consumer Discretionary",  "Automobile Manufacturers", "US"),
    "AVGO": ("Technology",              "Semiconductors",           "US"),
    "ORCL": ("Technology",              "Systems Software",         "US"),
    "CRM":  ("Technology",              "Application Software",     "US"),
    "ADBE": ("Technology",              "Application Software",     "US"),
    "AMD":  ("Technology",              "Semiconductors",           "US"),
    "QCOM": ("Technology",              "Semiconductors",           "US"),
    # Healthcare
    "JNJ":  ("Health Care", "Pharmaceuticals",                      "US"),
    "UNH":  ("Health Care", "Managed Health Care",                  "US"),
    "LLY":  ("Health Care", "Pharmaceuticals",                      "US"),
    "ABBV": ("Health Care", "Biotechnology",                        "US"),
    "PFE":  ("Health Care", "Pharmaceuticals",                      "US"),
    "MRK":  ("Health Care", "Pharmaceuticals",                      "US"),
    "TMO":  ("Health Care", "Life Sciences Tools & Services",       "US"),
    "AMGN": ("Health Care", "Biotechnology",                        "US"),
    # Consumer Staples
    "WMT":  ("Consumer Staples",     "Hypermarkets & Super Centers",     "US"),
    "COST": ("Consumer Staples",     "Hypermarkets & Super Centers",     "US"),
    "PG":   ("Consumer Staples",     "Personal & Household Products",    "US"),
    "KO":   ("Consumer Staples",     "Soft Drinks & Non-alcoholic Beverages", "US"),
    "PEP":  ("Consumer Staples",     "Soft Drinks & Non-alcoholic Beverages", "US"),
    # Consumer Discretionary
    "HD":   ("Consumer Discretionary", "Home Improvement Retail",  "US"),
    "MCD":  ("Consumer Discretionary", "Restaurants",              "US"),
    "NKE":  ("Consumer Discretionary", "Footwear",                 "US"),
    "SBUX": ("Consumer Discretionary", "Restaurants",              "US"),
    "TGT":  ("Consumer Discretionary", "General Merchandise Stores","US"),
    # Industrials
    "CAT":  ("Industrials", "Construction Machinery & Heavy Equipment", "US"),
    "DE":   ("Industrials", "Agricultural & Farm Machinery",        "US"),
    "HON":  ("Industrials", "Industrial Conglomerates",             "US"),
    "GE":   ("Industrials", "Industrial Conglomerates",             "US"),
    "RTX":  ("Industrials", "Aerospace & Defense",                  "US"),
    "BA":   ("Industrials", "Aerospace & Defense",                  "US"),
    "UPS":  ("Industrials", "Air Freight & Logistics",              "US"),
    # Energy & Utilities
    "XOM":  ("Energy",     "Integrated Oil & Gas",          "US"),
    "CVX":  ("Energy",     "Integrated Oil & Gas",          "US"),
    "COP":  ("Energy",     "Oil & Gas E&P",                 "US"),
    "NEE":  ("Utilities",  "Electric Utilities",            "US"),
    # Materials & Real Estate
    "FCX":  ("Materials",   "Copper",                        "US"),
    "NEM":  ("Materials",   "Gold",                          "US"),
    "LIN":  ("Materials",   "Industrial Gases",              "US"),
    "AMT":  ("Real Estate", "Specialized REITs",             "US"),
    "PLD":  ("Real Estate", "Industrial REITs",              "US"),
    "WELL": ("Real Estate", "Health Care REITs",             "US"),
    "CCI":  ("Real Estate", "Specialized REITs",             "US"),
}

# Group equity tickers by sector for diversified sampling
from collections import defaultdict
EQUITY_BY_SECTOR = defaultdict(list)
for ticker, (sector, industry, geo) in EQUITY_META.items():
    EQUITY_BY_SECTOR[sector].append(ticker)

# ── ETF pool ──────────────────────────────────────────────────────────────────
# (asset_class_label, sector/strategy, geography)

ETF_META = {
    # Broad market → ETF bucket
    "SPY":  ("ETF", "Broad US Equity — S&P 500",         "US"),
    "QQQ":  ("ETF", "US Large Cap Growth — Nasdaq 100",  "US"),
    "IWM":  ("ETF", "US Small Cap",                      "US"),
    "VTI":  ("ETF", "Total US Market",                   "US"),
    "VOO":  ("ETF", "Broad US Equity — S&P 500",         "US"),
    "DIA":  ("ETF", "US Large Cap Value — Dow Jones",    "US"),
    # Sector → ETF bucket
    "XLF":  ("ETF", "Financials Sector",                 "US"),
    "XLK":  ("ETF", "Technology Sector",                 "US"),
    "XLE":  ("ETF", "Energy Sector",                     "US"),
    "XLV":  ("ETF", "Health Care Sector",                "US"),
    "XLI":  ("ETF", "Industrials Sector",                "US"),
    "XLY":  ("ETF", "Consumer Discretionary Sector",     "US"),
    "XLU":  ("ETF", "Utilities Sector",                  "US"),
    "XLP":  ("ETF", "Consumer Staples Sector",           "US"),
    "XLRE": ("ETF", "Real Estate Sector",                "US"),
    # International → ETF bucket
    "EFA":  ("ETF", "Developed Market ex-US",            "International"),
    "EEM":  ("ETF", "Emerging Markets",                  "International"),
    "VEU":  ("ETF", "All World ex-US",                   "International"),
    # Commodities → ETF bucket
    "GLD":  ("ETF", "Gold",                              "Global"),
    "SLV":  ("ETF", "Silver",                            "Global"),
    "DBC":  ("ETF", "Broad Commodities",                 "Global"),
    "VNQ":  ("ETF", "US REITs",                          "US"),
}

ETF_BROAD       = ["SPY", "QQQ", "IWM", "VTI", "VOO", "DIA"]
ETF_SECTOR      = ["XLF", "XLK", "XLE", "XLV", "XLI", "XLY", "XLU", "XLP", "XLRE"]
ETF_INTL        = ["EFA", "EEM", "VEU"]
ETF_COMMODITY   = ["GLD", "SLV", "DBC", "VNQ"]

# ── Fixed income ETF pool → Fixed Income bucket ───────────────────────────────
FI_ETF_META = {
    "AGG":  ("Fixed Income", "US Core Aggregate Bonds",          "US"),
    "TLT":  ("Fixed Income", "US Long-Duration Treasuries",      "US"),
    "LQD":  ("Fixed Income", "US Investment Grade Corp Bonds",   "US"),
    "HYG":  ("Fixed Income", "US High Yield Corporate Bonds",    "US"),
    "JNK":  ("Fixed Income", "US High Yield Corporate Bonds",    "US"),
    "EMB":  ("Fixed Income", "EM USD-Denominated Bonds",         "Global"),
    "BIL":  ("Fixed Income", "Short-Term US Treasury Bills",     "US"),
    "SHY":  ("Fixed Income", "Short-Term US Treasuries",         "US"),
    "BKLN": ("Fixed Income", "Senior Loans / Floating Rate",     "US"),
}

# ── Top-tier BDCs → Private Credit bucket ────────────────────────────────────
# Only these appear in client portfolios. OBDC is the covenant-stress name.
TOP_TIER_BDCS = ["ARCC", "MAIN", "BXSL", "OBDC", "HTGC", "GBDC"]
BDC_STRESS_NAME = "OBDC"  # Blue Owl Capital — approaching covenant breach

BDC_META = {
    "ARCC": ("Private Credit", "BDC — Senior Secured Lending",      "US"),
    "MAIN": ("Private Credit", "BDC — Middle Market Lending",       "US"),
    "BXSL": ("Private Credit", "BDC — Large Cap Senior Secured",    "US"),
    "OBDC": ("Private Credit", "BDC — Diversified Direct Lending",  "US"),
    "HTGC": ("Private Credit", "BDC — Technology & Life Sciences",  "US"),
    "GBDC": ("Private Credit", "BDC — Middle Market Lending",       "US"),
}

# ── Synthetic PE funds → Alternatives bucket ─────────────────────────────────
PE_FUNDS = [
    ("FUND_BX_RE23",   "Blackstone Real Estate Partners XI",     "Alternatives", "Real Estate PE",         "US"),
    ("FUND_KKR_AM15",  "KKR Americas XV",                        "Alternatives", "Buyout PE",              "US"),
    ("FUND_APO_X",     "Apollo Investment Fund X",               "Alternatives", "Credit & Distressed PE", "US"),
    ("FUND_CG_VII",    "Carlyle Partners VII",                   "Alternatives", "Buyout PE",              "US"),
    ("FUND_TPG_VIII",  "TPG Capital VIII",                       "Alternatives", "Growth Equity PE",       "US"),
    ("FUND_NB_IX",     "Neuberger Berman Private Equity IX",     "Alternatives", "Growth Equity PE",       "Global"),
    ("FUND_WP_XIV",    "Warburg Pincus Global Growth XIV",       "Alternatives", "Growth Equity PE",       "Global"),
    ("FUND_GAM_IV",    "General Atlantic Strategic Capital IV",  "Alternatives", "Growth Equity PE",       "Global"),
]

# COMMAND ----------

def _drift(target, lo, hi):
    """Return an actual allocation with slight random drift."""
    return round(random.uniform(max(lo, target - 3.0), min(hi, target + 3.0)), 2)

def _distribute(total_pct, n):
    """Distribute total_pct across n holdings using a random split."""
    cuts = sorted(random.uniform(0, total_pct) for _ in range(n - 1))
    cuts = [0.0] + cuts + [total_pct]
    return [round(cuts[i+1] - cuts[i], 2) for i in range(n)]

records = []
position_counter = 1

# Cache IPS lookup per client
ips_by_client = ips_df.set_index(["client_id", "asset_class"])

for _, client in clients_df.iterrows():
    client_id    = client["client_id"]
    tier         = client["tier"]
    total_aum    = float(client["total_aum"])
    bdc_eligible = bool(client["bdc_eligible"])

    def ips(asset_class, field):
        return float(ips_by_client.loc[(client_id, asset_class), field])

    # ── Equity positions ──────────────────────────────────────────────────────
    eq_target = ips("Equity", "target_allocation_pct")
    eq_lo     = ips("Equity", "min_allocation_pct")
    eq_hi     = ips("Equity", "max_allocation_pct")
    eq_actual = _drift(eq_target, eq_lo, eq_hi)

    # Pick 2-4 stocks per sector from at least 5 sectors
    sectors = list(EQUITY_BY_SECTOR.keys())
    random.shuffle(sectors)
    chosen_equities = []
    for sector in sectors[:min(len(sectors), 7)]:
        k = min(len(EQUITY_BY_SECTOR[sector]), random.randint(1, 3))
        chosen_equities.extend(random.sample(EQUITY_BY_SECTOR[sector], k))
    # Trim to a realistic count
    n_eq = random.randint(10, 16)
    chosen_equities = chosen_equities[:n_eq]

    eq_weights = _distribute(eq_actual, len(chosen_equities))
    for ticker, alloc_pct in zip(chosen_equities, eq_weights):
        sector, industry, geo = EQUITY_META[ticker]
        mkt_value  = round(total_aum * alloc_pct / 100, 2)
        cost_basis = round(mkt_value * random.uniform(0.78, 1.25), 2)
        price      = random.uniform(50, 900)
        records.append({
            "position_id":                 f"POS{position_counter:06d}",
            "client_id":                   client_id,
            "asset_id":                    ticker,
            "asset_class":                 "Equity",
            "holding_name":                ticker,
            "sector":                      sector,
            "industry":                    industry,
            "geography":                   geo,
            "market_value":                mkt_value,
            "current_allocation_pct":      alloc_pct,
            "cost_basis":                  cost_basis,
            "quantity":                    round(mkt_value / price, 4),
            "as_of_date":                  as_of_date,
            "tax_loss_harvesting_eligible": cost_basis > mkt_value,
        })
        position_counter += 1

    # ── Fixed income ETF positions ────────────────────────────────────────────
    fi_target = ips("Fixed Income", "target_allocation_pct")
    fi_lo     = ips("Fixed Income", "min_allocation_pct")
    fi_hi     = ips("Fixed Income", "max_allocation_pct")
    fi_actual = _drift(fi_target, fi_lo, fi_hi)

    fi_tickers = random.sample(list(FI_ETF_META.keys()), random.randint(2, 4))
    fi_weights = _distribute(fi_actual, len(fi_tickers))
    for ticker, alloc_pct in zip(fi_tickers, fi_weights):
        asset_class_label, industry, geo = FI_ETF_META[ticker]
        mkt_value  = round(total_aum * alloc_pct / 100, 2)
        cost_basis = round(mkt_value * random.uniform(0.85, 1.15), 2)
        price      = random.uniform(80, 120)
        records.append({
            "position_id":                 f"POS{position_counter:06d}",
            "client_id":                   client_id,
            "asset_id":                    ticker,
            "asset_class":                 "Fixed Income",
            "holding_name":                ticker,
            "sector":                      "Fixed Income",
            "industry":                    industry,
            "geography":                   geo,
            "market_value":                mkt_value,
            "current_allocation_pct":      alloc_pct,
            "cost_basis":                  cost_basis,
            "quantity":                    round(mkt_value / price, 4),
            "as_of_date":                  as_of_date,
            "tax_loss_harvesting_eligible": cost_basis > mkt_value,
        })
        position_counter += 1

    # ── ETF positions (broad market, sector, international, commodity) ─────────
    etf_target = ips("ETF", "target_allocation_pct")
    etf_lo     = ips("ETF", "min_allocation_pct")
    etf_hi     = ips("ETF", "max_allocation_pct")
    etf_actual = _drift(etf_target, etf_lo, etf_hi)

    etf_pool = (
        random.sample(ETF_BROAD, random.randint(1, 2)) +
        random.sample(ETF_SECTOR, random.randint(1, 3)) +
        random.sample(ETF_INTL, random.randint(0, 2)) +
        random.sample(ETF_COMMODITY, random.randint(0, 1))
    )
    etf_weights = _distribute(etf_actual, len(etf_pool))
    for ticker, alloc_pct in zip(etf_pool, etf_weights):
        _, industry, geo = ETF_META[ticker]
        mkt_value  = round(total_aum * alloc_pct / 100, 2)
        cost_basis = round(mkt_value * random.uniform(0.85, 1.15), 2)
        price      = random.uniform(30, 500)
        records.append({
            "position_id":                 f"POS{position_counter:06d}",
            "client_id":                   client_id,
            "asset_id":                    ticker,
            "asset_class":                 "ETF",
            "holding_name":                ticker,
            "sector":                      "ETF",
            "industry":                    industry,
            "geography":                   geo,
            "market_value":                mkt_value,
            "current_allocation_pct":      alloc_pct,
            "cost_basis":                  cost_basis,
            "quantity":                    round(mkt_value / price, 4),
            "as_of_date":                  as_of_date,
            "tax_loss_harvesting_eligible": cost_basis > mkt_value,
        })
        position_counter += 1

    # ── Alternatives — PE funds ───────────────────────────────────────────────
    alt_target = ips("Alternatives", "target_allocation_pct")
    if alt_target > 0:
        alt_lo     = ips("Alternatives", "min_allocation_pct")
        alt_hi     = ips("Alternatives", "max_allocation_pct")
        alt_actual = _drift(alt_target, alt_lo, alt_hi)
        chosen_pe  = random.sample(PE_FUNDS, random.randint(2, 3))
        alt_weights = _distribute(alt_actual, len(chosen_pe))
        for (asset_id, holding_name, asset_class_label, industry, geo), alloc_pct in zip(chosen_pe, alt_weights):
            mkt_value  = round(total_aum * alloc_pct / 100, 2)
            cost_basis = round(mkt_value * random.uniform(0.80, 1.20), 2)
            records.append({
                "position_id":                 f"POS{position_counter:06d}",
                "client_id":                   client_id,
                "asset_id":                    asset_id,
                "asset_class":                 "Alternatives",
                "holding_name":                holding_name,
                "sector":                      "Alternatives",
                "industry":                    industry,
                "geography":                   geo,
                "market_value":                mkt_value,
                "current_allocation_pct":      alloc_pct,
                "cost_basis":                  cost_basis,
                "quantity":                    1.0,
                "as_of_date":                  as_of_date,
                "tax_loss_harvesting_eligible": False,
            })
            position_counter += 1

    # ── Private Credit — top-tier BDCs (bdc_eligible clients only) ────────────
    pc_target = ips("Private Credit", "target_allocation_pct")
    if pc_target > 0 and bdc_eligible:
        pc_hi     = ips("Private Credit", "max_allocation_pct")
        # Deliberate drift over max for a subset — triggers allocation drift alert
        if random.random() < 0.4:
            pc_actual = round(random.uniform(pc_hi + 0.5, pc_hi + 4.0), 2)
        else:
            pc_actual = _drift(pc_target, ips("Private Credit", "min_allocation_pct"), pc_hi)

        # Bias OBDC into ~60% of bdc_eligible portfolios for the covenant story
        if random.random() < 0.60:
            available_bdcs = [BDC_STRESS_NAME] + random.sample(
                [b for b in TOP_TIER_BDCS if b != BDC_STRESS_NAME],
                random.randint(0, 2)
            )
        else:
            available_bdcs = random.sample(
                [b for b in TOP_TIER_BDCS if b != BDC_STRESS_NAME],
                random.randint(1, 2)
            )

        pc_weights = _distribute(pc_actual, len(available_bdcs))
        for ticker, alloc_pct in zip(available_bdcs, pc_weights):
            _, industry, geo = BDC_META[ticker]
            mkt_value  = round(total_aum * alloc_pct / 100, 2)
            cost_basis = round(mkt_value * random.uniform(0.88, 1.12), 2)
            price      = random.uniform(8, 22)
            records.append({
                "position_id":                 f"POS{position_counter:06d}",
                "client_id":                   client_id,
                "asset_id":                    ticker,
                "asset_class":                 "Private Credit",
                "holding_name":                ticker,
                "sector":                      "Private Credit",
                "industry":                    industry,
                "geography":                   geo,
                "market_value":                mkt_value,
                "current_allocation_pct":      alloc_pct,
                "cost_basis":                  cost_basis,
                "quantity":                    round(mkt_value / price, 4),
                "as_of_date":                  as_of_date,
                "tax_loss_harvesting_eligible": cost_basis > mkt_value,
            })
            position_counter += 1

    # ── Cash ──────────────────────────────────────────────────────────────────
    cash_target = ips("Cash", "target_allocation_pct")
    cash_lo     = ips("Cash", "min_allocation_pct")
    cash_hi     = ips("Cash", "max_allocation_pct")
    cash_actual = _drift(cash_target, cash_lo, cash_hi)
    mkt_value   = round(total_aum * cash_actual / 100, 2)
    records.append({
        "position_id":                 f"POS{position_counter:06d}",
        "client_id":                   client_id,
        "asset_id":                    "CASH_USD",
        "asset_class":                 "Cash",
        "holding_name":                "USD Cash & Money Market",
        "sector":                      "Cash",
        "industry":                    "Cash & Equivalents",
        "geography":                   "US",
        "market_value":                mkt_value,
        "current_allocation_pct":      cash_actual,
        "cost_basis":                  mkt_value,
        "quantity":                    mkt_value,
        "as_of_date":                  as_of_date,
        "tax_loss_harvesting_eligible": False,
    })
    position_counter += 1

# COMMAND ----------

holdings_df = pd.DataFrame(records)
print(f"Generated {len(holdings_df)} positions across {len(clients_df)} clients")
print(f"Avg positions per client: {len(holdings_df) / len(clients_df):.1f}")

obdc_holders = holdings_df[holdings_df["asset_id"] == BDC_STRESS_NAME]["client_id"].nunique()
print(f"\nClients holding {BDC_STRESS_NAME} (covenant-stress scenario): {obdc_holders}")

by_class = holdings_df.groupby("asset_class")["market_value"].agg(["count", "sum"])
by_class.columns = ["positions", "total_market_value"]
print("\nPositions and AUM by asset class:")
print(by_class.to_string())

# COMMAND ----------

target = uc_table("daily_holdings")
sdf = spark.createDataFrame(holdings_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
