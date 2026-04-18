# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "requests",
# ]
# ///
# Scratchpad: pull structured financial data from SEC EDGAR — free, no API key.
#
# Two endpoints used:
#   data.sec.gov/api/xbrl/companyfacts/{CIK}.json  → all XBRL financial facts
#   data.sec.gov/submissions/{CIK}.json             → filing history / metadata
#
# CIK lookup: www.sec.gov/files/company_tickers.json maps every ticker → CIK.

# COMMAND ----------

import json
import requests
import pandas as pd

HEADERS = {"User-Agent": "andrew.tolbert@databricks.com"}  # SEC requires a real User-Agent

def cik10(cik: int) -> str:
    """Zero-pad a CIK integer to 10 digits as EDGAR expects."""
    return str(cik).zfill(10)

# COMMAND ----------

# ── Step 1: Resolve ticker → CIK ──────────────────────────────────────────────
# SEC publishes a full ticker→CIK map; fetch once and reuse.

_tickers_url = "https://www.sec.gov/files/company_tickers.json"
_ticker_map_raw = requests.get(_tickers_url, headers=HEADERS).json()
# Keys are sequential integers; values are {cik_str, ticker, title}
TICKER_TO_CIK = {v["ticker"]: int(v["cik_str"]) for v in _ticker_map_raw.values()}

def get_cik(ticker: str) -> int:
    cik = TICKER_TO_CIK.get(ticker.upper())
    if cik is None:
        raise ValueError(f"Ticker '{ticker}' not found in SEC ticker map")
    return cik

# Quick test
TEST_TICKER = "GS"
gs_cik = get_cik(TEST_TICKER)
print(f"{TEST_TICKER} CIK: {gs_cik}  (padded: {cik10(gs_cik)})")

# COMMAND ----------

# ── Step 2: Pull filing history via submissions endpoint ───────────────────────
# Returns company metadata plus a paginated list of all filings.

def get_submissions(ticker: str) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{cik10(get_cik(ticker))}.json"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

subs = get_submissions(TEST_TICKER)
print(f"Company: {subs['name']}")
print(f"SIC:     {subs['sic']} — {subs['sicDescription']}")
print(f"State:   {subs['stateOfIncorporation']}")
print(f"Fiscal year end: {subs['fiscalYearEnd']}")

# COMMAND ----------

# Most-recent filings as a DataFrame
recent = pd.DataFrame(subs["filings"]["recent"])
print(f"Total filings in recent window: {len(recent)}")
recent[["filingDate", "form", "primaryDocument", "accessionNumber"]].head(20)

# COMMAND ----------

# Filter to 10-K / 10-Q / 8-K only
key_forms = recent[recent["form"].isin(["10-K", "10-Q", "8-K"])].copy()
key_forms[["filingDate", "form", "primaryDocument"]].head(15)

# COMMAND ----------

# ── Step 3: Pull ALL structured financials via companyfacts (XBRL) ─────────────
# This single call returns every tagged financial fact the company has ever filed.
# Response shape:
#   { "cik": ..., "entityName": ..., "facts": { "us-gaap": { <concept>: { "label": ..., "units": { "USD": [...] } } } } }

def get_company_facts(ticker: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik10(get_cik(ticker))}.json"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

facts = get_company_facts(TEST_TICKER)
gaap = facts["facts"].get("us-gaap", {})
print(f"XBRL concepts available for {TEST_TICKER}: {len(gaap)}")
print(list(gaap.keys())[:30])

# COMMAND ----------

# ── Step 4: Extract a specific concept into a tidy DataFrame ───────────────────
# Each concept → units (usually "USD" or "shares") → list of {end, val, form, accn, ...}

def extract_concept(facts: dict, concept: str, unit: str = "USD") -> pd.DataFrame:
    """Pull all filed values for one US-GAAP concept into a DataFrame."""
    node = facts["facts"].get("us-gaap", {}).get(concept)
    if node is None:
        return pd.DataFrame()
    rows = node.get("units", {}).get(unit, [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["concept"] = concept
    # Keep only annual 10-K filings where 'frame' is present (point-in-time, not restated)
    df = df[df["form"] == "10-K"].copy()
    df["end"] = pd.to_datetime(df["end"])
    df = df.sort_values("end").drop_duplicates(subset=["end"], keep="last")
    return df[["end", "val", "form", "accn", "concept"]]

# Revenue: Revenues or RevenueFromContractWithCustomerExcludingAssessedTax
revenue_df = extract_concept(facts, "Revenues")
print(f"Annual revenue rows: {len(revenue_df)}")
revenue_df.tail(6)

# COMMAND ----------

# ── Step 5: Build a multi-metric summary for GS ───────────────────────────────
# Map friendly names to US-GAAP XBRL concept tags.
# Financial firms use different tags than industrials — GS uses Revenues, not NetRevenues.

CONCEPTS = {
    "revenue":      ("Revenues",                                  "USD"),
    "net_income":   ("NetIncomeLoss",                             "USD"),
    "total_assets": ("Assets",                                    "USD"),
    "equity":       ("StockholdersEquity",                        "USD"),
    "eps_diluted":  ("EarningsPerShareDiluted",                   "USD/shares"),
    "shares_out":   ("CommonStockSharesOutstanding",              "shares"),
}

rows = []
for label, (concept, unit) in CONCEPTS.items():
    df = extract_concept(facts, concept, unit)
    if df.empty:
        print(f"  {label} ({concept}): no data")
        continue
    latest = df.sort_values("end").iloc[-1]
    rows.append({"metric": label, "concept": concept, "year_end": latest["end"].year,
                 "value": latest["val"]})

summary = pd.DataFrame(rows)
print(f"\n=== {TEST_TICKER} — latest annual values ===")
display(summary)

# COMMAND ----------

# ── Step 6: Time-series of net income — last 5 fiscal years ───────────────────

ni = extract_concept(facts, "NetIncomeLoss")
ni["year"] = ni["end"].dt.year
ni_recent = ni[ni["year"] >= 2020][["year", "val"]].set_index("year")
ni_recent.columns = [f"{TEST_TICKER}_net_income"]
ni_recent["val_B"] = ni_recent[f"{TEST_TICKER}_net_income"] / 1e9
print(f"\n{TEST_TICKER} Net Income (USD billions):")
display(ni_recent[["val_B"]].rename(columns={"val_B": "Net Income ($B)"}))

# COMMAND ----------

# ── Step 7: Cross-company comparison — all equity tickers ─────────────────────
# Pull net income for every equity ticker in TICKER_CONFIG and compare.
# (No API key, no rate limit beyond being polite with the User-Agent header.)

EQUITY_TICKERS_DEMO = ["GS", "MS", "JPM", "BX", "APO", "KKR"]
COMPARE_CONCEPT     = "NetIncomeLoss"

compare_rows = []
for ticker in EQUITY_TICKERS_DEMO:
    try:
        f = get_company_facts(ticker)
        df = extract_concept(f, COMPARE_CONCEPT)
        if df.empty:
            print(f"  {ticker}: no {COMPARE_CONCEPT} data")
            continue
        df["year"] = df["end"].dt.year
        for _, row in df[df["year"] >= 2021].iterrows():
            compare_rows.append({"ticker": ticker, "year": row["year"],
                                  "net_income_B": row["val"] / 1e9})
        print(f"  {ticker}: ok")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

compare_df = pd.DataFrame(compare_rows)
pivot = compare_df.pivot(index="year", columns="ticker", values="net_income_B").round(2)
print("\nNet Income ($B) by year:")
display(pivot)

# COMMAND ----------

# ── Bonus: list every available concept for a company ─────────────────────────
# Useful for exploring what a company actually reports vs. what you expect.

print(f"All US-GAAP concepts filed by {TEST_TICKER}:\n")
for concept in sorted(gaap.keys()):
    units_available = list(gaap[concept].get("units", {}).keys())
    print(f"  {concept:60s}  {units_available}")
