# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "edgartools",
# ]
# ///
# BDC Early Warning System — edgartools implementation
#
# Based on: https://www.edgartools.io/building-a-bdc-early-warning-system-in-python/
# Uses the edgartools library (pip: edgartools) which wraps SEC EDGAR XBRL data.
#
# BDC tickers (TICKER_CONFIG type="private_credit"):
#   AINV — Apollo Investment Corp
#   OCSL — Oaktree Specialty Lending
#   TCPC — BlackRock TCP Capital  ← case study in the article
#
# ── WARNING SIGNAL FRAMEWORK (exact thresholds from article) ──────────────────
#
# Tier 1 — Leading Indicators
#   1. PIK-to-NII Ratio
#        XBRL: InterestIncomeOperatingPaidInKind / NetInvestmentIncome
#        Watch > 20%  |  Concern > 30%
#   2. Dividend Coverage (NII per share / dividends declared)
#        XBRL: InvestmentCompanyInvestmentIncomeLossPerShare / CommonStockDividendsPerShareDeclared
#        Watch < 105%  |  Concern < 100%
#
# Tier 2 — Confirming Indicators
#   3. NAV Trajectory (consecutive quarterly declines)
#        XBRL: NetAssetValuePerShare
#        Watch 3+ quarters  |  Concern 6+ quarters
#   4. Unrealized Depreciation / NAV
#        XBRL: TaxBasisOfInvestmentsGrossUnrealizedDepreciation / AssetsNet
#        Watch > 40%  |  Concern > 55%
#   5. Non-Accrual Rate (non-accrual loans / total portfolio at fair value)
#        Not XBRL-tagged — requires Schedule of Investments parsing
#        Watch > 1.5%  |  Concern > 3%
#
# Tier 3 — Lagging Indicators
#   6. Realized Loss Acceleration (YoY growth in realized losses)
#        XBRL: RealizedInvestmentGainsLosses
#        Watch > 2x prior year  |  Concern > 4x prior year
#   7. Cumulative Losses vs. Book Value
#        XBRL: InvestmentCompanyGainLossOnInvestmentPerShare (cumulative sum) vs. NAV/share
#        Watch > 50%  |  Concern > 100%

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

from edgar import Company, set_identity
import pandas as pd

set_identity("andrew.tolbert@databricks.com")  # required by SEC EDGAR fair-use policy

client      = FMPClient(api_key=FMP_API_KEY)
BDC_TICKERS = get_tickers(types=["private_credit"])
print(f"BDC tickers: {BDC_TICKERS}")

def get_company(ticker: str) -> Company:
    """Return an edgartools Company using CIK looked up via FMP profile."""
    return Company(int(client.get_cik(ticker)))

THRESHOLDS = {
    "pik_nii_watch":         20.0,   "pik_nii_concern":       30.0,
    "div_coverage_watch":   105.0,   "div_coverage_concern": 100.0,
    "nav_decline_watch":      3,     "nav_decline_concern":    6,
    "deprec_nav_watch":      40.0,   "deprec_nav_concern":    55.0,
    "rl_accel_watch":         2.0,   "rl_accel_concern":       4.0,
    "cum_loss_watch":        50.0,   "cum_loss_concern":     100.0,
}

CONCEPTS = {
    "pik":        "us-gaap:InterestIncomeOperatingPaidInKind",
    "nii":        "us-gaap:NetInvestmentIncome",
    "nii_ps":     "us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare",
    "div_ps":     "us-gaap:CommonStockDividendsPerShareDeclared",
    "nav_ps":     "us-gaap:NetAssetValuePerShare",
    "deprec":     "us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation",
    "net_assets": "us-gaap:AssetsNet",
    "realized_gl":"us-gaap:RealizedInvestmentGainsLosses",
    "gl_ps":      "us-gaap:InvestmentCompanyGainLossOnInvestmentPerShare",
}

# COMMAND ----------

# ── Step 1: Fetch all time series for all tickers into one long DataFrame ──────

rows = []
for ticker in BDC_TICKERS:
    try:
        facts = get_company(ticker).get_facts()
        for metric, concept in CONCEPTS.items():
            ts = facts.time_series(concept)
            if ts is not None and not ts.empty:
                ts = ts[["period_end", "fiscal_period", "numeric_value"]].copy()
                ts["ticker"] = ticker
                ts["metric"] = metric
                rows.append(ts)
        print(f"  {ticker}: fetched")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

raw = pd.concat(rows, ignore_index=True)
print(f"\nTotal rows collected: {len(raw)}")
display(raw.groupby(["ticker", "metric"]).size().unstack(fill_value=0))

# COMMAND ----------

# ── Step 2: Build FY snapshot — latest annual value per ticker/metric ──────────

fy_latest = (
    raw[raw["fiscal_period"] == "FY"]
    .sort_values("period_end")
    .drop_duplicates(subset=["ticker", "metric"], keep="last")
    .pivot(index="ticker", columns="metric", values="numeric_value")
)

display(fy_latest.round(2))

# COMMAND ----------

# ── Step 3: Compute all signals from the collected data ────────────────────────

signals = pd.DataFrame(index=fy_latest.index)

# T1: PIK / NII (%)
if {"pik", "nii"}.issubset(fy_latest.columns):
    signals["T1_pik_nii_pct"] = (fy_latest["pik"] / fy_latest["nii"] * 100).round(1)

# T1: Dividend coverage (%)
if {"nii_ps", "div_ps"}.issubset(fy_latest.columns):
    signals["T1_div_coverage_pct"] = (fy_latest["nii_ps"] / fy_latest["div_ps"] * 100).round(1)

# T2: NAV consecutive quarterly declines — requires full time series
nav_ts = raw[raw["metric"] == "nav_ps"].sort_values(["ticker", "period_end"])
for ticker, grp in nav_ts.groupby("ticker"):
    vals = grp["numeric_value"].tolist()
    consec = 0
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] < vals[i - 1]: consec += 1
        else: break
    signals.loc[ticker, "T2_nav_consec_declines"] = int(consec)

# T2: Unrealized depreciation / net assets (%)
if {"deprec", "net_assets"}.issubset(fy_latest.columns):
    signals["T2_deprec_pct_nav"] = (fy_latest["deprec"] / fy_latest["net_assets"] * 100).round(1)

# T3: Realized loss YoY acceleration multiple
rl_ts = (
    raw[(raw["metric"] == "realized_gl") & (raw["fiscal_period"] == "FY")]
    .sort_values(["ticker", "period_end"])
)
for ticker, grp in rl_ts.groupby("ticker"):
    losses = grp["numeric_value"].clip(upper=0).abs().tolist()
    if len(losses) >= 2 and losses[-2] > 0:
        signals.loc[ticker, "T3_rl_yoy_multiple"] = round(losses[-1] / losses[-2], 2)

# T3: Cumulative per-share losses as % of current NAV/share
gl_ts = (
    raw[(raw["metric"] == "gl_ps") & (raw["fiscal_period"] == "FY")]
    .sort_values(["ticker", "period_end"])
)
for ticker, grp in gl_ts.groupby("ticker"):
    cum_loss = abs(grp["numeric_value"].clip(upper=0).sum())
    nav_val  = fy_latest.loc[ticker, "nav_ps"] if ticker in fy_latest.index and "nav_ps" in fy_latest.columns else None
    if nav_val and nav_val > 0:
        signals.loc[ticker, "T3_cum_loss_pct_nav"] = round(cum_loss / nav_val * 100, 1)

display(signals)

# COMMAND ----------

# ── Step 4: Apply traffic-light labels and display dashboard ───────────────────

def flag(val, watch, concern, low_is_bad=False):
    if pd.isna(val): return "⚪ n/a"
    over_concern = val <= concern if low_is_bad else val >= concern
    over_watch   = val <= watch   if low_is_bad else val >= watch
    icon = "🔴" if over_concern else ("🟡" if over_watch else "🟢")
    return f"{val} {icon}"

dashboard = signals.copy().astype(object)
for col in dashboard.columns:
    if col not in signals.columns: continue
    t = THRESHOLDS
    params = {
        "T1_pik_nii_pct":          (t["pik_nii_watch"],       t["pik_nii_concern"],       False),
        "T1_div_coverage_pct":     (t["div_coverage_watch"],  t["div_coverage_concern"],  True),
        "T2_nav_consec_declines":  (t["nav_decline_watch"],   t["nav_decline_concern"],   False),
        "T2_deprec_pct_nav":       (t["deprec_nav_watch"],    t["deprec_nav_concern"],    False),
        "T3_rl_yoy_multiple":      (t["rl_accel_watch"],      t["rl_accel_concern"],      False),
        "T3_cum_loss_pct_nav":     (t["cum_loss_watch"],      t["cum_loss_concern"],      False),
    }.get(col)
    if params:
        dashboard[col] = signals[col].apply(lambda v: flag(v, *params))

display(dashboard.T)

# COMMAND ----------

# ── Bonus: edgartools BDC registry + bulk portfolio dataset ───────────────────

from edgar.bdc import get_bdc_list, fetch_bdc_dataset

bdcs = get_bdc_list()
print(f"BDCs in SEC registry: {len(bdcs)}")
display(bdcs.head(10))

# COMMAND ----------

dataset = fetch_bdc_dataset(2024, 4)
top = dataset.schedule_of_investments.top_companies(25)
print("Top 25 portfolio companies by number of BDC holders:")
display(top)
