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

from edgar import Company, set_identity
import pandas as pd

set_identity("andrew.tolbert@databricks.com")  # required by SEC EDGAR fair-use policy

BDC_TICKERS = get_tickers(types=["private_credit"])
print(f"BDC tickers: {BDC_TICKERS}")

THRESHOLDS = {
    "pik_nii_watch":         20.0,
    "pik_nii_concern":       30.0,
    "div_coverage_watch":   105.0,
    "div_coverage_concern": 100.0,
    "nav_decline_watch":      3,
    "nav_decline_concern":    6,
    "deprec_nav_watch":      40.0,
    "deprec_nav_concern":    55.0,
    "non_accrual_watch":      1.5,
    "non_accrual_concern":    3.0,
    "rl_accel_watch":         2.0,   # x prior year
    "rl_accel_concern":       4.0,
    "cum_loss_watch":        50.0,   # % of NAV/share
    "cum_loss_concern":     100.0,
}

def signal(value, watch, concern, low_is_bad=False):
    """Return 🟢/🟡/🔴 based on whether value crosses watch/concern thresholds."""
    if value is None:
        return "⚪ N/A"
    if low_is_bad:
        if value <= concern: return "🔴 CONCERN"
        if value <= watch:   return "🟡 WATCH"
        return "🟢 OK"
    else:
        if value >= concern: return "🔴 CONCERN"
        if value >= watch:   return "🟡 WATCH"
        return "🟢 OK"

# COMMAND ----------

# ── Tier 1: PIK-to-NII Ratio ───────────────────────────────────────────────────

print("=" * 65)
print("TIER 1 — PIK-to-NII Ratio")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    pik = facts.time_series("us-gaap:InterestIncomeOperatingPaidInKind")
    nii = facts.time_series("us-gaap:NetInvestmentIncome")

    if pik is None or nii is None:
        print(f"\n{ticker}: PIK or NII data not available")
        continue

    pik_fy = pik[pik["fiscal_period"] == "FY"].sort_values("period_end")
    nii_fy = nii[nii["fiscal_period"] == "FY"].sort_values("period_end")

    if pik_fy.empty or nii_fy.empty:
        print(f"\n{ticker}: no FY rows for PIK or NII")
        continue

    merged = pd.merge(
        pik_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "pik"}),
        nii_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "nii"}),
        on="period_end"
    )
    merged["pik_pct"] = (merged["pik"] / merged["nii"] * 100).round(1)

    latest_pct = merged["pik_pct"].iloc[-1] if len(merged) else None
    s = signal(latest_pct, THRESHOLDS["pik_nii_watch"], THRESHOLDS["pik_nii_concern"])
    print(f"\n{ticker} — Latest PIK/NII: {latest_pct}%  [{s}]")
    print(f"  Watch >{THRESHOLDS['pik_nii_watch']}%  |  Concern >{THRESHOLDS['pik_nii_concern']}%")
    display(merged[["period_end", "pik", "nii", "pik_pct"]])

# COMMAND ----------

# ── Tier 1: Dividend Coverage ──────────────────────────────────────────────────
# Article uses InvestmentCompanyInvestmentIncomeLossPerShare for NII per share —
# this is the investment-company-specific per-share NII concept, more accurate
# than dividing total NII by shares for BDCs.

print("=" * 65)
print("TIER 1 — Dividend Coverage (NII per share / dividends declared)")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    nii_ps = facts.time_series("us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare")
    div_ps = facts.time_series("us-gaap:CommonStockDividendsPerShareDeclared")

    if nii_ps is None or div_ps is None:
        print(f"\n{ticker}: NII per share or dividend data not available")
        continue

    nii_fy  = nii_ps[nii_ps["fiscal_period"] == "FY"].sort_values("period_end")
    div_fy  = div_ps[div_ps["fiscal_period"] == "FY"].sort_values("period_end")

    if nii_fy.empty or div_fy.empty:
        print(f"\n{ticker}: no FY rows")
        continue

    merged = pd.merge(
        nii_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "nii_ps"}),
        div_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "div_ps"}),
        on="period_end"
    )
    merged["coverage_pct"] = (merged["nii_ps"] / merged["div_ps"] * 100).round(1)

    latest = merged["coverage_pct"].iloc[-1] if len(merged) else None
    s = signal(latest, THRESHOLDS["div_coverage_watch"], THRESHOLDS["div_coverage_concern"], low_is_bad=True)
    print(f"\n{ticker} — Latest coverage: {latest}%  [{s}]")
    print(f"  Watch <{THRESHOLDS['div_coverage_watch']}%  |  Concern <{THRESHOLDS['div_coverage_concern']}%")
    display(merged[["period_end", "nii_ps", "div_ps", "coverage_pct"]])

# COMMAND ----------

# ── Tier 2: NAV Trajectory ─────────────────────────────────────────────────────

print("=" * 65)
print("TIER 2 — NAV Trajectory (consecutive quarterly declines)")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    nav = facts.time_series("us-gaap:NetAssetValuePerShare")
    if nav is None:
        print(f"\n{ticker}: NAV per share not available")
        continue

    nav = nav.drop_duplicates(subset=["period_end"]).sort_values("period_end")
    vals = nav["numeric_value"].tolist()

    # Count consecutive declines from most recent quarter backwards
    consec = 0
    for i in range(len(vals) - 1, 0, -1):
        if vals[i] < vals[i - 1]: consec += 1
        else: break

    s = signal(consec, THRESHOLDS["nav_decline_watch"], THRESHOLDS["nav_decline_concern"])
    print(f"\n{ticker} — Consecutive declining quarters: {consec}  [{s}]")
    print(f"  Watch {THRESHOLDS['nav_decline_watch']}+  |  Concern {THRESHOLDS['nav_decline_concern']}+")
    display(nav[["period_end", "numeric_value"]].tail(12).rename(columns={"numeric_value": "nav_per_share"}))

# COMMAND ----------

# ── Tier 2: Unrealized Depreciation / NAV ─────────────────────────────────────
# Article uses TaxBasisOfInvestmentsGrossUnrealizedDepreciation / AssetsNet.
# Case study: TCPC went from $143M → $489M unrealized depreciation over stress period.

print("=" * 65)
print("TIER 2 — Unrealized Depreciation / NAV")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    dep    = facts.time_series("us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation")
    assets = facts.time_series("us-gaap:AssetsNet")

    if dep is None or assets is None:
        print(f"\n{ticker}: depreciation or net assets not available")
        continue

    dep_fy    = dep[dep["fiscal_period"] == "FY"].drop_duplicates("period_end").sort_values("period_end")
    assets_fy = assets[assets["fiscal_period"] == "FY"].drop_duplicates("period_end").sort_values("period_end")

    if dep_fy.empty or assets_fy.empty:
        print(f"\n{ticker}: no FY rows")
        continue

    merged = pd.merge(
        dep_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "depreciation"}),
        assets_fy[["period_end", "numeric_value"]].rename(columns={"numeric_value": "net_assets"}),
        on="period_end"
    )
    merged["pct_of_nav"] = (merged["depreciation"] / merged["net_assets"] * 100).round(1)

    latest = merged["pct_of_nav"].iloc[-1] if len(merged) else None
    s = signal(latest, THRESHOLDS["deprec_nav_watch"], THRESHOLDS["deprec_nav_concern"])
    print(f"\n{ticker} — Latest depreciation/NAV: {latest}%  [{s}]")
    print(f"  Watch >{THRESHOLDS['deprec_nav_watch']}%  |  Concern >{THRESHOLDS['deprec_nav_concern']}%")
    display(merged)

# COMMAND ----------

# ── Tier 3: Realized Loss Acceleration ────────────────────────────────────────
# Track YoY growth in realized losses. Positive = losses getting larger.
# Case study: TCPC realized losses grew $67M → $278M (>4x) over stress period.

print("=" * 65)
print("TIER 3 — Realized Loss Acceleration (YoY)")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    rl = facts.time_series("us-gaap:RealizedInvestmentGainsLosses")
    if rl is None:
        print(f"\n{ticker}: realized gains/losses not available")
        continue

    rl_fy = rl[rl["fiscal_period"] == "FY"].drop_duplicates("period_end").sort_values("period_end").copy()
    # Losses are negative values; flip sign so bigger = worse
    rl_fy["loss_abs"] = rl_fy["numeric_value"].clip(upper=0).abs()
    rl_fy["yoy_multiple"] = (rl_fy["loss_abs"] / rl_fy["loss_abs"].shift(1)).round(2)

    latest_mult = rl_fy["yoy_multiple"].iloc[-1] if len(rl_fy) > 1 else None
    s = signal(latest_mult, THRESHOLDS["rl_accel_watch"], THRESHOLDS["rl_accel_concern"])
    print(f"\n{ticker} — Latest YoY loss multiple: {latest_mult}x  [{s}]")
    print(f"  Watch >{THRESHOLDS['rl_accel_watch']}x  |  Concern >{THRESHOLDS['rl_accel_concern']}x")
    display(rl_fy[["period_end", "numeric_value", "loss_abs", "yoy_multiple"]].tail(6))

# COMMAND ----------

# ── Tier 3: Cumulative Losses vs. Book Value ───────────────────────────────────
# Sum of per-share gain/loss over time vs. current NAV per share.
# When cumulative losses exceed NAV, book value has been materially impaired.

print("=" * 65)
print("TIER 3 — Cumulative Losses vs. Book Value (NAV/share)")
print("=" * 65)

for ticker in BDC_TICKERS:
    facts = Company(ticker).get_facts()

    gl_ps = facts.time_series("us-gaap:InvestmentCompanyGainLossOnInvestmentPerShare")
    nav   = facts.time_series("us-gaap:NetAssetValuePerShare")

    if gl_ps is None or nav is None:
        print(f"\n{ticker}: gain/loss per share or NAV not available")
        continue

    gl_fy  = gl_ps[gl_ps["fiscal_period"] == "FY"].drop_duplicates("period_end").sort_values("period_end").copy()
    nav_fy = nav.drop_duplicates("period_end").sort_values("period_end")

    gl_fy["cum_loss_ps"] = gl_fy["numeric_value"].clip(upper=0).cumsum().abs()
    latest_nav = nav_fy["numeric_value"].iloc[-1] if not nav_fy.empty else None

    if latest_nav and not gl_fy.empty:
        gl_fy["cum_loss_pct_nav"] = (gl_fy["cum_loss_ps"] / latest_nav * 100).round(1)
        latest_pct = gl_fy["cum_loss_pct_nav"].iloc[-1]
        s = signal(latest_pct, THRESHOLDS["cum_loss_watch"], THRESHOLDS["cum_loss_concern"])
        print(f"\n{ticker} — Cumulative losses as % of current NAV: {latest_pct}%  [{s}]")
        print(f"  Watch >{THRESHOLDS['cum_loss_watch']}%  |  Concern >{THRESHOLDS['cum_loss_concern']}%")
        display(gl_fy[["period_end", "numeric_value", "cum_loss_ps", "cum_loss_pct_nav"]].tail(6))

# COMMAND ----------

# ── Full scanner — all signals in one pass (article Example 5) ─────────────────

def scan_bdc(ticker: str) -> dict:
    """Scan a BDC for all 7 warning signals. Returns a dict of computed values."""
    facts   = Company(ticker).get_facts()
    signals = {"ticker": ticker}

    # T1: PIK ratio
    pik = facts.time_series("us-gaap:InterestIncomeOperatingPaidInKind")
    nii = facts.time_series("us-gaap:NetInvestmentIncome")
    if pik is not None and nii is not None:
        pik_fy = pik[pik["fiscal_period"] == "FY"].sort_values("period_end")
        nii_fy = nii[nii["fiscal_period"] == "FY"].sort_values("period_end")
        if len(pik_fy) and len(nii_fy):
            signals["pik_pct"] = round(
                float(pik_fy["numeric_value"].iloc[-1] / nii_fy["numeric_value"].iloc[-1]) * 100, 1
            )

    # T1: Dividend coverage
    nii_ps = facts.time_series("us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare")
    div_ps = facts.time_series("us-gaap:CommonStockDividendsPerShareDeclared")
    if nii_ps is not None and div_ps is not None:
        nii_fy2 = nii_ps[nii_ps["fiscal_period"] == "FY"].sort_values("period_end")
        div_fy2 = div_ps[div_ps["fiscal_period"] == "FY"].sort_values("period_end")
        if len(nii_fy2) and len(div_fy2):
            signals["div_coverage_pct"] = round(
                float(nii_fy2["numeric_value"].iloc[-1] / div_fy2["numeric_value"].iloc[-1]) * 100, 1
            )

    # T2: NAV consecutive declines
    nav = facts.time_series("us-gaap:NetAssetValuePerShare")
    if nav is not None:
        vals = nav.drop_duplicates("period_end").sort_values("period_end")["numeric_value"].tolist()
        consec = 0
        for i in range(len(vals) - 1, 0, -1):
            if vals[i] < vals[i - 1]: consec += 1
            else: break
        signals["nav_consec_declines"] = consec

    # T2: Unrealized depreciation / NAV
    dep    = facts.time_series("us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation")
    assets = facts.time_series("us-gaap:AssetsNet")
    if dep is not None and assets is not None:
        dep_fy    = dep[dep["fiscal_period"] == "FY"].sort_values("period_end")
        assets_fy = assets[assets["fiscal_period"] == "FY"].sort_values("period_end")
        if len(dep_fy) and len(assets_fy):
            signals["deprec_pct_nav"] = round(
                float(dep_fy["numeric_value"].iloc[-1] / assets_fy["numeric_value"].iloc[-1]) * 100, 1
            )

    # T3: Realized loss YoY multiple
    rl = facts.time_series("us-gaap:RealizedInvestmentGainsLosses")
    if rl is not None:
        rl_fy = rl[rl["fiscal_period"] == "FY"].sort_values("period_end")
        losses = rl_fy["numeric_value"].clip(upper=0).abs().tolist()
        if len(losses) >= 2 and losses[-2] > 0:
            signals["rl_yoy_multiple"] = round(losses[-1] / losses[-2], 2)

    return signals


print("=" * 65)
print("FULL BDC SCANNER — all signals")
print("=" * 65)

rows = []
for ticker in BDC_TICKERS:
    try:
        result = scan_bdc(ticker)
        rows.append(result)
        print(f"  {ticker}: done")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

summary = pd.DataFrame(rows).set_index("ticker")

# Apply signal labels
def label(col, watch, concern, low_is_bad=False):
    def _f(v):
        if pd.isna(v): return "⚪"
        if low_is_bad:
            if v <= concern: return f"{v} 🔴"
            if v <= watch:   return f"{v} 🟡"
            return f"{v} 🟢"
        else:
            if v >= concern: return f"{v} 🔴"
            if v >= watch:   return f"{v} 🟡"
            return f"{v} 🟢"
    if col in summary.columns:
        summary[col] = summary[col].apply(_f)

label("pik_pct",            THRESHOLDS["pik_nii_watch"],        THRESHOLDS["pik_nii_concern"])
label("div_coverage_pct",   THRESHOLDS["div_coverage_watch"],   THRESHOLDS["div_coverage_concern"],   low_is_bad=True)
label("nav_consec_declines",THRESHOLDS["nav_decline_watch"],    THRESHOLDS["nav_decline_concern"])
label("deprec_pct_nav",     THRESHOLDS["deprec_nav_watch"],     THRESHOLDS["deprec_nav_concern"])
label("rl_yoy_multiple",    THRESHOLDS["rl_accel_watch"],       THRESHOLDS["rl_accel_concern"])

display(summary.T)

# COMMAND ----------

# ── Bonus: edgartools BDC registry + bulk portfolio dataset ───────────────────
# The edgartools library has a built-in BDC module that tracks all SEC-registered
# BDCs and can fetch the full Schedule of Investments in one call.

from edgar.bdc import get_bdc_list, fetch_bdc_dataset

# All BDCs tracked by the SEC
bdcs = get_bdc_list()
print(f"BDCs in SEC registry: {len(bdcs)}")
display(bdcs.head(10))

# COMMAND ----------

# Full portfolio dataset for a given quarter — 100k+ line items across all BDCs.
# Useful for: cross-BDC exposure analysis, concentration risk, co-investment patterns.
dataset = fetch_bdc_dataset(2024, 4)

soi = dataset.schedule_of_investments
top = soi.top_companies(25)
print("Top 25 portfolio companies by BDC count:")
display(top)
