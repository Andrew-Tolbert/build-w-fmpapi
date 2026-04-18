# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "requests",
# ]
# ///
# BDC Early Warning System — scratchpad analysis
#
# Replicates the framework from:
#   https://www.edgartools.io/building-a-bdc-early-warning-system-in-python/
#
# CIK lookup: FMPClient.get_cik() — pulls from /stable/profile (uses FMP_API_KEY)
# Financial data: SEC EDGAR XBRL free APIs — no additional key required.
#   data.sec.gov/api/xbrl/companyfacts/{CIK}.json  → all XBRL financial facts
#   data.sec.gov/submissions/CIK{CIK}.json         → filing metadata
#
# BDC tickers analysed (from TICKER_CONFIG, type="private_credit"):
#   AINV — Apollo Investment Corp     (fiscal year ends March)
#   OCSL — Oaktree Specialty Lending  (fiscal year ends September)
#
# ── WARNING SIGNAL FRAMEWORK ──────────────────────────────────────────────────
#
# Tier 1 — Leading Indicators (earliest warning)
#   1. PIK-to-NII Ratio         warn  > 20%  |  critical > 30%
#   2. Dividend Coverage (NII)  alert < 105% |  critical < 100%
#
# Tier 2 — Confirming Indicators
#   3. NAV Trajectory           flag on 3+ consecutive quarterly declines
#   4. Unrealized Depreciation / NAV   monitor trend; flag when worsening
#   5. Non-Accrual Rate         flag > 3% of portfolio fair value
#
# Tier 3 — Lagging Indicators
#   6. Realized Loss Acceleration  flag when QoQ pace is accelerating
#   7. Cumulative Losses / Book Value  flag > 5% in trailing 4 quarters
#
# ── XBRL AVAILABILITY NOTE ────────────────────────────────────────────────────
# All 7 signals use EDGAR XBRL data except one:
#   PIK income  — tagged as us-gaap:InterestIncomeOperatingPaidInKind (XBRL ✓)
#   Non-Accrual — NOT in standard XBRL taxonomy; disclosed in the Schedule of
#                 Investments footnotes and requires filing text parsing.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../utils/fmp_client

# COMMAND ----------

import requests
import pandas as pd

HEADERS = {"User-Agent": "andrew.tolbert@databricks.com"}  # SEC requires a real User-Agent

client     = FMPClient(api_key=FMP_API_KEY)
BDC_TICKERS = get_tickers(types=["private_credit"])

# ── Warning thresholds ─────────────────────────────────────────────────────────
THRESHOLDS = {
    "pik_nii_warn":            0.20,
    "pik_nii_critical":        0.30,
    "div_coverage_alert":      1.05,
    "div_coverage_critical":   1.00,
    "non_accrual_flag":        0.03,
    "cum_loss_book_flag":      0.05,
    "nav_decline_quarters":    3,
}

# COMMAND ----------

# ── CIK lookup via FMP profile ─────────────────────────────────────────────────
# FMPClient.get_cik() calls /stable/profile and extracts the cik field,
# zero-padded to 10 digits as EDGAR expects.

for t in BDC_TICKERS:
    print(f"{t}: CIK {client.get_cik(t)}")

# COMMAND ----------

# ── Pull submissions (company metadata + recent filing index) ──────────────────

def get_submissions(ticker: str) -> dict:
    url = f"https://data.sec.gov/submissions/CIK{client.get_cik(ticker)}.json"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

subs = {t: get_submissions(t) for t in BDC_TICKERS}
for t, s in subs.items():
    print(f"{t}: {s['name']} | fiscal year end: {s['fiscalYearEnd']} | SIC: {s['sic']}")

# COMMAND ----------

# ── Pull all XBRL facts ────────────────────────────────────────────────────────

def get_company_facts(ticker: str) -> dict:
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{client.get_cik(ticker)}.json"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json()

facts = {t: get_company_facts(t) for t in BDC_TICKERS}
for t, f in facts.items():
    n = len(f["facts"].get("us-gaap", {}))
    print(f"{t}: {n} US-GAAP concepts available")

# COMMAND ----------

# ── Core extractor: pull a XBRL concept across quarters and annual filings ──────
# Includes both 10-K and 10-Q rows; period type inferred from the 'form' column.

def extract_concept(facts_dict: dict, concept: str, unit: str = "USD") -> pd.DataFrame:
    node = facts_dict["facts"].get("us-gaap", {}).get(concept)
    if node is None:
        return pd.DataFrame()
    rows = node.get("units", {}).get(unit, [])
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df[df["form"].isin(["10-K", "10-Q"])].copy()
    df["end"] = pd.to_datetime(df["end"])
    if "start" in df.columns:
        df["start"] = pd.to_datetime(df["start"])
    # For flow items (income, gains) keep only period-length rows matching the form:
    #   10-K  → ~365-day period; 10-Q → ~90-day period (point-in-time items have no start)
    if "start" in df.columns and df["start"].notna().any():
        df["days"] = (df["end"] - df["start"]).dt.days
        df = df[
            ((df["form"] == "10-K") & (df["days"].between(300, 400))) |
            ((df["form"] == "10-Q") & (df["days"].between(75, 110)))
        ]
    df = df.sort_values("end").drop_duplicates(subset=["end", "form"], keep="last")
    df["ticker"] = None  # filled by caller
    return df[["end", "val", "form", "accn", "concept"] +
              (["start", "days"] if "start" in df.columns and df["start"].notna().any() else [])]

def pull(ticker: str, concept: str, unit: str = "USD") -> pd.DataFrame:
    df = extract_concept(facts[ticker], concept, unit)
    df["ticker"] = ticker
    return df

# COMMAND ----------

# ── Explore what BDC-relevant concepts are actually filed ──────────────────────
# Investment-company GAAP uses different tags than industrial GAAP.

BDC_CONCEPTS_TO_CHECK = [
    # Net Investment Income
    "NetInvestmentIncome",
    # NAV
    "NetAssets",
    "NetAssetValuePerShare",
    # Dividends
    "CommonStockDividendsPerShareDeclared",
    "DividendsPaidCommonStock",
    # Realized gains/losses
    "NetRealizedGainLossOnInvestments",
    "RealizedInvestmentGainsLosses",
    "GainLossOnSaleOfInvestments",
    # Unrealized appreciation/depreciation (change for the period)
    "NetChangeInUnrealizedAppreciationDepreciationOnInvestments",
    "UnrealizedGainLossOnInvestments",
    # Balance sheet
    "Assets",
    "StockholdersEquity",
    "Investments",
    # Shares
    "CommonStockSharesOutstanding",
    # Interest income components (proxy for PIK estimation)
    "InvestmentIncomeInterest",
    "InvestmentIncomeDividend",
    # PIK — the correct US-GAAP XBRL concept used by BDCs
    "InterestIncomeOperatingPaidInKind",
    "PaymentInKindInterest",              # older/alternate tag
]

print("Concept availability by ticker:\n")
print(f"{'Concept':<55} {'AINV':>10} {'OCSL':>10}")
print("-" * 78)
for c in BDC_CONCEPTS_TO_CHECK:
    avail = {}
    for t in BDC_TICKERS:
        node = facts[t]["facts"].get("us-gaap", {}).get(c)
        if node:
            unit_keys = list(node.get("units", {}).keys())
            n = sum(len(v) for v in node["units"].values())
            avail[t] = f"YES ({n})"
        else:
            avail[t] = "—"
    print(f"  {c:<53} {avail['AINV']:>10} {avail['OCSL']:>10}")

# COMMAND ----------

# ── Pull core BDC metrics for both tickers ─────────────────────────────────────

# Adjust concept names based on what's available per ticker (check cell above first)
# These are best-guess tags; validate against the availability table.

CONCEPT_MAP = {
    "nii":             ("NetInvestmentIncome",                              "USD"),
    "nav_total":       ("NetAssets",                                        "USD"),
    "nav_per_share":   ("NetAssetValuePerShare",                            "USD/shares"),
    "div_per_share":   ("CommonStockDividendsPerShareDeclared",             "USD/shares"),
    "realized_gl":     ("NetRealizedGainLossOnInvestments",                 "USD"),
    "unrealized_chg":  ("NetChangeInUnrealizedAppreciationDepreciationOnInvestments", "USD"),
    "total_assets":    ("Assets",                                           "USD"),
    "equity":          ("StockholdersEquity",                               "USD"),
    "shares_out":      ("CommonStockSharesOutstanding",                     "shares"),
    "interest_income": ("InvestmentIncomeInterest",                         "USD"),
    # PIK — correct XBRL concept; falls back to older tag if empty
    "pik_income":      ("InterestIncomeOperatingPaidInKind",                "USD"),
}

all_data = {}
for ticker in BDC_TICKERS:
    all_data[ticker] = {}
    for label, (concept, unit) in CONCEPT_MAP.items():
        df = pull(ticker, concept, unit)
        all_data[ticker][label] = df
        n = len(df)
        print(f"  {ticker} / {label:<18}: {n} rows {'✓' if n > 0 else '✗ MISSING'}")
    print()

# COMMAND ----------

# ── Tier 2: NAV Trajectory ─────────────────────────────────────────────────────
# Track quarterly NAV per share; flag 3+ consecutive declining quarters.

print("=" * 70)
print("TIER 2 — NAV TRAJECTORY")
print("=" * 70)

for ticker in BDC_TICKERS:
    nav_ps = all_data[ticker]["nav_per_share"]
    if nav_ps.empty:
        # Fall back to total NAV / shares outstanding
        nav_t  = all_data[ticker]["nav_total"]
        shares = all_data[ticker]["shares_out"]
        if nav_t.empty or shares.empty:
            print(f"\n{ticker}: insufficient NAV data\n")
            continue
        merged = pd.merge(nav_t[["end","val"]].rename(columns={"val":"nav_total"}),
                          shares[["end","val"]].rename(columns={"val":"shares"}),
                          on="end", how="inner")
        merged["nav_per_share"] = merged["nav_total"] / merged["shares"]
    else:
        merged = nav_ps[["end","val"]].rename(columns={"val":"nav_per_share"})

    merged = merged.sort_values("end").tail(12)  # last 12 periods
    merged["qoq_chg_pct"] = merged["nav_per_share"].pct_change() * 100
    merged["declining"]   = merged["nav_per_share"] < merged["nav_per_share"].shift(1)

    # Count consecutive declines from most recent backwards
    consec = 0
    for d in reversed(merged["declining"].tolist()):
        if d: consec += 1
        else: break

    flag = "🔴 FLAG" if consec >= THRESHOLDS["nav_decline_quarters"] else "🟢 OK"
    print(f"\n{ticker} — NAV per share trend (last {len(merged)} periods): {flag}")
    print(f"  Consecutive declining quarters: {consec}")
    display(merged[["end","nav_per_share","qoq_chg_pct"]].tail(8).round(4))

# COMMAND ----------

# ── Tier 1: Dividend Coverage ──────────────────────────────────────────────────
# Coverage = NII / Dividends declared.  Alert < 1.05, Critical < 1.00.
#
# NII per share = NII / weighted shares (or use NII directly vs. div_per_share * shares)

print("=" * 70)
print("TIER 1 — DIVIDEND COVERAGE (NII / Dividends)")
print("=" * 70)

for ticker in BDC_TICKERS:
    nii   = all_data[ticker]["nii"]
    divps = all_data[ticker]["div_per_share"]
    shrs  = all_data[ticker]["shares_out"]

    if nii.empty or divps.empty:
        print(f"\n{ticker}: missing NII or dividend data\n")
        continue

    # Compute NII per share using closest shares figure
    nii_merged = pd.merge_asof(
        nii[["end","val"]].rename(columns={"val":"nii"}),
        shrs[["end","val"]].rename(columns={"val":"shares"}),
        on="end", direction="nearest"
    )
    nii_merged["nii_per_share"] = nii_merged["nii"] / nii_merged["shares"]

    cov = pd.merge_asof(
        nii_merged[["end","nii","shares","nii_per_share"]],
        divps[["end","val"]].rename(columns={"val":"div_per_share"}),
        on="end", direction="nearest"
    )
    cov["coverage"] = cov["nii_per_share"] / cov["div_per_share"]
    cov = cov.sort_values("end").tail(8)

    latest_cov = cov.iloc[-1]["coverage"]
    if latest_cov < THRESHOLDS["div_coverage_critical"]:
        signal = "🔴 CRITICAL"
    elif latest_cov < THRESHOLDS["div_coverage_alert"]:
        signal = "🟡 ALERT"
    else:
        signal = "🟢 OK"

    print(f"\n{ticker} — Latest coverage: {latest_cov:.3f}x  [{signal}]")
    print(f"  Thresholds: alert < {THRESHOLDS['div_coverage_alert']:.0%}, "
          f"critical < {THRESHOLDS['div_coverage_critical']:.0%}")
    display(cov[["end","nii_per_share","div_per_share","coverage"]].round(4))

# COMMAND ----------

# ── Tier 2: Unrealized Depreciation / NAV ─────────────────────────────────────
# Ratio = cumulative unrealized change (when negative) / NAV total.
# Track trend — worsening ratio confirms stress signalled by PIK/dividend coverage.
#
# Note: unrealized_chg is the *period change*, not the cumulative balance.
# Cumulative unrealized position lives on the balance sheet; best proxy is the
# unrealized appreciation/depreciation line in stockholders equity or in the
# Schedule of Investments — not always XBRL-tagged as a standalone balance.
# Here we compute rolling cumulative from period changes as an approximation.

print("=" * 70)
print("TIER 2 — UNREALIZED DEPRECIATION / NAV")
print("=" * 70)

for ticker in BDC_TICKERS:
    uc    = all_data[ticker]["unrealized_chg"]
    nav_t = all_data[ticker]["nav_total"]

    if uc.empty or nav_t.empty:
        print(f"\n{ticker}: missing unrealized change or NAV data\n")
        continue

    merged = pd.merge_asof(
        uc[["end","val"]].rename(columns={"val":"unrealized_chg"}),
        nav_t[["end","val"]].rename(columns={"val":"nav_total"}),
        on="end", direction="nearest"
    ).sort_values("end")

    # Cumulative sum of period changes as a proxy for cumulative unrealized position
    merged["cum_unrealized"] = merged["unrealized_chg"].cumsum()
    merged["depr_pct_nav"]   = merged["cum_unrealized"] / merged["nav_total"] * 100

    print(f"\n{ticker} — Unrealized depreciation as % of NAV:")
    display(merged[["end","unrealized_chg","cum_unrealized","nav_total","depr_pct_nav"]].tail(8).round(2))

# COMMAND ----------

# ── Tier 3: Realized Loss Acceleration ────────────────────────────────────────
# Track cumulative realized losses. Flag when the pace (QoQ $ increase in losses)
# is accelerating. Also compute cumulative losses / book value (equity).

print("=" * 70)
print("TIER 3 — REALIZED LOSS ACCELERATION & CUMULATIVE LOSSES / BOOK VALUE")
print("=" * 70)

for ticker in BDC_TICKERS:
    rl  = all_data[ticker]["realized_gl"]
    eq  = all_data[ticker]["equity"]

    if rl.empty:
        print(f"\n{ticker}: no realized gain/loss data\n")
        continue

    rl_sorted = rl[["end","val"]].rename(columns={"val":"realized_gl"}).sort_values("end")
    rl_sorted["cum_realized"] = rl_sorted["realized_gl"].cumsum()
    rl_sorted["qoq_change"]   = rl_sorted["cum_realized"].diff()

    if not eq.empty:
        merged = pd.merge_asof(
            rl_sorted,
            eq[["end","val"]].rename(columns={"val":"equity"}),
            on="end", direction="nearest"
        )
        merged["cum_loss_pct_equity"] = (
            merged["cum_realized"].clip(upper=0).abs() / merged["equity"] * 100
        )
        # Flag: cumulative losses > 5% of book value in trailing 4 quarters
        trailing4 = rl_sorted.tail(4)["realized_gl"].sum()
        latest_eq  = eq.sort_values("end").iloc[-1]["val"]
        trailing_pct = abs(min(trailing4, 0)) / latest_eq * 100
        flag = "🔴 FLAG" if trailing_pct > THRESHOLDS["cum_loss_book_flag"] * 100 else "🟢 OK"
        print(f"\n{ticker} — Trailing-4Q realized losses as % of book: "
              f"{trailing_pct:.1f}%  [{flag}]")
        display(merged[["end","realized_gl","cum_realized","equity","cum_loss_pct_equity"]].tail(8).round(2))
    else:
        print(f"\n{ticker} — Realized GL trend (no equity data for ratio):")
        display(rl_sorted.tail(8).round(2))

# COMMAND ----------

# ── Tier 1: PIK-to-NII Ratio ──────────────────────────────────────────────────
# PIK income is XBRL-tagged as us-gaap:InterestIncomeOperatingPaidInKind.
# This is the same concept the edgartools library surfaces via:
#   facts.time_series("us-gaap:InterestIncomeOperatingPaidInKind")
# If a BDC hasn't adopted that tag, the cell falls back to printing filing URLs.

print("=" * 70)
print("TIER 1 — PIK-to-NII RATIO  (XBRL availability check + filing URLs)")
print("=" * 70)

for ticker in BDC_TICKERS:
    pik = all_data[ticker]["pik_income"]
    nii = all_data[ticker]["nii"]

    print(f"\n{ticker}:")
    if not pik.empty and not nii.empty:
        ratio_df = pd.merge_asof(
            pik[["end","val"]].rename(columns={"val":"pik"}),
            nii[["end","val"]].rename(columns={"val":"nii"}),
            on="end", direction="nearest"
        )
        ratio_df["pik_nii_pct"] = ratio_df["pik"] / ratio_df["nii"] * 100
        ratio_df = ratio_df.sort_values("end").tail(8)
        latest = ratio_df.iloc[-1]["pik_nii_pct"]
        if latest > THRESHOLDS["pik_nii_critical"] * 100:
            signal = "🔴 CRITICAL"
        elif latest > THRESHOLDS["pik_nii_warn"] * 100:
            signal = "🟡 WARN"
        else:
            signal = "🟢 OK"
        print(f"  PIK/NII latest: {latest:.1f}%  [{signal}]")
        display(ratio_df)
    else:
        print(f"  ⚠️  PIK income NOT in XBRL — must parse filing documents.")
        print(f"      Look for 'PIK', 'paid-in-kind', or 'non-cash interest' in:")
        print(f"      • Notes to Financial Statements (income statement footnotes)")
        print(f"      • Schedule of Investments (identifies PIK-generating positions)")
        print()
        print(f"  Recent 10-K / 10-Q filing URLs for {ticker}:")
        recent_filings = pd.DataFrame(subs[ticker]["filings"]["recent"])
        key = recent_filings[recent_filings["form"].isin(["10-K", "10-Q"])].head(6)
        cik_padded = client.get_cik(ticker)
        for _, row in key.iterrows():
            acc_clean = row["accessionNumber"].replace("-", "")
            doc_url   = (f"https://www.sec.gov/Archives/edgar/data/"
                         f"{int(cik_padded)}/{acc_clean}/{row['primaryDocument']}")
            print(f"    [{row['form']}] {row['filingDate']}  {doc_url}")

# COMMAND ----------

# ── Tier 2: Non-Accrual Rate — XBRL gap, filing URLs ─────────────────────────
# Non-accrual investments are disclosed in the Schedule of Investments or in
# a dedicated footnote. Not a standard XBRL concept. The two data points needed:
#   - Fair value of non-accrual investments
#   - Total portfolio fair value
# Both appear in the Schedule of Investments appended to each 10-Q/10-K.
#
# Threshold: non-accrual / total portfolio FV > 3% → flag.
# Case study: TCPC had 3.4% non-accrual rate when stress was confirmed.

print("=" * 70)
print("TIER 2 — NON-ACCRUAL RATE  (requires filing text parsing)")
print("=" * 70)

for ticker in BDC_TICKERS:
    print(f"\n{ticker}: non-accrual data must be extracted from filing HTML/XBRL.")
    print(f"  Flag threshold: > {THRESHOLDS['non_accrual_flag']:.0%} of total portfolio FV")
    print(f"  Extraction path: Schedule of Investments → rows where 'non-accrual' annotation")
    print(f"  Or search for the text 'non-accrual' / 'non-accruing' in each 10-Q.")
    investments_node = facts[ticker]["facts"].get("us-gaap", {}).get("Investments")
    if investments_node:
        unit_keys = list(investments_node.get("units", {}).keys())
        n = sum(len(v) for v in investments_node["units"].values())
        print(f"  Total 'Investments' XBRL rows (portfolio FV proxy): {n}  units: {unit_keys}")
        inv_df = pull(ticker, "Investments")
        display(inv_df.tail(6)[["end","val","form"]].round(0))

# COMMAND ----------

# ── Consolidated warning dashboard ────────────────────────────────────────────
# Roll up all computable signals into a single summary table.
# PIK and non-accrual are marked MANUAL — require filing text extraction.

print("=" * 70)
print("CONSOLIDATED EARLY WARNING DASHBOARD")
print("=" * 70)

def latest_val(df: pd.DataFrame, col: str = "val"):
    if df.empty: return None
    return df.sort_values("end").iloc[-1][col]

summary_rows = []
for ticker in BDC_TICKERS:
    # --- Dividend coverage ---
    nii_df  = all_data[ticker]["nii"]
    div_df  = all_data[ticker]["div_per_share"]
    shr_df  = all_data[ticker]["shares_out"]

    if not nii_df.empty and not div_df.empty and not shr_df.empty:
        nii_ps = latest_val(nii_df) / latest_val(shr_df)
        div_ps = latest_val(div_df)
        cov    = nii_ps / div_ps if div_ps else None
    else:
        cov = None

    # --- NAV trajectory ---
    nav_df = all_data[ticker]["nav_per_share"]
    if nav_df.empty:
        nav_df2 = all_data[ticker]["nav_total"]
        shr_df2 = all_data[ticker]["shares_out"]
        if not nav_df2.empty and not shr_df2.empty:
            merged = pd.merge_asof(
                nav_df2[["end","val"]].rename(columns={"val":"nav"}),
                shr_df2[["end","val"]].rename(columns={"val":"shr"}),
                on="end", direction="nearest"
            ).sort_values("end")
            merged["navps"] = merged["nav"] / merged["shr"]
            nav_series = merged["navps"].tolist()
        else:
            nav_series = []
    else:
        nav_series = nav_df.sort_values("end")["val"].tolist()

    consec_decl = 0
    for i in range(len(nav_series)-1, 0, -1):
        if nav_series[i] < nav_series[i-1]: consec_decl += 1
        else: break

    # --- Realized loss / book value ---
    rl_df = all_data[ticker]["realized_gl"]
    eq_df = all_data[ticker]["equity"]
    if not rl_df.empty and not eq_df.empty:
        trailing4_rl  = rl_df.sort_values("end").tail(4)["val"].sum()
        latest_eq_val = latest_val(eq_df)
        loss_pct      = abs(min(trailing4_rl, 0)) / latest_eq_val * 100 if latest_eq_val else None
    else:
        loss_pct = None

    def fmt_cov(v):
        if v is None: return "N/A"
        if v < THRESHOLDS["div_coverage_critical"]: return f"{v:.2f}x 🔴"
        if v < THRESHOLDS["div_coverage_alert"]:    return f"{v:.2f}x 🟡"
        return f"{v:.2f}x 🟢"

    def fmt_nav(n):
        if n >= THRESHOLDS["nav_decline_quarters"]: return f"{n}q 🔴"
        if n >= 2: return f"{n}q 🟡"
        return f"{n}q 🟢"

    def fmt_loss(v):
        if v is None: return "N/A"
        pct = THRESHOLDS["cum_loss_book_flag"] * 100
        if v > pct: return f"{v:.1f}% 🔴"
        if v > pct * 0.7: return f"{v:.1f}% 🟡"
        return f"{v:.1f}% 🟢"

    summary_rows.append({
        "ticker":                      ticker,
        "T1: PIK / NII":              "MANUAL — parse filing",
        "T1: Div Coverage":            fmt_cov(cov),
        "T2: NAV Consec Declines":     fmt_nav(consec_decl),
        "T2: Unrealized Depr / NAV":  "see above",
        "T2: Non-Accrual Rate":       "MANUAL — parse filing",
        "T3: Cum Loss / Book Value":   fmt_loss(loss_pct),
        "T3: RL Acceleration":        "see above",
    })

display(pd.DataFrame(summary_rows).set_index("ticker").T)

# COMMAND ----------

# ── Next steps: extracting PIK and non-accrual from filing HTML ────────────────
# Both require fetching and parsing the actual 10-Q/10-K document.
# Two approaches:
#
# Option A — edgartools library (https://github.com/dgunning/edgartools)
#   from edgar import Company
#   company = Company("AINV")
#   filings = company.get_filings(form="10-Q")
#   filing  = filings.latest()
#   # edgartools parses the XBRL instance document and Schedule of Investments
#   # to surface PIK and non-accrual data programmatically.
#
# Option B — raw EDGAR fetch + LLM extraction
#   1. Use filing URL from get_submissions() (see PIK cell above for URL pattern)
#   2. Fetch the HTML document with requests
#   3. Send the income statement and Schedule of Investments sections to an LLM
#      with a structured extraction prompt:
#      "Extract: (a) total PIK/paid-in-kind interest income, (b) total interest income,
#       (c) fair value of non-accrual investments, (d) total portfolio fair value."
#   4. Store the structured results in a Delta table for trend analysis.
#
# This notebook pulls all XBRL-available metrics. Add PIK + non-accrual extraction
# from filing text to complete the full 7-signal framework.
