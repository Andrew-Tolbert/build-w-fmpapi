# Databricks notebook source
# Pull BDC early warning XBRL data from SEC EDGAR via edgartools.
# CIK resolved per ticker using FMP /stable/profile (reuses existing client).
#
# Output (UC Volume — directory cleared on every run):
#   bdc_early_warning/bdc_time_series.csv   — long format, every data point
#   bdc_early_warning/bdc_fy_snapshot.csv   — wide format, latest FY per metric
#
# year_quarter column format: "2024-Q3" / "2024-FY"
# Uses fiscal_year from edgartools when present (handles non-calendar FY ends:
#   AINV fiscal year ends March, OCSL September, TCPC December).
#
# EDGAR concepts collected:
#   pik         InterestIncomeOperatingPaidInKind              (T1: PIK income)
#   nii         NetInvestmentIncome                            (T1: net investment income)
#   nii_ps      InvestmentCompanyInvestmentIncomeLossPerShare  (T1: NII/share)
#   div_ps      CommonStockDividendsPerShareDeclared           (T1: dividends/share)
#   nav_ps      NetAssetValuePerShare                          (T2: NAV trajectory)
#   deprec      TaxBasisOfInvestmentsGrossUnrealizedDepreciation (T2: unrealized losses)
#   net_assets  AssetsNet                                      (T2: NAV denominator)
#   realized_gl RealizedInvestmentGainsLosses                  (T3: realized loss accel)
#   gl_ps       InvestmentCompanyGainLossOnInvestmentPerShare  (T3: cumulative losses)

# COMMAND ----------

# MAGIC %run ../../utils/ingest_config

# COMMAND ----------

# MAGIC %run ../../utils/fmp_client

# COMMAND ----------

import os
import pandas as pd
from edgar import Company, set_identity

set_identity("andrew.tolbert@databricks.com")

client = FMPClient(api_key=FMP_API_KEY)

CONCEPTS = {
    "pik":         "us-gaap:InterestIncomeOperatingPaidInKind",
    "nii":         "us-gaap:NetInvestmentIncome",
    "nii_ps":      "us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare",
    "div_ps":      "us-gaap:CommonStockDividendsPerShareDeclared",
    "nav_ps":      "us-gaap:NetAssetValuePerShare",
    "deprec":      "us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation",
    "net_assets":  "us-gaap:AssetsNet",
    "realized_gl": "us-gaap:RealizedInvestmentGainsLosses",
    "gl_ps":       "us-gaap:InvestmentCompanyGainLossOnInvestmentPerShare",
}

out_dir = volume_subdir("bdc_early_warning")
clear_directory(out_dir)
os.makedirs(out_dir, exist_ok=True)

# COMMAND ----------

rows = []
for ticker in get_tickers(types=["private_credit"]):
    try:
        cik   = client.get_cik(ticker)
        facts = Company(int(cik)).get_facts()
        for metric, concept in CONCEPTS.items():
            ts = facts.time_series(concept)
            if ts is None or ts.empty:
                continue
            ts = ts.copy()
            ts["ticker"] = ticker
            ts["cik"]    = cik
            ts["metric"] = metric
            rows.append(ts)
        print(f"  {ticker} ({cik}): ok")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

raw = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()
print(f"\nTotal rows: {len(raw)}")
print(f"Columns available from edgartools: {list(raw.columns)}")

# COMMAND ----------

# ── Build year_quarter column ──────────────────────────────────────────────────
# edgartools returns fiscal_year when available — prefer it over calendar year
# derived from period_end so non-calendar fiscal years (AINV=March, OCSL=Sep)
# produce the correct label (e.g. AINV FY ending Mar-2025 → "2025-FY" not "2024-FY").

raw["period_end"] = pd.to_datetime(raw["period_end"])

if "fiscal_year" in raw.columns:
    extracted = raw["fiscal_year"].astype(str).str.extract(r"(\d{4})")[0]
    fallback = raw["period_end"].dt.year
    raw["year"] = extracted.fillna(fallback.astype(str)).astype(int)
else:
    raw["year"] = raw["period_end"].dt.year

raw["year_quarter"] = raw["year"].astype(str) + "-" + raw["fiscal_period"].astype(str)

# COMMAND ----------

# ── bdc_time_series — long format, every data point ───────────────────────────

keep_cols = ["ticker", "cik", "metric", "period_end", "fiscal_period",
             "year_quarter", "numeric_value"]
# Also keep accession_number / form if edgartools provided them (useful for SEC filing joins)
for optional in ["accession_number", "form", "period_start"]:
    if optional in raw.columns:
        keep_cols.append(optional)

bdc_time_series = raw[keep_cols].copy()
bdc_time_series["ingested_at"] = pd.Timestamp.now().isoformat()

# ── bdc_fy_snapshot — wide format, latest FY value per ticker/metric ───────────

fy_raw = (
    raw[raw["fiscal_period"] == "FY"]
    .sort_values("period_end")
    .drop_duplicates(subset=["ticker", "metric"], keep="last")
)

bdc_fy_snapshot = (
    fy_raw
    .pivot(index=["ticker", "cik", "year_quarter"], columns="metric", values="numeric_value")
    .reset_index()
)
bdc_fy_snapshot.columns.name = None

# Merge optional filing columns back from the most recent FY row per ticker
optional_cols = [c for c in ["accession_number", "form", "period_start"] if c in fy_raw.columns]
if optional_cols:
    filing_meta = (
        fy_raw.sort_values("period_end")
        .drop_duplicates(subset=["ticker"], keep="last")
        [["ticker", "cik"] + optional_cols]
    )
    bdc_fy_snapshot = bdc_fy_snapshot.merge(filing_meta, on=["ticker", "cik"], how="left")

bdc_fy_snapshot["ingested_at"] = pd.Timestamp.now().isoformat()

# ── Write CSVs ─────────────────────────────────────────────────────────────────

ts_path   = f"{out_dir}/bdc_time_series.csv"
snap_path = f"{out_dir}/bdc_fy_snapshot.csv"

bdc_time_series.to_csv(ts_path,   index=False)
bdc_fy_snapshot.to_csv(snap_path, index=False)

print(f"Written: {ts_path}   ({len(bdc_time_series)} rows)")
print(f"Written: {snap_path} ({len(bdc_fy_snapshot)} rows)")

# COMMAND ----------

display(bdc_time_series.groupby(["ticker", "metric", "fiscal_period"]).size().unstack(fill_value=0))

# COMMAND ----------

display(bdc_fy_snapshot)
