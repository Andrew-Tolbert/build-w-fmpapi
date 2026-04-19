# Databricks notebook source
# Pull BDC early warning XBRL data from SEC EDGAR via edgartools.
# CIK resolved per ticker using FMP /stable/profile (reuses existing client).
#
# Output (UC Volume, overwritten on each run — no log table needed):
#   bdc_early_warning/bdc_time_series.csv   — long format, every data point
#   bdc_early_warning/bdc_fy_snapshot.csv   — wide format, latest FY per metric
#
# EDGAR concepts collected:
#   pik         InterestIncomeOperatingPaidInKind      (T1: PIK income)
#   nii         NetInvestmentIncome                    (T1: net investment income)
#   nii_ps      InvestmentCompanyInvestmentIncomeLossPerShare  (T1: NII/share)
#   div_ps      CommonStockDividendsPerShareDeclared   (T1: dividends/share)
#   nav_ps      NetAssetValuePerShare                  (T2: NAV trajectory)
#   deprec      TaxBasisOfInvestmentsGrossUnrealizedDepreciation (T2: unrealized losses)
#   net_assets  AssetsNet                              (T2: NAV denominator)
#   realized_gl RealizedInvestmentGainsLosses          (T3: realized loss accel)
#   gl_ps       InvestmentCompanyGainLossOnInvestmentPerShare   (T3: cumulative losses)

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

# COMMAND ----------

rows = []
for ticker in get_tickers(types=["private_credit"]):
    try:
        cik   = client.get_cik(ticker)
        facts = Company(int(cik)).get_facts()
        for metric, concept in CONCEPTS.items():
            ts = facts.time_series(concept)
            if ts is not None and not ts.empty:
                ts = ts[["period_end", "fiscal_period", "numeric_value"]].copy()
                ts["ticker"] = ticker
                ts["cik"]    = cik
                ts["metric"] = metric
                rows.append(ts)
        print(f"  {ticker} ({cik}): ok")
    except Exception as e:
        print(f"  {ticker}: ERROR — {e}")

raw = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame(
    columns=["ticker", "cik", "metric", "period_end", "fiscal_period", "numeric_value"]
)
print(f"\nTotal rows collected: {len(raw)}")

# COMMAND ----------

# bdc_time_series — long format, every data point
bdc_time_series = raw[["ticker", "cik", "metric", "period_end", "fiscal_period", "numeric_value"]].copy()
bdc_time_series["ingested_at"] = pd.Timestamp.now().isoformat()

# bdc_fy_snapshot — wide format, latest FY value per ticker/metric
bdc_fy_snapshot = (
    raw[raw["fiscal_period"] == "FY"]
    .sort_values("period_end")
    .drop_duplicates(subset=["ticker", "metric"], keep="last")
    .pivot(index=["ticker", "cik"], columns="metric", values="numeric_value")
    .reset_index()
)
bdc_fy_snapshot.columns.name    = None
bdc_fy_snapshot["ingested_at"]  = pd.Timestamp.now().isoformat()

out_dir = volume_subdir("bdc_early_warning")
os.makedirs(out_dir, exist_ok=True)

ts_path  = f"{out_dir}/bdc_time_series.csv"
snap_path = f"{out_dir}/bdc_fy_snapshot.csv"

bdc_time_series.to_csv(ts_path,   index=False)
bdc_fy_snapshot.to_csv(snap_path, index=False)

print(f"Written: {ts_path}   ({len(bdc_time_series)} rows)")
print(f"Written: {snap_path} ({len(bdc_fy_snapshot)} rows)")

# COMMAND ----------

display(bdc_fy_snapshot)
