# Databricks notebook source
# COMMAND ----------

# Generate the asset master table with AUM, performance, and PE-specific metrics.
# Powers the top-level dashboard KPIs: Total AUM, YTD Alpha, Net Flows, Revenue Yield.
# Also stores PE metrics (DPI, Net IRR, MOIC) for alternative positions.
# Target: uc.wealth.asset_master
# Source: S4

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

import random
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
random.seed(42)

# COMMAND ----------

# Derive AUM per asset from the holdings table
holdings_df = spark.table(uc_table("daily_holdings")).toPandas()
aum_by_asset = holdings_df.groupby(["asset_id","asset_class","holding_name"])["market_value"].sum().reset_index()
aum_by_asset.columns = ["asset_id","asset_class","asset_name","aum"]

# COMMAND ----------

# Benchmark mapping
BENCHMARK_MAP = {
    "Equity":         "^GSPC",
    "Fixed Income":   "AGG",
    "ETF":            "^GSPC",
    "Alternatives":   "^GSPC",
    "Private Credit": "AGG",
    "Cash":           None,
}

# Add performance and metadata columns
rows = []
for _, row in aum_by_asset.iterrows():
    asset_class = row["asset_class"]
    is_pe       = asset_class == "Alternatives"
    is_pc       = asset_class == "Private Credit"

    # YTD return: alternatives and private credit show stable/moderate returns
    if is_pe:
        ytd_return = round(random.uniform(8.0, 22.0), 2)
    elif is_pc:
        ytd_return = round(random.uniform(6.0, 12.0), 2)
    elif asset_class == "Cash":
        ytd_return = round(random.uniform(4.5, 5.2), 2)
    else:
        ytd_return = round(random.uniform(-5.0, 18.0), 2)

    # Alpha: positive for most, negative for covenant-breach name
    if row["asset_id"] == "AINV":
        ytd_alpha_bps = round(random.uniform(-250, -80), 0)   # underperforming — demo signal
    else:
        ytd_alpha_bps = round(random.uniform(-50, 200), 0)

    rows.append({
        "asset_id":            row["asset_id"],
        "asset_name":          row["asset_name"],
        "asset_class":         asset_class,
        "strategy":            "Private Credit" if is_pc else ("Buyout" if is_pe else asset_class),
        "aum":                 round(row["aum"], 2),
        "ytd_return_pct":      ytd_return,
        "ytd_alpha_bps":       ytd_alpha_bps,
        "net_flows_ytd":       round(random.uniform(-5e6, 20e6), 2),
        "revenue_yield_bps":   round(random.uniform(50, 125) if is_pe or is_pc else random.uniform(5, 50), 1),
        "benchmark_id":        BENCHMARK_MAP.get(asset_class),
        # PE-specific metrics (null for non-PE)
        "gp_name":             row["asset_name"].split(" Fund")[0] if is_pe else None,
        "vintage_year":        random.randint(2018, 2022) if is_pe else None,
        "dpi":                 round(random.uniform(0.2, 1.4), 2) if is_pe else None,
        "net_irr":             round(random.uniform(12.0, 28.0), 2) if is_pe else None,
        "moic":                round(random.uniform(1.2, 2.8), 2) if is_pe else None,
        # Private credit covenant metrics
        "covenant_threshold":  3.50 if is_pc else None,
        "net_debt_to_ebitda":  3.20 if row["asset_id"] == "AINV" else (round(random.uniform(1.5, 2.9), 2) if is_pc else None),
        "covenant_headroom":   0.30 if row["asset_id"] == "AINV" else (round(random.uniform(0.6, 2.0), 2) if is_pc else None),
    })

asset_master_df = pd.DataFrame(rows)
print(f"Generated {len(asset_master_df)} asset master records")

# Verify AINV shows the covenant breach scenario
ainv = asset_master_df[asset_master_df["asset_id"] == "AINV"]
if not ainv.empty:
    print(f"\nAINV covenant check — headroom: {ainv['covenant_headroom'].values[0]}x (threshold: {ainv['covenant_threshold'].values[0]}x)")

# COMMAND ----------

# Compute portfolio-level KPI summary
total_aum          = asset_master_df["aum"].sum()
wtd_alpha          = (asset_master_df["ytd_alpha_bps"] * asset_master_df["aum"]).sum() / total_aum
net_flows_ytd      = asset_master_df["net_flows_ytd"].sum()
wtd_revenue_yield  = (asset_master_df["revenue_yield_bps"] * asset_master_df["aum"]).sum() / total_aum

print(f"\n--- Portfolio KPIs ---")
print(f"Total AUM:       ${total_aum:,.0f}")
print(f"YTD Alpha:       {wtd_alpha:.0f} bps")
print(f"Net Flows YTD:   ${net_flows_ytd:,.0f}")
print(f"Revenue Yield:   {wtd_revenue_yield:.1f} bps")

# COMMAND ----------

target = uc_table("asset_master")
sdf = spark.createDataFrame(asset_master_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
