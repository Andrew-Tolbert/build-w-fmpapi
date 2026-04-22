# Databricks notebook source
# Build the asset master table — one row per unique asset held across all clients.
# AUM is aggregated from daily_holdings. Sector/industry/geography are carried
# through from holdings for portfolio composition views.
#
# Performance metrics (ytd_return, alpha) are intentionally omitted here —
# they will be joined from materialized price data once 02_historical_prices
# has been landed and Delta-ized.
#
# BDC covenant metrics are manually seeded for the demo:
#   OBDC net_debt_to_ebitda=3.22 vs threshold=3.50 → headroom=0.28x → Agent 1 trigger
#
# Target: {catalog}.{schema}.asset_master

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark    = SparkSession.builder.getOrCreate()
uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('asset_master')}")

# COMMAND ----------

holdings_df = spark.table(uc_table("daily_holdings")).toPandas()

aum_by_asset = (
    holdings_df
    .groupby(["asset_id", "asset_class", "holding_name", "sector", "industry", "geography"])["market_value"]
    .sum()
    .reset_index()
    .rename(columns={"market_value": "aum"})
)
print(f"Unique assets: {len(aum_by_asset)}")

# COMMAND ----------

# ── BDC covenant metrics ──────────────────────────────────────────────────────
# OBDC (Blue Owl Capital) is seeded near the 3.5x covenant threshold.
# All other top-tier BDCs are healthy.

BDC_COVENANT_THRESHOLD = 3.50

BDC_COVENANT = {
    "ARCC": (1.82, 1.68),   # (net_debt_to_ebitda, headroom)
    "MAIN": (1.55, 1.95),
    "BXSL": (1.93, 1.57),
    "OBDC": (3.22, 0.28),   # ← approaching covenant breach — Agent 1 demo trigger
    "HTGC": (2.08, 1.42),
    "GBDC": (1.71, 1.79),
}

# ── PE fund GP metadata ───────────────────────────────────────────────────────
PE_GP = {
    "FUND_BX_RE23":  "Blackstone",
    "FUND_KKR_AM15": "KKR",
    "FUND_APO_X":    "Apollo",
    "FUND_CG_VII":   "Carlyle",
    "FUND_TPG_VIII": "TPG",
    "FUND_NB_IX":    "Neuberger Berman",
    "FUND_WP_XIV":   "Warburg Pincus",
    "FUND_GAM_IV":   "General Atlantic",
}

# COMMAND ----------

rows = []
for _, row in aum_by_asset.iterrows():
    asset_id    = row["asset_id"]
    asset_class = row["asset_class"]
    is_pc       = asset_class == "Private Credit"
    is_pe       = asset_class == "Alternatives"

    covenant_threshold = covenant_net_debt = covenant_headroom = None
    if is_pc and asset_id in BDC_COVENANT:
        covenant_net_debt, covenant_headroom = BDC_COVENANT[asset_id]
        covenant_threshold = BDC_COVENANT_THRESHOLD

    rows.append({
        "asset_id":           asset_id,
        "asset_name":         row["holding_name"],
        "asset_class":        asset_class,
        "sector":             row["sector"],
        "industry":           row["industry"],
        "geography":          row["geography"],
        "aum":                round(row["aum"], 2),
        "client_count":       int(holdings_df[holdings_df["asset_id"] == asset_id]["client_id"].nunique()),
        # PE structural metadata
        "gp_name":            PE_GP.get(asset_id),
        # BDC covenant metrics
        "covenant_threshold":   covenant_threshold,
        "net_debt_to_ebitda":   covenant_net_debt,
        "covenant_headroom":    covenant_headroom,
        # Performance fields — populated later from materialized price data
        "ytd_return_pct":     None,
        "ytd_alpha_bps":      None,
        "net_flows_ytd":      None,
        "revenue_yield_bps":  None,
        "benchmark_id":       None,
    })

asset_master_df = pd.DataFrame(rows).sort_values("aum", ascending=False)

# COMMAND ----------

print(f"Asset master: {len(asset_master_df)} assets")

by_class = asset_master_df.groupby("asset_class")["aum"].sum().sort_values(ascending=False)
print("\nAUM by asset class:")
for cls, aum in by_class.items():
    print(f"  {cls:<20} ${aum:>15,.0f}")

print(f"\nTotal AUM: ${asset_master_df['aum'].sum():,.0f}")

print("\n── BDC covenant summary ─────────────────────────────────────────────")
bdc_rows = asset_master_df[asset_master_df["covenant_threshold"].notna()].sort_values("covenant_headroom")
print(bdc_rows[["asset_id", "net_debt_to_ebitda", "covenant_threshold", "covenant_headroom", "client_count"]].to_string(index=False))

# COMMAND ----------

target = uc_table("asset_master")
sdf = spark.createDataFrame(asset_master_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
