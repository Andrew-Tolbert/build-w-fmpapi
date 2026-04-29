# Databricks notebook source
# Generate IPS allocation targets per risk profile.
# This is a reference table keyed by risk_profile (not per-client).
# 7 profiles × 5 asset classes = 35 rows.
# "ETF" is no longer a standalone asset class — ETFs are reclassified at the
# reporting layer into Equity, Fixed Income, or Alternatives via bronze_etf_info.
# Target: {catalog}.{schema}.ips_targets

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import current_timestamp

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# # Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('ips_targets')}")

# COMMAND ----------

# risk_profile → {asset_class: (target_pct, min_pct, max_pct)}
# All profiles sum to 100% at target.
# "ETF" is no longer a standalone asset class — ETF holdings are reclassified by
# their underlying exposure (bronze_etf_info.assetClass) into Equity, Fixed Income,
# or Alternatives at the reporting layer. Their former ETF budget is redistributed here
# based on typical ETF composition in the portfolio:
#   ~55% of ETF AUM is equity ETFs (SPY, QQQ, XL* sector, EFA, EEM)
#   ~30% is fixed income ETFs (AGG, TLT, LQD, HYG, JNK, EMB, BIL, SHY, BKLN)
#   ~15% is alternatives ETFs (GLD, SLV, DBC, VNQ)

IPS_PROFILES = {
    # ── UHNW profiles ─────────────────────────────────────────────────────────
    "Growth": {
        # ETF was 10% → +6% Equity, +2% Fixed Income, +2% Alternatives
        "Equity":         (51.0, 41.0, 61.0),
        "Fixed Income":   (17.0,  7.0, 27.0),
        "Alternatives":   (22.0, 12.0, 32.0),
        "Private Credit": ( 5.0,  0.0, 10.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Income": {
        # ETF was 10% → +3% Equity, +5% Fixed Income, +2% Alternatives
        "Equity":         (28.0, 18.0, 38.0),
        "Fixed Income":   (35.0, 25.0, 45.0),
        "Alternatives":   (17.0,  7.0, 27.0),
        "Private Credit": (15.0,  8.0, 20.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Balanced": {
        # ETF was 10% → +5% Equity, +3% Fixed Income, +2% Alternatives
        "Equity":         (40.0, 30.0, 50.0),
        "Fixed Income":   (23.0, 13.0, 33.0),
        "Alternatives":   (22.0, 12.0, 32.0),
        "Private Credit": (10.0,  5.0, 15.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Alternatives-Heavy": {
        # ETF was 10% → +3% Equity, +2% Fixed Income, +5% Alternatives
        "Equity":         (28.0, 18.0, 38.0),
        "Fixed Income":   (12.0,  7.0, 22.0),
        "Alternatives":   (40.0, 30.0, 50.0),
        "Private Credit": (15.0,  8.0, 22.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    # ── HNW profiles — no Private Credit allocation ────────────────────────────
    "Conservative": {
        # ETF was 15% → +7% Equity, +5% Fixed Income, +3% Alternatives
        "Equity":         (42.0, 32.0, 52.0),
        "Fixed Income":   (45.0, 35.0, 55.0),
        "Alternatives":   ( 8.0,  0.0, 15.0),
        "Private Credit": ( 0.0,  0.0,  5.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Moderate": {
        # ETF was 15% → +8% Equity, +4% Fixed Income, +3% Alternatives
        "Equity":         (58.0, 48.0, 68.0),
        "Fixed Income":   (29.0, 19.0, 39.0),
        "Alternatives":   ( 8.0,  0.0, 15.0),
        "Private Credit": ( 0.0,  0.0,  5.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Growth-HNW": {
        # ETF was 15% → +9% Equity, +3% Fixed Income, +3% Alternatives
        "Equity":         (64.0, 54.0, 74.0),
        "Fixed Income":   (23.0, 13.0, 33.0),
        "Alternatives":   ( 8.0,  0.0, 15.0),
        "Private Credit": ( 0.0,  0.0,  5.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
}

# COMMAND ----------

records = []
for profile, asset_classes in IPS_PROFILES.items():
    total = sum(t for t, _, _ in asset_classes.values())
    assert abs(total - 100.0) < 0.01, f"Profile {profile} targets sum to {total}, expected 100"
    for asset_class, (target, lo, hi) in asset_classes.items():
        records.append({
            "risk_profile":           profile,
            "asset_class":            asset_class,
            "target_allocation_pct":  target,
            "min_allocation_pct":     lo,
            "max_allocation_pct":     hi,
            "rebalance_trigger_pct":  round(hi - target, 1),
        })

ips_df = pd.DataFrame(records)
print(f"Generated {len(ips_df)} IPS rows across {ips_df['risk_profile'].nunique()} profiles")
print(ips_df.groupby("risk_profile")["target_allocation_pct"].sum().reset_index(name="total_pct").to_string(index=False))

# COMMAND ----------

sdf = spark.createDataFrame(ips_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("ips_targets"))
print(f"Written {sdf.count()} rows to {uc_table('ips_targets')}")

# COMMAND ----------

spark.table(uc_table("ips_targets")).orderBy("risk_profile", "asset_class").display()
