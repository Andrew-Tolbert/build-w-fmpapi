# Databricks notebook source
# Generate IPS allocation targets per risk profile.
# This is a reference table keyed by risk_profile (not per-client).
# 7 profiles × 6 asset classes = 42 rows.
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

IPS_PROFILES = {
    # ── UHNW profiles ─────────────────────────────────────────────────────────
    "Growth": {
        "Equity":         (45.0, 35.0, 55.0),
        "Fixed Income":   (15.0,  5.0, 25.0),
        "ETF":            (10.0,  5.0, 15.0),
        "Alternatives":   (20.0, 10.0, 30.0),
        "Private Credit": ( 5.0,  0.0, 10.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Income": {
        "Equity":         (25.0, 15.0, 35.0),
        "Fixed Income":   (30.0, 20.0, 40.0),
        "ETF":            (10.0,  5.0, 15.0),
        "Alternatives":   (15.0,  5.0, 25.0),
        "Private Credit": (15.0,  8.0, 20.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Balanced": {
        "Equity":         (35.0, 25.0, 45.0),
        "Fixed Income":   (20.0, 10.0, 30.0),
        "ETF":            (10.0,  5.0, 15.0),
        "Alternatives":   (20.0, 10.0, 30.0),
        "Private Credit": (10.0,  5.0, 15.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Alternatives-Heavy": {
        "Equity":         (25.0, 15.0, 35.0),
        "Fixed Income":   (10.0,  5.0, 20.0),
        "ETF":            (10.0,  5.0, 15.0),
        "Alternatives":   (35.0, 25.0, 45.0),
        "Private Credit": (15.0,  8.0, 22.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    # ── HNW profiles — no Private Credit allocation ────────────────────────────
    "Conservative": {
        "Equity":         (35.0, 25.0, 45.0),
        "Fixed Income":   (40.0, 30.0, 50.0),
        "ETF":            (15.0,  5.0, 20.0),
        "Alternatives":   ( 5.0,  0.0, 10.0),
        "Private Credit": ( 0.0,  0.0,  5.0),
        "Cash":           ( 5.0,  2.0, 10.0),
    },
    "Moderate": {
        "Equity":         (45.0, 35.0, 55.0),
        "Fixed Income":   (25.0, 15.0, 35.0),
        "ETF":            (15.0,  5.0, 20.0),
        "Alternatives":   ( 5.0,  0.0, 10.0),
        "Private Credit": ( 0.0,  0.0,  5.0),
        "Cash":           (10.0,  2.0, 15.0),
    },
    "Growth-HNW": {
        "Equity":         (55.0, 45.0, 65.0),
        "Fixed Income":   (20.0, 10.0, 30.0),
        "ETF":            (15.0,  5.0, 20.0),
        "Alternatives":   ( 5.0,  0.0, 10.0),
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
