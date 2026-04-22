# Databricks notebook source
# Generate Investment Policy Statement (IPS) allocation targets per client.
# Each IPS profile defines target/min/max for 6 asset classes summing to 100%.
# UHNW profiles include Private Credit; HNW profiles do not.
# Target: {catalog}.{schema}.client_ips_targets

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import current_timestamp

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('client_ips_targets')}")

# COMMAND ----------

clients_df = spark.table(uc_table("clients")).toPandas()
print(f"Loaded {len(clients_df)} clients")

# COMMAND ----------

# ips_profile → {asset_class: (target_pct, min_pct, max_pct)}
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
for _, client in clients_df.iterrows():
    profile = client["ips_profile"]
    for asset_class, (target, lo, hi) in IPS_PROFILES[profile].items():
        records.append({
            "client_id":             client["client_id"],
            "ips_profile":           profile,
            "asset_class":           asset_class,
            "target_allocation_pct": target,
            "min_allocation_pct":    lo,
            "max_allocation_pct":    hi,
            "rebalance_trigger_pct": round(hi - target, 1),
        })

ips_df = pd.DataFrame(records)
print(f"Generated {len(ips_df)} IPS rows across {len(clients_df)} clients")
print(ips_df.groupby("ips_profile")["client_id"].nunique().reset_index(name="clients"))

# COMMAND ----------

target = uc_table("client_ips_targets")
sdf = spark.createDataFrame(ips_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
