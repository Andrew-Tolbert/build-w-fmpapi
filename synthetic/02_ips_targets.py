# Databricks notebook source
# COMMAND ----------

# Generate synthetic Investment Policy Statement (IPS) targets per client.
# Reads client list from uc.wealth.clients, then generates allocation targets
# that include deliberate drift violations for demo purposes.
# Target: uc.wealth.client_ips_targets
# Source: S2

# COMMAND ----------


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

# Load client list
clients_df = spark.table(uc_table("clients")).toPandas()
print(f"Loaded {len(clients_df)} clients")

# COMMAND ----------

# Asset class allocation targets — UHNW clients get more alternatives/private credit
ASSET_CLASSES = ["Equity", "Fixed Income", "ETF", "Alternatives", "Private Credit", "Cash"]

def ips_for_client(tier):
    """Return a dict of {asset_class: (target, min, max)} summing to 100%."""
    if tier == "UHNW":
        return {
            "Equity":         (35.0, 25.0, 45.0),
            "Fixed Income":   (20.0, 10.0, 30.0),
            "ETF":            (10.0,  5.0, 15.0),
            "Alternatives":   (20.0, 10.0, 30.0),
            "Private Credit": (10.0,  5.0, 15.0),
            "Cash":           ( 5.0,  2.0, 10.0),
        }
    else:
        return {
            "Equity":         (50.0, 40.0, 60.0),
            "Fixed Income":   (30.0, 20.0, 40.0),
            "ETF":            (10.0,  5.0, 15.0),
            "Alternatives":   ( 5.0,  0.0, 10.0),
            "Private Credit": ( 0.0,  0.0,  5.0),
            "Cash":           ( 5.0,  2.0, 10.0),
        }

# COMMAND ----------

records = []
for _, row in clients_df.iterrows():
    allocations = ips_for_client(row["tier"])
    for asset_class, (target, lo, hi) in allocations.items():
        records.append({
            "client_id":              row["client_id"],
            "asset_class":            asset_class,
            "target_allocation_pct":  target,
            "min_allocation_pct":     lo,
            "max_allocation_pct":     hi,
            "rebalance_trigger_pct":  round(hi - target, 1),  # drift band above target
        })

ips_df = pd.DataFrame(records)
print(f"Generated {len(ips_df)} IPS target rows ({len(clients_df)} clients × {len(ASSET_CLASSES)} asset classes)")

# COMMAND ----------

target = uc_table("client_ips_targets")
sdf = spark.createDataFrame(ips_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
