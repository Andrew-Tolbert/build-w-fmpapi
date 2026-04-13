# Databricks notebook source
# COMMAND ----------

# Generate synthetic daily holdings — mapping clients to positions.
# Deliberately creates allocation drift and TLH opportunities for demo scenarios.
# Key demo constraint: several UHNW clients hold AINV (the covenant-breach name).
# Target: uc.wealth.daily_holdings
# Source: S3

# COMMAND ----------

# MAGIC %pip install python-dotenv

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

import random
import pandas as pd
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
random.seed(42)

# COMMAND ----------

clients_df = spark.table(uc_table("clients")).toPandas()
ips_df     = spark.table(uc_table("client_ips_targets")).toPandas()
as_of_date = datetime.date.today().isoformat()

# COMMAND ----------

# Asset pool — maps each position to an asset_class and a display name
ASSET_POOL = {
    "Equity":         [("GS","Goldman Sachs"),("MS","Morgan Stanley"),("JPM","JPMorgan Chase"),
                       ("BX","Blackstone"),("APO","Apollo Global"),("KKR","KKR & Co")],
    "Fixed Income":   [("AGG_BOND","iShares Core U.S. Aggregate Bond"),("HYG_BOND","iShares iBoxx $ High Yield")],
    "ETF":            [("SPY","SPDR S&P 500 ETF"),("AGG","iShares Core U.S. Agg Bond ETF"),
                       ("BKLN","Invesco Senior Loan ETF"),("HYG","iShares iBoxx High Yield ETF")],
    "Alternatives":   [("FUND_BX_CP9","Blackstone Capital Partners IX"),
                       ("FUND_KKR_A14","KKR Americas XII"),
                       ("FUND_APO_IX","Apollo Investment Fund IX")],
    "Private Credit": [("AINV","Apollo Investment Corp"),("OCSL","Oaktree Specialty Lending")],
    "Cash":           [("CASH_USD","USD Cash")],
}

# COMMAND ----------

records = []
position_counter = 1

for _, client in clients_df.iterrows():
    client_id = client["client_id"]
    tier      = client["tier"]
    total_aum = float(client["total_aum"])

    # Get this client's IPS targets
    client_ips = ips_df[ips_df["client_id"] == client_id].set_index("asset_class")

    for asset_class, assets in ASSET_POOL.items():
        if asset_class not in client_ips.index:
            continue
        target_pct = client_ips.loc[asset_class, "target_allocation_pct"]
        max_pct    = client_ips.loc[asset_class, "max_allocation_pct"]
        if target_pct == 0 and asset_class == "Private Credit" and tier == "HNW":
            continue  # HNW clients don't hold private credit

        # Introduce deliberate drift for UHNW clients in Private Credit (demo trigger)
        if asset_class == "Private Credit" and tier == "UHNW":
            actual_pct = round(random.uniform(max_pct + 1.0, max_pct + 5.0), 1)  # over max → drift alert
        else:
            actual_pct = round(random.uniform(max(0, target_pct - 3), target_pct + 3), 1)

        # Distribute actual_pct across available assets in this class
        chosen = random.sample(assets, min(len(assets), random.randint(1, 2)))
        weights = [random.random() for _ in chosen]
        total_w = sum(weights)
        weights = [w / total_w for w in weights]

        for (asset_id, holding_name), w in zip(chosen, weights):
            alloc_pct  = round(actual_pct * w, 2)
            mkt_value  = round(total_aum * alloc_pct / 100, 2)
            cost_basis = round(mkt_value * random.uniform(0.80, 1.20), 2)
            tlh_eligible = cost_basis > mkt_value  # held at a loss

            records.append({
                "position_id":                f"POS{position_counter:06d}",
                "client_id":                  client_id,
                "asset_id":                   asset_id,
                "asset_class":                asset_class,
                "holding_name":               holding_name,
                "market_value":               mkt_value,
                "current_allocation_pct":     alloc_pct,
                "cost_basis":                 cost_basis,
                "quantity":                   round(mkt_value / random.uniform(10, 500), 4) if asset_class not in ("Cash","Private Credit","Alternatives") else 1.0,
                "as_of_date":                 as_of_date,
                "tax_loss_harvesting_eligible": tlh_eligible,
            })
            position_counter += 1

holdings_df = pd.DataFrame(records)
print(f"Generated {len(holdings_df)} positions across {len(clients_df)} clients")

# Show clients holding AINV (the covenant-breach trigger name)
ainv_holders = holdings_df[holdings_df["asset_id"] == "AINV"]["client_id"].unique()
print(f"\nClients holding AINV (covenant-breach scenario): {len(ainv_holders)}")
print(ainv_holders)

# COMMAND ----------

target = uc_table("daily_holdings")
sdf = spark.createDataFrame(holdings_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
