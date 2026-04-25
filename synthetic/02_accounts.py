# Databricks notebook source
# Generate synthetic GS PWM accounts per client.
# UHNW: 2–4 accounts each. HNW: 1–2 accounts each.
# AUM is split across accounts using a Dirichlet distribution.
# Target: {catalog}.{schema}.accounts

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import random
import numpy as np
import pandas as pd
import datetime
from pyspark.sql.functions import current_timestamp

random.seed(43)
np.random.seed(43)

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# # Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('accounts')}")

# COMMAND ----------

clients_df = spark.table(uc_table("clients")).toPandas()
print(f"Loaded {len(clients_df)} clients")

# COMMAND ----------

# GS PWM account types by tier eligibility
UHNW_ACCOUNT_TYPES = [
    "PWM Discretionary",
    "Separately Managed Account (SMA)",
    "Alternative Investment Vehicle",
    "Trust Account",
    "Foundation/Endowment",
]
HNW_ACCOUNT_TYPES = [
    "PWM Discretionary",
    "Separately Managed Account (SMA)",
]

today = datetime.date.today()

records = []
account_counter = 1

for _, client in clients_df.iterrows():
    client_id     = client["client_id"]
    tier          = client["tier"]
    total_aum     = client["total_aum"]
    client_incep  = pd.Timestamp(client["inception_date"]).date()

    if tier == "UHNW":
        n_accounts   = random.randint(2, 4)
        eligible_extra = [t for t in UHNW_ACCOUNT_TYPES if t != "PWM Discretionary"]
    else:
        n_accounts   = random.randint(1, 2)
        eligible_extra = [t for t in HNW_ACCOUNT_TYPES if t != "PWM Discretionary"]

    # Build account type list — always start with PWM Discretionary
    account_types = ["PWM Discretionary"]
    if n_accounts > 1:
        extra = random.sample(eligible_extra, min(n_accounts - 1, len(eligible_extra)))
        account_types += extra

    # Split AUM via Dirichlet — concentrated on primary account
    alpha = np.array([5.0] + [1.5] * (len(account_types) - 1))
    aum_weights = np.random.dirichlet(alpha)
    account_aums = (aum_weights * total_aum).tolist()

    for idx, (acc_type, acc_aum) in enumerate(zip(account_types, account_aums)):
        # Primary account opens close to client inception; subsequent ones lag slightly
        days_offset = random.randint(0, 30) if idx == 0 else random.randint(30, 365)
        incep = client_incep + datetime.timedelta(days=days_offset)
        incep = min(incep, today - datetime.timedelta(days=365))

        records.append({
            "account_id":     f"ACC{account_counter:05d}",
            "client_id":      client_id,
            "account_name":   f"{client['client_name']} — {acc_type}",
            "account_type":   acc_type,
            "account_aum":    round(acc_aum, 2),
            "inception_date": incep,
            "base_currency":  "USD",
        })
        account_counter += 1

accounts_df = pd.DataFrame(records)

# COMMAND ----------

# Validation: account AUM should sum to client total_aum within rounding
check = accounts_df.groupby("client_id")["account_aum"].sum().reset_index(name="sum_aum")
check = check.merge(clients_df[["client_id", "total_aum"]], on="client_id")
check["diff"] = (check["sum_aum"] - check["total_aum"]).abs()
max_diff = check["diff"].max()
print(f"Accounts generated: {len(accounts_df)}")
print(f"  Avg per client: {len(accounts_df) / len(clients_df):.1f}")
print(f"  Max AUM rounding diff: ${max_diff:,.2f}")
print(accounts_df.groupby("account_type")["account_id"].count().reset_index(name="count").to_string(index=False))

# COMMAND ----------

sdf = spark.createDataFrame(accounts_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("accounts"))
print(f"Written {sdf.count()} rows to {uc_table('accounts')}")
