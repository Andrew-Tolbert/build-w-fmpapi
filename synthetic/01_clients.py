# Databricks notebook source
# COMMAND ----------

# Generate synthetic UHNW client CRM data.
# Target: uc.wealth.clients
# Source: S1 — Faker-generated client records

# COMMAND ----------

# MAGIC %pip install faker python-dotenv

# COMMAND ----------

# MAGIC %run ../utils/config

# COMMAND ----------

import random
import pandas as pd
from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.getOrCreate()
fake  = Faker()
random.seed(42)
Faker.seed(42)

# COMMAND ----------

ADVISOR_IDS = ["ADV001", "ADV002", "ADV003", "ADV004"]
TIERS       = ["UHNW"] * 14 + ["HNW"] * 6   # 14 UHNW, 6 HNW = 20 clients

UHNW_NAME_PATTERNS = [
    "{last} Family Office",
    "{last} Trust",
    "{last} & Associates",
    "{last} Capital",
    "{first} {last} Private Wealth",
]

TONE_PROFILES = ["Formal", "Conversational", "Relationship-first"]
CONTACT_PREFS = ["Email", "Call", "Secure Message"]

# COMMAND ----------

records = []
for i, tier in enumerate(TIERS):
    first = fake.first_name()
    last  = fake.last_name()
    pattern = random.choice(UHNW_NAME_PATTERNS) if tier == "UHNW" else f"{first} {last}"
    client_name = pattern.format(first=first, last=last)

    records.append({
        "client_id":              f"CLT{i+1:04d}",
        "client_name":            client_name,
        "tier":                   tier,
        "total_aum":              round(random.uniform(25e6, 500e6) if tier == "UHNW" else random.uniform(5e6, 25e6), 2),
        "advisor_id":             random.choice(ADVISOR_IDS),
        "share_of_wallet_pct":    round(random.uniform(0.30, 0.95), 2),
        "contact_method_pref":    random.choice(CONTACT_PREFS),
        "tone_profile":           random.choice(TONE_PROFILES),
        "relationship_start_year": random.randint(2010, 2022),
    })

clients_df = pd.DataFrame(records)
print(f"Generated {len(clients_df)} clients")
print(clients_df[["client_id","client_name","tier","total_aum","advisor_id"]].head(10).to_string())

# COMMAND ----------

target = uc_table("clients")
sdf = spark.createDataFrame(clients_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
