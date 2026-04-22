# Databricks notebook source
# Generate synthetic UHNW/HNW client CRM data.
# 110 clients: 80 UHNW (4 IPS profiles × 20) + 30 HNW (3 IPS profiles × 10).
# Target: {catalog}.{schema}.clients

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

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

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# Uncomment to drop and fully recreate the clients table:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('clients')}")

# COMMAND ----------

ADVISOR_IDS = [f"ADV{i:03d}" for i in range(1, 7)]  # 6 advisors

UHNW_NAME_PATTERNS = [
    "{last} Family Office",
    "{last} Trust",
    "{last} & Associates",
    "{last} Capital Management",
    "{first} {last} Private Wealth",
    "The {last} Group",
    "{last} Wealth Partners",
    "{last} Family Foundation",
]

HNW_NAME_PATTERNS = [
    "{first} {last}",
    "{last} Family",
    "{first} {last} Trust",
]

TONE_PROFILES = ["Formal", "Conversational", "Relationship-first"]
CONTACT_PREFS = ["Email", "Call", "Secure Message"]

# ips_profile drives allocation targets in 02_ips_targets.py
UHNW_IPS_PROFILES = ["Growth", "Income", "Balanced", "Alternatives-Heavy"]
HNW_IPS_PROFILES  = ["Conservative", "Moderate", "Growth-HNW"]

# COMMAND ----------

records = []
client_counter = 1

# 80 UHNW — 20 per IPS profile
for profile in UHNW_IPS_PROFILES:
    for _ in range(20):
        first = fake.first_name()
        last  = fake.last_name()
        pattern = random.choice(UHNW_NAME_PATTERNS)
        records.append({
            "client_id":               f"CLT{client_counter:04d}",
            "client_name":             pattern.format(first=first, last=last),
            "tier":                    "UHNW",
            "ips_profile":             profile,
            "bdc_eligible":            False,  # set below for 25 clients
            "total_aum":               round(random.uniform(25e6, 800e6), 2),
            "advisor_id":              random.choice(ADVISOR_IDS),
            "share_of_wallet_pct":     round(random.uniform(0.30, 0.95), 2),
            "contact_method_pref":     random.choice(CONTACT_PREFS),
            "tone_profile":            random.choice(TONE_PROFILES),
            "relationship_start_year": random.randint(2005, 2022),
        })
        client_counter += 1

# 30 HNW — 10 per IPS profile
for profile in HNW_IPS_PROFILES:
    for _ in range(10):
        first = fake.first_name()
        last  = fake.last_name()
        pattern = random.choice(HNW_NAME_PATTERNS)
        records.append({
            "client_id":               f"CLT{client_counter:04d}",
            "client_name":             pattern.format(first=first, last=last),
            "tier":                    "HNW",
            "ips_profile":             profile,
            "bdc_eligible":            False,
            "total_aum":               round(random.uniform(5e6, 25e6), 2),
            "advisor_id":              random.choice(ADVISOR_IDS),
            "share_of_wallet_pct":     round(random.uniform(0.30, 0.80), 2),
            "contact_method_pref":     random.choice(CONTACT_PREFS),
            "tone_profile":            random.choice(TONE_PROFILES),
            "relationship_start_year": random.randint(2008, 2023),
        })
        client_counter += 1

# COMMAND ----------

# Mark 25 UHNW clients as BDC-eligible (board-approved for private credit BDC exposure)
clients_df = pd.DataFrame(records)
uhnw_ids = clients_df[clients_df["tier"] == "UHNW"]["client_id"].tolist()
bdc_eligible_ids = set(random.sample(uhnw_ids, 25))
clients_df["bdc_eligible"] = clients_df["client_id"].isin(bdc_eligible_ids)

print(f"Generated {len(clients_df)} clients")
print(f"  UHNW: {(clients_df['tier'] == 'UHNW').sum()} | HNW: {(clients_df['tier'] == 'HNW').sum()}")
print(f"  BDC-eligible UHNW: {clients_df['bdc_eligible'].sum()}")
print(clients_df[["client_id", "client_name", "tier", "ips_profile", "total_aum", "bdc_eligible"]].head(12).to_string())

# COMMAND ----------

target = uc_table("clients")
sdf = spark.createDataFrame(clients_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target)
print(f"Written {sdf.count()} rows to {target}")
