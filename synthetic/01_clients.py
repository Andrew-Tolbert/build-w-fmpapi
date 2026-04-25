# Databricks notebook source
# Generate synthetic UHNW/HNW client CRM data.
# 250 clients: 150 UHNW + 100 HNW, total AUM ~$100B (log-normal distribution).
# Target: {catalog}.{schema}.clients

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import random
import numpy as np
import pandas as pd
from faker import Faker
from pyspark.sql.functions import current_timestamp

fake = Faker()
random.seed(42)
np.random.seed(42)
Faker.seed(42)

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# # Uncomment to drop and fully recreate:
# spark.sql(f"DROP TABLE IF EXISTS {uc_table('clients')}")

# COMMAND ----------

ADVISOR_IDS = [f"ADV{i:03d}" for i in range(1, 13)]  # 12 advisors

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

# Profile weights: Growth + Balanced are most common for UHNW
UHNW_PROFILES = (
    ["Growth"] * 45 + ["Balanced"] * 45 + ["Income"] * 30 + ["Alternatives-Heavy"] * 30
)
HNW_PROFILES = ["Moderate"] * 40 + ["Growth-HNW"] * 35 + ["Conservative"] * 25

# COMMAND ----------

# AUM distribution: log-normal, normalize to $95B UHNW / $5B HNW
uhnw_raw = np.random.lognormal(mean=19.5, sigma=1.2, size=150)
uhnw_aum = (uhnw_raw / uhnw_raw.sum()) * 95_000_000_000

hnw_raw = np.random.lognormal(mean=17.5, sigma=0.8, size=100)
hnw_aum = (hnw_raw / hnw_raw.sum()) * 5_000_000_000

import datetime
today = datetime.date.today()

def random_inception(years_min=1, years_max=12):
    days_back = random.randint(years_min * 365, years_max * 365)
    return today - datetime.timedelta(days=days_back)

records = []
client_counter = 1

# 150 UHNW
for i in range(150):
    first = fake.first_name()
    last  = fake.last_name()
    pattern = random.choice(UHNW_NAME_PATTERNS)
    records.append({
        "client_id":      f"CLT{client_counter:04d}",
        "client_name":    pattern.format(first=first, last=last),
        "tier":           "UHNW",
        "risk_profile":   random.choice(UHNW_PROFILES),
        "total_aum":      round(float(uhnw_aum[i]), 2),
        "base_currency":  "USD",
        "advisor_id":     random.choice(ADVISOR_IDS),
        "bdc_eligible":   False,
        "tone_profile":   random.choice(TONE_PROFILES),
        "contact_pref":   random.choice(CONTACT_PREFS),
        "inception_date": random_inception(1, 12),
    })
    client_counter += 1

# 100 HNW
for i in range(100):
    first = fake.first_name()
    last  = fake.last_name()
    pattern = random.choice(HNW_NAME_PATTERNS)
    records.append({
        "client_id":      f"CLT{client_counter:04d}",
        "client_name":    pattern.format(first=first, last=last),
        "tier":           "HNW",
        "risk_profile":   random.choice(HNW_PROFILES),
        "total_aum":      round(float(hnw_aum[i]), 2),
        "base_currency":  "USD",
        "advisor_id":     random.choice(ADVISOR_IDS),
        "bdc_eligible":   False,
        "tone_profile":   random.choice(TONE_PROFILES),
        "contact_pref":   random.choice(CONTACT_PREFS),
        "inception_date": random_inception(1, 8),
    })
    client_counter += 1

clients_df = pd.DataFrame(records)

# Mark ~25 UHNW clients as BDC-eligible (board-approved for private credit BDC exposure)
uhnw_ids = clients_df[clients_df["tier"] == "UHNW"]["client_id"].tolist()
bdc_eligible_ids = set(random.sample(uhnw_ids, 25))
clients_df["bdc_eligible"] = clients_df["client_id"].isin(bdc_eligible_ids)

# COMMAND ----------

# Validation
total_aum = clients_df["total_aum"].sum()
print(f"Clients generated: {len(clients_df)}")
print(f"  UHNW: {(clients_df['tier'] == 'UHNW').sum()} | HNW: {(clients_df['tier'] == 'HNW').sum()}")
print(f"  BDC-eligible: {clients_df['bdc_eligible'].sum()}")
print(f"  Total AUM: ${total_aum / 1e9:.2f}B")
print(f"  UHNW AUM: ${clients_df[clients_df['tier']=='UHNW']['total_aum'].sum() / 1e9:.2f}B")
print(f"  HNW AUM:  ${clients_df[clients_df['tier']=='HNW']['total_aum'].sum() / 1e9:.2f}B")
print(clients_df.groupby("risk_profile")["client_id"].count().reset_index(name="count").to_string(index=False))

# COMMAND ----------

sdf = spark.createDataFrame(clients_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("clients"))
print(f"Written {sdf.count()} rows to {uc_table('clients')}")
