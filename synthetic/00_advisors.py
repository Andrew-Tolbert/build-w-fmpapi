# Databricks notebook source
# Generate synthetic GS Private Wealth Management advisor roster.
# 12 advisors: 1 PMD, 2 MDs, 5 VPs, 3 Associates, 1 Analyst.
# Titles mirror the actual GS PWM rank structure used in the field.
# Target: {catalog}.{schema}.advisors

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import current_timestamp

uc_table = lambda name: f"{UC_CATALOG}.{UC_SCHEMA}.{name}"

# COMMAND ----------

# GS PWM rank hierarchy (1 = most senior)
# PMD = Partner & Managing Director (top of the practice)
# MD  = Managing Director
# VP  = Vice President
# ASC = Associate
# ANL = Analyst

ADVISORS = [
    {
        "advisor_id":        "ADV001",
        "first_name":        "James",
        "last_name":         "Whitfield",
        "full_name":         "James R. Whitfield",
        "title":             "Partner & Managing Director",
        "functional_title":  "Head of Private Wealth Management",
        "rank":              "PMD",
        "rank_order":        1,
        "office":            "New York, NY",
        "specialization":    "Ultra-High Net Worth Families, Alternatives",
        "years_at_gs":       22,
        "years_experience":  27,
        "email":             "james.whitfield@gs.com",
    },
    {
        "advisor_id":        "ADV002",
        "first_name":        "Margaret",
        "last_name":         "Chen",
        "full_name":         "Margaret T. Chen",
        "title":             "Managing Director",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "MD",
        "rank_order":        2,
        "office":            "New York, NY",
        "specialization":    "Family Office Services, Tax & Estate Planning",
        "years_at_gs":       16,
        "years_experience":  20,
        "email":             "margaret.chen@gs.com",
    },
    {
        "advisor_id":        "ADV003",
        "first_name":        "Robert",
        "last_name":         "Kessler",
        "full_name":         "Robert S. Kessler",
        "title":             "Managing Director",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "MD",
        "rank_order":        2,
        "office":            "Miami, FL",
        "specialization":    "Equity Markets, Concentrated Stock",
        "years_at_gs":       14,
        "years_experience":  19,
        "email":             "robert.kessler@gs.com",
    },
    {
        "advisor_id":        "ADV004",
        "first_name":        "Priya",
        "last_name":         "Mehta",
        "full_name":         "Priya N. Mehta",
        "title":             "Vice President",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "VP",
        "rank_order":        3,
        "office":            "New York, NY",
        "specialization":    "Private Credit, Alternative Investments",
        "years_at_gs":       9,
        "years_experience":  12,
        "email":             "priya.mehta@gs.com",
    },
    {
        "advisor_id":        "ADV005",
        "first_name":        "Thomas",
        "last_name":         "Hannigan",
        "full_name":         "Thomas J. Hannigan",
        "title":             "Vice President",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "VP",
        "rank_order":        3,
        "office":            "Boston, MA",
        "specialization":    "Balanced Portfolios, Endowments & Foundations",
        "years_at_gs":       8,
        "years_experience":  11,
        "email":             "thomas.hannigan@gs.com",
    },
    {
        "advisor_id":        "ADV006",
        "first_name":        "Sarah",
        "last_name":         "Okonkwo",
        "full_name":         "Sarah A. Okonkwo",
        "title":             "Vice President",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "VP",
        "rank_order":        3,
        "office":            "New York, NY",
        "specialization":    "Philanthropy, Impact Investing, ESG",
        "years_at_gs":       7,
        "years_experience":  10,
        "email":             "sarah.okonkwo@gs.com",
    },
    {
        "advisor_id":        "ADV007",
        "first_name":        "David",
        "last_name":         "Steinberg",
        "full_name":         "David E. Steinberg",
        "title":             "Vice President",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "VP",
        "rank_order":        3,
        "office":            "Los Angeles, CA",
        "specialization":    "Fixed Income, Liability-Driven Investing",
        "years_at_gs":       7,
        "years_experience":  9,
        "email":             "david.steinberg@gs.com",
    },
    {
        "advisor_id":        "ADV008",
        "first_name":        "Elena",
        "last_name":         "Vasquez",
        "full_name":         "Elena M. Vasquez",
        "title":             "Vice President",
        "functional_title":  "Private Wealth Advisor",
        "rank":              "VP",
        "rank_order":        3,
        "office":            "Chicago, IL",
        "specialization":    "Growth Equity, Technology Sector Wealth",
        "years_at_gs":       6,
        "years_experience":  8,
        "email":             "elena.vasquez@gs.com",
    },
    {
        "advisor_id":        "ADV009",
        "first_name":        "Michael",
        "last_name":         "Torres",
        "full_name":         "Michael K. Torres",
        "title":             "Associate",
        "functional_title":  "Private Wealth Management Associate",
        "rank":              "ASC",
        "rank_order":        4,
        "office":            "New York, NY",
        "specialization":    "Portfolio Analytics, Client Reporting",
        "years_at_gs":       4,
        "years_experience":  4,
        "email":             "michael.torres@gs.com",
    },
    {
        "advisor_id":        "ADV010",
        "first_name":        "Aisha",
        "last_name":         "Williams",
        "full_name":         "Aisha R. Williams",
        "title":             "Associate",
        "functional_title":  "Private Wealth Management Associate",
        "rank":              "ASC",
        "rank_order":        4,
        "office":            "New York, NY",
        "specialization":    "Estate Planning, Trust Structures",
        "years_at_gs":       3,
        "years_experience":  5,
        "email":             "aisha.williams@gs.com",
    },
    {
        "advisor_id":        "ADV011",
        "first_name":        "Christopher",
        "last_name":         "Park",
        "full_name":         "Christopher H. Park",
        "title":             "Associate",
        "functional_title":  "Private Wealth Management Associate",
        "rank":              "ASC",
        "rank_order":        4,
        "office":            "San Francisco, CA",
        "specialization":    "Alternatives Due Diligence, BDC Advisory",
        "years_at_gs":       3,
        "years_experience":  3,
        "email":             "christopher.park@gs.com",
    },
    {
        "advisor_id":        "ADV012",
        "first_name":        "Natalie",
        "last_name":         "Russo",
        "full_name":         "Natalie B. Russo",
        "title":             "Analyst",
        "functional_title":  "Private Wealth Management Analyst",
        "rank":              "ANL",
        "rank_order":        5,
        "office":            "New York, NY",
        "specialization":    "Quantitative Research, Portfolio Construction",
        "years_at_gs":       1,
        "years_experience":  1,
        "email":             "natalie.russo@gs.com",
    },
]

advisors_df = pd.DataFrame(ADVISORS)

# COMMAND ----------

print(f"Advisors generated: {len(advisors_df)}")
print(advisors_df.groupby(["rank", "title"])["advisor_id"].count().reset_index(name="count").to_string(index=False))
print()
print(advisors_df[["advisor_id", "full_name", "title", "office"]].to_string(index=False))

# COMMAND ----------

sdf = spark.createDataFrame(advisors_df).withColumn("ingested_at", current_timestamp())
sdf.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(uc_table("advisors"))
print(f"Written {sdf.count()} rows to {uc_table('advisors')}")

# COMMAND ----------

spark.table(uc_table("advisors")).display()
