# Databricks notebook source
# MAGIC %md
# MAGIC # gold_app_management_tone — Create / Refresh
# MAGIC Synthetic management tone data for all advisor holdings.
# MAGIC Sections: Prepared Remarks, Q&A, Overall, Delta (NULL numerics — commentary only).
# MAGIC Re-run this cell to rebuild after data or sentiment updates.

# COMMAND ----------

UC_CATALOG = "ahtsa"
UC_SCHEMA  = "awm"


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ahtsa.awm.gold_unified_signals where signal_type LIKE '%Management Tone%' and source_type ='earnings_transcript'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Real-data SELECT — gold_unified_signals → gold_app_management_tone shape
# MAGIC Reads the latest quarter per symbol from `gold_unified_signals` and reshapes it to match
# MAGIC the `gold_app_management_tone` schema (3 sections: Prepared Remarks, Q&A, Overall).
# MAGIC Delta (section_order=4) is excluded until QoQ string-parsing logic is added.
# MAGIC
# MAGIC `signal_value` layout: `[neg_frac, neu_frac, pos_frac]` — multiplied × 100 and cast to INT.

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE ahtsa.awm.gold_app_management_tone 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE ahtsa.awm.gold_app_management_tone AS
# MAGIC WITH latest_signals AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     signal_type,
# MAGIC     signal_date,
# MAGIC     source_description,
# MAGIC     signal_value,
# MAGIC     sentiment,
# MAGIC     rationale,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY symbol, signal_type ORDER BY signal_date DESC)       AS rn,
# MAGIC     LEAD(source_description, 1) OVER (PARTITION BY symbol, signal_type ORDER BY signal_date DESC) AS prior_source_description
# MAGIC   FROM ahtsa.awm.gold_unified_signals
# MAGIC   WHERE signal_type LIKE '%Management Tone%' and source_type = 'earnings_transcript'
# MAGIC ),
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     symbol,
# MAGIC     signal_type,
# MAGIC     signal_date,
# MAGIC     source_description,
# MAGIC     prior_source_description,
# MAGIC     from_json(signal_value, 'ARRAY<DOUBLE>') AS tone_array,
# MAGIC     sentiment,
# MAGIC     rationale
# MAGIC   FROM latest_signals
# MAGIC   WHERE rn = 1
# MAGIC )
# MAGIC SELECT
# MAGIC   symbol                                                              AS holding_id,
# MAGIC   CASE signal_type
# MAGIC     WHEN 'Management Tone - prepared_remarks' THEN 'Prepared Remarks'
# MAGIC     WHEN 'Management Tone - qa'               THEN 'Q&A'
# MAGIC     WHEN 'Management Tone - Overall'          THEN 'Overall'
# MAGIC     WHEN 'Management Tone - Delta'            THEN 'Delta'
# MAGIC   END                                                                AS section,
# MAGIC   CASE signal_type
# MAGIC     WHEN 'Management Tone - prepared_remarks' THEN 1
# MAGIC     WHEN 'Management Tone - qa'               THEN 2
# MAGIC     WHEN 'Management Tone - Overall'          THEN 3
# MAGIC     WHEN 'Management Tone - Delta'            THEN 4
# MAGIC   END                                                                AS section_order,
# MAGIC   CAST(ROUND(tone_array[2] * 100) AS INT)                           AS positive_pct,
# MAGIC   CAST(ROUND(tone_array[1] * 100) AS INT)                           AS neutral_pct,
# MAGIC   CAST(ROUND(tone_array[0] * 100) AS INT)                           AS negative_pct,
# MAGIC   LOWER(sentiment)                                                   AS sentiment,
# MAGIC   rationale                                                          AS section_note,
# MAGIC   signal_date                                                        AS earnings_date,
# MAGIC   YEAR(signal_date)                                                  AS year,
# MAGIC   QUARTER(signal_date)                                               AS quarter,
# MAGIC   regexp_extract(source_description,       'Q[1-4] \\d{4}', 0)     AS quarter_label,
# MAGIC   regexp_extract(prior_source_description, 'Q[1-4] \\d{4}', 0)     AS prior_quarter_label,
# MAGIC   source_description
# MAGIC FROM parsed
# MAGIC ORDER BY holding_id, section_order

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ahtsa.awm.gold_app_management_tone where holding_id = 'TSLA'
