# Databricks notebook source
# Pull FactSet SEC filing chunks via Delta Share for target companies.
# Resolves CUSIPs → FactSet entity IDs, then joins to vectorized EDG filings.
# Output: Display results; persist downstream as needed.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW target_companies AS (
# MAGIC
# MAGIC   WITH cusips AS (
# MAGIC   SELECT '037833100' AS cusip, 'AAPL' AS ticker
# MAGIC   UNION ALL SELECT '594918104',  'MSFT'
# MAGIC   UNION ALL SELECT '02079K305',  'GOOGL'
# MAGIC   UNION ALL SELECT '023135106',  'AMZN'
# MAGIC   UNION ALL SELECT '88160R101',  'TSLA'
# MAGIC   UNION ALL SELECT '67066G104',  'NVDA'
# MAGIC   UNION ALL SELECT '46625H100',  'JPM'
# MAGIC   UNION ALL SELECT '30303M102',  'META'
# MAGIC )
# MAGIC   SELECT
# MAGIC     i.ticker,
# MAGIC     i.cusip,
# MAGIC     c.FSYM_ID            AS fsym_security_id,
# MAGIC     cov.FSYM_ID          AS fsym_regional_id,
# MAGIC     se.FACTSET_ENTITY_ID,
# MAGIC     e.ENTITY_PROPER_NAME,
# MAGIC     e.ISO_COUNTRY,
# MAGIC     e.ENTITY_TYPE,
# MAGIC     tr.TICKER_REGION
# MAGIC   FROM cusips i
# MAGIC   JOIN delta_share_factset_do_not_delete_or_edit.sym_v1.sym_cusip c
# MAGIC     ON c.CUSIP = i.cusip
# MAGIC   JOIN delta_share_factset_do_not_delete_or_edit.ent_v1.ent_scr_sec_entity se
# MAGIC     ON se.FSYM_ID = c.FSYM_ID
# MAGIC   JOIN delta_share_factset_do_not_delete_or_edit.sym_v1.sym_entity e
# MAGIC     ON e.FACTSET_ENTITY_ID = se.FACTSET_ENTITY_ID
# MAGIC   JOIN delta_share_factset_do_not_delete_or_edit.sym_v1.sym_coverage cov
# MAGIC     ON cov.FSYM_SECURITY_ID = c.FSYM_ID AND cov.REGIONAL_FLAG = true
# MAGIC   JOIN delta_share_factset_do_not_delete_or_edit.sym_v1.sym_ticker_region tr
# MAGIC     ON tr.FSYM_ID = cov.FSYM_ID
# MAGIC   AND tr.TICKER_REGION LIKE CONCAT('%-', e.ISO_COUNTRY)
# MAGIC   WHERE e.ENTITY_TYPE = 'PUB'
# MAGIC   ORDER BY i.ticker
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM (
# MAGIC     SELECT
# MAGIC         edg.* EXCEPT(is_exhibit),
# MAGIC         docs.content AS chunk_text,
# MAGIC         tc.ticker,
# MAGIC         tc.TICKER_REGION,
# MAGIC         CASE edg.fds_filing_type
# MAGIC             WHEN '10-K'  THEN '10-K Annual Report'
# MAGIC             WHEN '10KSB' THEN '10-K Annual Report'
# MAGIC             WHEN '10-Q'  THEN '10-Q Quarterly Report'
# MAGIC             WHEN '8-K'   THEN '8-K Current Report'
# MAGIC             WHEN '20-F'  THEN '20-F Annual Report (Foreign)'
# MAGIC             WHEN '6-K'   THEN '6-K Report (Foreign)'
# MAGIC             ELSE edg.fds_filing_type
# MAGIC         END AS doc_type_label,
# MAGIC         CASE
# MAGIC             WHEN edg.exhibit_level > 0 OR edg.exhibit_title IS NOT NULL THEN true
# MAGIC             ELSE false
# MAGIC         END AS is_exhibit,
# MAGIC         YEAR(edg.acceptance_date) AS filing_year,
# MAGIC         CONCAT('Q', QUARTER(edg.acceptance_date)) AS filing_quarter,
# MAGIC         'edg_metadata' AS source_table,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY edg.chunk_id ORDER BY tc.ticker_region) AS _rn
# MAGIC     FROM factset_vectors_do_not_edit_or_delete.vector.edg_metadata edg
# MAGIC     JOIN factset_vectors_do_not_edit_or_delete.vector.all_docs docs
# MAGIC         ON edg.chunk_id = docs.chunk_id AND docs.product = 'EDG'
# MAGIC     INNER JOIN target_companies tc
# MAGIC     ON tc.FACTSET_ENTITY_ID = docs.entity_id
# MAGIC     WHERE docs.content IS NOT NULL
# MAGIC       AND TRIM(docs.content) != ''
# MAGIC       AND edg.token_count != 0
# MAGIC ) WHERE _rn = 1
