# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Produce unified signals from SEC filings (10-K, 10-Q, 8-K, 424B, etc.).
#
# Reads sec_filing_chunks (populated by 3_ingest_data/1_FMAPI/12_sec_parsing.py),
# aggregates text per filing with section prioritization (mda → risk_factors → rest),
# then extracts multiple signals per filing via a single ai_query call.
#
# One AI call per filing — ai_query returns a JSON array of 1-4 signals which are
# exploded into individual rows. Each signal carries its own sentiment so directional
# events (Credit Event, Earnings Miss) are Negative rather than blending with any
# positive context in the same document.
#
# Signal types by filing context:
#   Earnings Beat / Earnings Miss / Earnings In-Line  — results disclosures (8-K, 10-Q, 10-K)
#   Guidance Change    — net company-level EPS/income direction; one signal, net impact only
#   Credit Event       — HIGHEST PRIORITY for BDCs: non-accrual, PIK toggle, NAV decline,
#                        covenant breach, credit rating change, debt restructuring
#   M&A                — merger, acquisition, divestiture, spin-off, major strategic transaction
#   Management Change  — CEO, CFO, CIO, or board-level departure or appointment
#   Regulatory Action  — SEC/DOJ/CFTC enforcement, investigation, fine, compliance failure
#   Capital Action     — debt issuance, buyback, dividend policy change, rights offering
#                        (critical for BDC 424B prospectus supplements)
#   Risk Disclosure    — new or materially heightened risk factors (10-K focused)
#   Operational Update — strategic change, restructuring, new product or market, cost program
#
# Signal ID: md5(symbol|accession|pos) — one row per signal per filing.
# Idempotency: a filing is considered processed when its position-0 signal exists in gold.
#
# NOTE: if re-running after the old single-signal-per-filing schema, clear first:
#   DELETE FROM gold_unified_signals WHERE source_type = 'sec_filing'
#
# Output: {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals  (source_type = 'sec_filing')
# Run after: 3_ingest_data/1_FMAPI/12_sec_parsing.py

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

dbutils.widgets.text("test_limit", "")  # empty = process all; set a number (e.g. "10") to cap filings for testing

# COMMAND ----------

# # Uncomment to reset SEC filing signals only
# spark.sql(f"DELETE FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals WHERE source_type = 'sec_filing'")

# COMMAND ----------

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals (
        signal_id             STRING,
        symbol                STRING,
        signal_date           DATE,
        source_type           STRING,
        source_description    STRING,
        sentiment             STRING,
        severity_score        DOUBLE,
        advisor_action_needed BOOLEAN,
        signal_type           STRING,
        signal                STRING,
        signal_value          STRING,
        rationale             STRING,
        processed_at          TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# COMMAND ----------

_test_limit = dbutils.widgets.get("test_limit").strip()
_limit_clause = f"LIMIT {_test_limit}" if _test_limit else ""

# A filing is "done" when its position-0 signal exists in gold.
new_count = spark.sql(f"""
    SELECT COUNT(DISTINCT c.accession)
    FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
    LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
        ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', '0'))
    WHERE c.is_latest = true
""").collect()[0][0]

print(f"SEC filings to map into gold_unified_signals: {new_count}")

# COMMAND ----------

if new_count == 0:
    print("No new SEC filing signals to process.")
else:
    spark.sql(f"""
        MERGE INTO {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals AS tgt
        USING (

            -- ── Find filings not yet in gold ──────────────────────────────────────
            WITH new_filings AS (
                SELECT DISTINCT c.symbol, c.accession
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                LEFT ANTI JOIN {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals g
                    ON g.signal_id = md5(CONCAT(c.symbol, '|', c.accession, '|', '0'))
                WHERE c.is_latest = true
                {_limit_clause}
            ),

            -- ── Aggregate chunks per filing with section priority ──────────────────
            -- 10-K: mda carries the most actionable content, followed by risk_factors.
            -- 8-K / other: all sections in chunk order (usually one section anyway).
            -- Cap raised to 12 000 chars — covers ~8 chunks vs the old 4, ensuring
            -- 10-K MDA and risk disclosures are not truncated mid-argument.
            aggregated AS (
                SELECT
                    c.symbol,
                    c.form_type,
                    c.filing_date,
                    c.accession,
                    LEFT(
                        CONCAT_WS(
                            '\\n\\n',
                            transform(
                                sort_array(collect_list(struct(
                                    CASE c.section_name
                                        WHEN 'mda'                  THEN 0
                                        WHEN 'risk_factors'         THEN 1
                                        WHEN 'financial_statements' THEN 2
                                        WHEN 'full_text'            THEN 3
                                        ELSE 4
                                    END AS sort_key,
                                    c.chunk_index AS chunk_idx,
                                    c.chunk_text  AS chunk_text
                                ))),
                                x -> x.chunk_text
                            )
                        ),
                        12000
                    ) AS document_text
                FROM {UC_CATALOG}.{UC_SCHEMA}.sec_filing_chunks c
                JOIN new_filings nf USING (symbol, accession)
                WHERE c.is_latest = true
                GROUP BY c.symbol, c.form_type, c.filing_date, c.accession
            ),

            -- ── Build framed context string ────────────────────────────────────────
            context AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    CONCAT(
                        'SEC Filing\\n',
                        'Issuer: ', symbol, '\\n',
                        'Form type: ', form_type, '\\n',
                        'Filing date: ', filing_date, '\\n',
                        '---\\n',
                        document_text
                    ) AS context_text
                FROM aggregated
                WHERE document_text IS NOT NULL
                  AND LENGTH(TRIM(document_text)) > 100
            ),

            -- ── Single ai_query per filing — returns JSON array of 1-4 signals ─────
            -- Each signal carries its own sentiment so a Credit Event in an otherwise
            -- strong 10-K is correctly labelled Negative rather than blended to Mixed.
            extracted AS (
                SELECT
                    symbol, form_type, filing_date, accession,
                    TRIM(REGEXP_REPLACE(
                        ai_query(
                            '{LLM_ENDPOINT}',
                            CONCAT(
                                'You are a Goldman Sachs wealth advisor analyst reviewing an SEC filing.\\n\\n',
                                'Return a JSON array of 1–4 distinct investment signals found in this document. ',
                                'Each element must have exactly these five fields:\\n',
                                '  signal_type  — one of: Earnings Beat, Earnings Miss, Earnings In-Line, ',
                                'Guidance Change, Credit Event, M&A, Management Change, ',
                                'Regulatory Action, Capital Action, Risk Disclosure, Operational Update\\n',
                                '  sentiment    — Positive, Negative, or Neutral for THIS signal specifically\\n',
                                '  signal_value — High, Medium, or Low\\n',
                                '  advisor_action_needed — true or false\\n',
                                '  rationale — 1-2 sentences on the investment implication for a UHNW advisor. ',
                                'Be specific with numbers when available.\\n\\n',
                                'Signal type definitions:\\n',
                                '  Earnings Beat/Miss/In-Line: any filing disclosing financial results — ',
                                'always use the specific label, never a generic Earnings\\n',
                                '  Guidance Change: net company-level EPS or net income direction — ',
                                'emit ONE signal, assess net impact. If overall guidance is lower it is Negative ',
                                'even when individual segments are guided higher\\n',
                                '  Credit Event: HIGHEST PRIORITY for BDCs — any non-accrual designation, ',
                                'PIK interest toggle, NAV per share decline, covenant breach, credit rating ',
                                'change, or debt restructuring\\n',
                                '  M&A: merger, acquisition, divestiture, spin-off, or major strategic transaction\\n',
                                '  Management Change: CEO, CFO, CIO, or board-level departure or appointment\\n',
                                '  Regulatory Action: SEC/DOJ/CFTC enforcement, investigation, fine, or ',
                                'material compliance failure\\n',
                                '  Capital Action: debt issuance, share buyback, dividend policy change, or ',
                                'rights offering — especially important for BDC 424B prospectus supplements ',
                                'which signal capital raises that may indicate stress or dilution\\n',
                                '  Risk Disclosure: new or materially heightened risk factors (10-K focused)\\n',
                                '  Operational Update: strategic change, restructuring, new product or market\\n\\n',
                                'Sentiment rules:\\n',
                                '  Positive: Earnings Beat, Guidance Change (up), M&A (strategic positive), ',
                                'Capital Action (buyback or dividend raise)\\n',
                                '  Negative: Earnings Miss, Credit Event, Guidance Change (down), ',
                                'Regulatory Action, Management Change (sudden unplanned departure), ',
                                'Capital Action (BDC dilutive equity raise or high-cost debt)\\n',
                                '  Neutral: Earnings In-Line, Risk Disclosure, Operational Update, ',
                                'Management Change (planned succession)\\n\\n',
                                'advisor_action_needed rules — set true ONLY for:\\n',
                                '  (a) BDC Credit Event: non-accrual, PIK toggle, or NAV decline ≥5%\\n',
                                '  (b) Earnings Miss ≥10% below prior expectations or consensus\\n',
                                '  (c) Sudden CEO or CFO departure (not announced retirement or succession)\\n',
                                '  (d) Regulatory Action with a material fine, enforcement order, or ',
                                'restriction on business activities\\n',
                                '  (e) M&A where the issuer is the primary acquiree or is making a ',
                                'transformative acquisition\\n',
                                '  (f) Guidance Change where company-level EPS or net income is cut ≥15%\\n',
                                '  NOT for: routine 10-K annual reports, in-line results, minor regulatory ',
                                'matters, planned leadership transitions, or ordinary capital raises\\n\\n',
                                'Form type context:\\n',
                                '  8-K: event-driven — classify the specific triggering event, not the form\\n',
                                '  10-K: annual — focus on MDA financial performance, guidance, and material ',
                                'new risk factors; credit quality summary is critical for BDCs\\n',
                                '  10-Q: quarterly — same as 10-K but narrower; focus on quarter-specific results\\n',
                                '  424B2/424B5: BDC prospectus supplement — classify as Capital Action; ',
                                'flag High if the raise suggests capital stress or significant dilution\\n\\n',
                                'Only include signals clearly present in the document — do not pad. ',
                                'Return ONLY the JSON array, no markdown, no surrounding text.\\n\\n',
                                context_text
                            )
                        ),
                        '```json|```', ''
                    )) AS signals_json
                FROM context
            ),

            -- ── Explode JSON array into one row per signal ─────────────────────────
            exploded AS (
                SELECT
                    e.symbol, e.form_type, e.filing_date, e.accession,
                    pv.pos,
                    pv.sig
                FROM extracted e
                LATERAL VIEW posexplode(
                    from_json(
                        e.signals_json,
                        'ARRAY<STRUCT<signal_type:STRING, sentiment:STRING, signal_value:STRING, advisor_action_needed:BOOLEAN, rationale:STRING>>'
                    )
                ) pv AS pos, sig
                WHERE pv.sig IS NOT NULL
                  AND pv.sig.signal_type IS NOT NULL
                  AND pv.sig.rationale   IS NOT NULL
                  AND pv.sig.signal_value IN ('High', 'Medium', 'Low')
            )

            SELECT
                md5(CONCAT(symbol, '|', accession, '|', CAST(pos AS STRING)))  AS signal_id,
                symbol,
                TRY_CAST(filing_date AS DATE)                                   AS signal_date,
                'sec_filing'                                                    AS source_type,
                CONCAT(form_type, ' — ', filing_date)                          AS source_description,
                INITCAP(COALESCE(sig.sentiment, 'neutral'))                     AS sentiment,
                CASE sig.signal_value
                    WHEN 'High'   THEN 0.9
                    WHEN 'Medium' THEN 0.5
                    WHEN 'Low'    THEN 0.2
                    ELSE 0.3
                END                                                             AS severity_score,
                COALESCE(sig.advisor_action_needed, false)                      AS advisor_action_needed,
                sig.signal_type                                                 AS signal_type,
                sig.signal_type                                                 AS signal,
                sig.signal_value                                                AS signal_value,
                sig.rationale                                                   AS rationale,
                CURRENT_TIMESTAMP()                                             AS processed_at
            FROM exploded

        ) AS src
        ON tgt.signal_id = src.signal_id
        WHEN MATCHED THEN UPDATE SET
            tgt.sentiment             = src.sentiment,
            tgt.severity_score        = src.severity_score,
            tgt.advisor_action_needed = src.advisor_action_needed,
            tgt.signal_type           = src.signal_type,
            tgt.signal                = src.signal,
            tgt.signal_value          = src.signal_value,
            tgt.rationale             = src.rationale,
            tgt.processed_at          = src.processed_at
        WHEN NOT MATCHED THEN INSERT (
            signal_id, symbol, signal_date, source_type, source_description,
            sentiment, severity_score, advisor_action_needed, signal_type, signal, signal_value, rationale, processed_at
        ) VALUES (
            src.signal_id, src.symbol, src.signal_date, src.source_type,
            src.source_description, src.sentiment, src.severity_score, src.advisor_action_needed,
            src.signal_type, src.signal, src.signal_value, src.rationale, src.processed_at
        )
    """)
    print(f"Merged signals from {new_count} SEC filings into gold_unified_signals.")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT symbol, signal_date, source_description, signal_type, signal_value,
               advisor_action_needed, sentiment, severity_score,
               LEFT(rationale, 250) AS rationale_preview
        FROM {UC_CATALOG}.{UC_SCHEMA}.gold_unified_signals
        WHERE source_type = 'sec_filing'
        ORDER BY symbol, signal_date DESC, severity_score DESC
    """)
)

# COMMAND ----------
