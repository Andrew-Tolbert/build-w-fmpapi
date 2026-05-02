# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Volumes/ahtsa/awm/job_dependencies/requirements.txt",
# ]
# ///
# Transcript section analysis — sampling + section-split strategy evaluation.
#
# Purpose:
#   1. Show raw transcript content samples so we can see the actual format
#   2. Test the current Q&A regex and report hit rate
#   3. Test the proposed 3-strategy splitter and compare coverage
#   4. Show per-strategy breakdown so we know which approach carries the load
#
# Run this interactively to validate the new split_sections() logic before
# deploying to 3_ingest_data/1_FMAPI/10_transcripts.py.

# COMMAND ----------

# MAGIC %run ../utils/ingest_config

# COMMAND ----------

# ── 1. Raw samples — first 3 000 chars of 10 transcripts ─────────────────────

from pyspark.sql import functions as F

samples = spark.sql(f"""
    SELECT symbol, year, quarter, date, title,
           LEFT(content, 3000) AS content_sample,
           LENGTH(content)     AS total_chars
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts
    WHERE content IS NOT NULL
      AND LENGTH(TRIM(content)) > 500
    ORDER BY symbol, year DESC, quarter DESC
    LIMIT 10
""")

display(samples)

# COMMAND ----------

# ── 2. Section distribution in current bronze_transcript_chunks ───────────────

display(spark.sql(f"""
    SELECT
        call_section,
        COUNT(DISTINCT CONCAT(symbol, '_', year, '_', quarter)) AS transcripts,
        COUNT(*)                                                  AS total_chunks
    FROM {UC_CATALOG}.{UC_SCHEMA}.bronze_transcript_chunks
    GROUP BY call_section
    ORDER BY transcripts DESC
"""))

# A high 'full_text' count means the current splitter isn't finding section boundaries.

# COMMAND ----------

# ── 3. Current regex hit rate ─────────────────────────────────────────────────
# Run the existing Q&A pattern against all transcripts and report how many match.

import re

_QA_RE_CURRENT = re.compile(
    r'(?im)^\s*(?:question[- ]and[- ]answer|q\s*&\s*a|q\s*and\s*a'
    r'|questions?\s+and\s+answers?'
    r'|operator\s+instructions?\s+for\s+q(?:uestion)?'
    r'|we\s+will\s+now\s+begin\s+the\s+question)',
)

all_transcripts = (
    spark.table(f"{UC_CATALOG}.{UC_SCHEMA}.bronze_transcripts")
    .select("symbol", "year", "quarter", "content")
    .filter(F.col("content").isNotNull() & (F.length(F.col("content")) > 500))
    .collect()
)

current_hits = 0
total = len(all_transcripts)

for row in all_transcripts:
    if _QA_RE_CURRENT.search(row["content"] or ""):
        current_hits += 1

print(f"Current regex — hits: {current_hits}/{total}  ({100*current_hits/total:.0f}%)")

# COMMAND ----------

# ── 4. Proposed 3-strategy splitter ──────────────────────────────────────────
# Strategy 1: expanded line-anchored Q&A phrases  (current approach + more phrases)
# Strategy 2: search within Operator blocks for Q&A keywords (catches embedded transitions)
# Strategy 3: first-new-speaker detection (analysts appearing after management team)

def split_sections_v2(text: str) -> tuple[list[tuple[str, str]], str]:
    """Return (sections, strategy_used).  strategy_used is '1', '2', '3', or 'none'."""

    MIN_SECTION = 200

    # ── Strategy 1: expanded line-anchored phrases ───────────────────────────
    _S1 = re.compile(
        r'(?im)^\s*(?:'
        r'question[- ]and[- ]answer'
        r'|q\s*&\s*a\s+session'
        r'|q\s*and\s*a'
        r'|questions?\s+and\s+answers?'
        r'|we\s+will\s+now\s+(?:open|begin|start|take)\s+(?:the\s+)?(?:floor\s+for\s+questions?|q(?:uestion)?(?:[-\s]*(?:and[-\s]*answer|&\s*a))?)'
        r'|(?:now\s+)?(?:open|begin)\s+(?:the\s+)?question[-\s]and[-\s]answer'
        r'|now\s+(?:open|begin|take)\s+(?:questions|q&a)'
        r'|operator\s+instructions?\s+for\s+q(?:uestion)?'
        r'|please\s+(?:press|dial)\s+(?:star|\*)\s*(?:one|1)\s+(?:if\s+you|to\s+ask|for\s+question)'
        r')',
    )

    m = _S1.search(text)
    if m:
        block_start = text.rfind('\n', 0, m.start())
        block_start = 0 if block_start == -1 else block_start
        prepared = text[:block_start].strip()
        qa = text[block_start:].strip()
        if len(prepared) >= MIN_SECTION and len(qa) >= MIN_SECTION:
            return ([("prepared_remarks", prepared), ("qa", qa)], "1")

    # ── Strategy 2: search inside Operator speaker blocks ────────────────────
    _OP_BLOCK = re.compile(r'(?ms)^Operator:\s+(.+?)(?=\n[A-Z][A-Za-z]|\Z)')
    _QA_KW    = re.compile(
        r'(?i)'
        r'(?:open(?:ing)?\s+(?:the\s+)?(?:floor|call|line)\s+for\s+questions?'
        r'|begin\s+(?:the\s+)?(?:question|q(?:\s*&\s*|\s+and\s+)a)'
        r'|(?:press|dial)\s+(?:star|\*)\s*(?:one|1)'
        r'|q(?:\s*&\s*|\s+and\s+)a\s+session'
        r'|(?:our\s+)?first\s+question\s+(?:comes?\s+from|is\s+from)'
        r'|take\s+questions?\s+(?:now|at\s+this\s+time)'
        r')',
    )

    for op_m in _OP_BLOCK.finditer(text):
        if _QA_KW.search(op_m.group(1)):
            split_pos = op_m.start()
            prepared = text[:split_pos].strip()
            qa = text[split_pos:].strip()
            if len(prepared) >= MIN_SECTION and len(qa) >= MIN_SECTION:
                return ([("prepared_remarks", prepared), ("qa", qa)], "2")

    # ── Strategy 3: first-new-speaker heuristic ──────────────────────────────
    # Management team = first 4 non-Operator speakers (they own prepared remarks).
    # An analyst/new speaker appearing later marks the Q&A start.
    _TURN = re.compile(r'(?m)^([A-Z][A-Za-z\.\-\s]{2,35}):\s+')

    mgmt: list[str] = []
    for sm in _TURN.finditer(text):
        spkr = sm.group(1).strip()
        if spkr == "Operator":
            continue
        if len(mgmt) < 4:
            if spkr not in mgmt:
                mgmt.append(spkr)
        elif spkr not in mgmt:
            # First speaker outside the management team = Q&A start
            # Walk back to the preceding Operator block for a clean split
            prev_op = text.rfind("\nOperator:", 0, sm.start())
            split_pos = prev_op if prev_op > 0 else sm.start()
            prepared = text[:split_pos].strip()
            qa = text[split_pos:].strip()
            if len(prepared) >= MIN_SECTION and len(qa) >= MIN_SECTION:
                return ([("prepared_remarks", prepared), ("qa", qa)], "3")

    return ([("full_text", text)], "none")


# ── Run against all transcripts and tally results ─────────────────────────────

from collections import Counter
strategy_counts = Counter()

for row in all_transcripts:
    _, strategy = split_sections_v2(row["content"] or "")
    strategy_counts[strategy] += 1

print("\nNew splitter strategy breakdown:")
for strat, count in sorted(strategy_counts.items()):
    label = {
        "1":    "Strategy 1 — expanded line-anchored phrases",
        "2":    "Strategy 2 — Operator block keyword search",
        "3":    "Strategy 3 — first-new-speaker heuristic",
        "none": "No boundary found (full_text)",
    }.get(strat, strat)
    print(f"  {label}: {count}/{total}  ({100*count/total:.0f}%)")

split_total = sum(v for k, v in strategy_counts.items() if k != "none")
print(f"\nTotal transcripts successfully split: {split_total}/{total}  ({100*split_total/total:.0f}%)")
print(f"Improvement over current regex: +{split_total - current_hits} transcripts")

# COMMAND ----------

# ── 5. Inspect transcripts that still fall through ────────────────────────────
# Show the first 2 000 chars of any 'full_text' transcripts so we can see
# why they resist splitting and decide if a further strategy is warranted.

unsplit = [
    row for row in all_transcripts
    if split_sections_v2(row["content"] or "")[1] == "none"
]

print(f"Unsplit transcripts: {len(unsplit)}")
for row in unsplit[:5]:
    print(f"\n{'='*60}")
    print(f"{row['symbol']}  Q{row['quarter']} {row['year']}")
    print(row["content"][:2000] if row["content"] else "(empty)")

# COMMAND ----------

# ── 6. Sample section boundaries for a few transcripts ───────────────────────
# Shows where the split falls in each transcript so we can verify it's correct.

print("Sample section splits:")
for row in all_transcripts[:5]:
    sections, strategy = split_sections_v2(row["content"] or "")
    print(f"\n{row['symbol']} Q{row['quarter']} {row['year']}  [strategy={strategy}]")
    for name, body in sections:
        print(f"  {name:20s}  {len(body):,} chars  |  first 120: {body[:120].replace(chr(10), ' ')!r}")
