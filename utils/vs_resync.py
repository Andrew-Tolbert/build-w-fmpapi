# Databricks notebook source
# Trigger and wait for a Databricks Vector Search index resync.
#
# Parameters:
#   index_name  — fully-qualified index name (e.g. "ahtsa.awm.vs_signals")

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

dbutils.widgets.text("index_name", "")
index_name = dbutils.widgets.get("index_name")

if not index_name:
    raise ValueError("index_name widget must be set")

# COMMAND ----------

w = WorkspaceClient()

print(f"Triggering sync for: {index_name}")
w.vector_search_indexes.sync_index(index_name=index_name)

# COMMAND ----------

POLL_INTERVAL_S = 30
TIMEOUT_S = 1800  # 30 min

elapsed = 0
while elapsed < TIMEOUT_S:
    idx = w.vector_search_indexes.get_index(index_name=index_name)
    if idx.status and idx.status.ready:
        print(f"Sync complete. Indexed rows: {idx.status.indexed_row_count or 'unknown'}")
        break
    msg = (idx.status.message or "syncing") if idx.status else "unknown"
    print(f"  [{elapsed}s] {msg} — retrying in {POLL_INTERVAL_S}s")
    time.sleep(POLL_INTERVAL_S)
    elapsed += POLL_INTERVAL_S
else:
    raise TimeoutError(f"Sync for {index_name} did not complete within {TIMEOUT_S // 60}m")
