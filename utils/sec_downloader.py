# Databricks notebook source
# SEC EDGAR document downloader — reusable helper for other notebooks.
# The SEC requires a descriptive User-Agent header; plain requests without it
# return 403 Forbidden. This module wraps urllib with the correct header,
# exponential-backoff retry, and a polite rate-limit delay.
#
# Usage:
#   %run ../utils/sec_downloader
#   text = fetch_sec_document(url)   # returns str or None

import time
import urllib.request
import urllib.error

# COMMAND ----------

_SEC_HEADERS    = {"User-Agent": "GS AWM Demo andrew.tolbert@databricks.com"}
_RATE_DELAY     = 0.12   # SEC guidance: no more than ~10 req/s
_MAX_RETRIES    = 3


def fetch_sec_document(url: str) -> str | None:
    """
    Fetch an SEC EDGAR document URL and return its text.

    Handles the mandatory User-Agent header, 429 rate-limit back-off,
    and transient 5xx retries.  Returns None if the document cannot
    be retrieved after _MAX_RETRIES attempts.
    """
    req = urllib.request.Request(url, headers=_SEC_HEADERS)
    for attempt in range(_MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                text = resp.read().decode("utf-8", errors="replace")
            time.sleep(_RATE_DELAY)
            return text
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 5 * (attempt + 1)
                print(f"    Rate-limited — sleeping {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            if e.code >= 500:
                time.sleep(2 ** attempt)
                continue
            # 4xx (other than 429) — not retryable
            print(f"    HTTP {e.code}: {url}")
            return None
        except (urllib.error.URLError, OSError) as e:
            print(f"    Network error (attempt {attempt + 1}/{_MAX_RETRIES}): {e}")
            time.sleep(2 ** attempt)
    print(f"    Giving up after {_MAX_RETRIES} retries: {url}")
    return None

# COMMAND ----------

print("fetch_sec_document loaded.")
