# Databricks notebook source
# News article HTML downloader — reusable helper for other notebooks.
# Uses requests for HTTP and trafilatura for clean article text extraction.
# trafilatura is purpose-built for extracting main content from news/blog pages,
# stripping navigation, ads, and boilerplate automatically.
#
# Usage:
#   %run ../utils/news_downloader
#   text = fetch_article_text(url)   # returns str or None

import time
import requests
import trafilatura

# COMMAND ----------

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GS-AWM-Demo/1.0; +mailto:andrew.tolbert@databricks.com)"
}
_TIMEOUT     = 20
_RATE_DELAY  = 0.25   # polite delay between requests
_MAX_RETRIES = 3


def fetch_article_text(url: str) -> str | None:
    """
    Fetch a news article URL and return the main article text.

    Downloads the HTML with requests and extracts the main content using
    trafilatura (boilerplate, navigation, and ads are stripped automatically).
    Returns None if the page cannot be retrieved or no main content is found.
    """
    html = None
    for attempt in range(_MAX_RETRIES):
        try:
            resp = requests.get(url, headers=_HEADERS, timeout=_TIMEOUT)
            resp.raise_for_status()
            html = resp.text
            break
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else 0
            if status == 429:
                wait = 5 * (attempt + 1)
                print(f"    Rate-limited — sleeping {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            if status >= 500:
                time.sleep(2 ** attempt)
                continue
            print(f"    HTTP {status}: {url}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"    Network error (attempt {attempt + 1}/{_MAX_RETRIES}): {e}")
            time.sleep(2 ** attempt)
    else:
        print(f"    Giving up after {_MAX_RETRIES} retries: {url}")
        return None

    time.sleep(_RATE_DELAY)
    return trafilatura.extract(html, include_comments=False, include_tables=False)

# COMMAND ----------

print("fetch_article_text loaded.")
