# Databricks notebook source
# News article HTML downloader — reusable helper for other notebooks.
# Uses trafilatura for both HTTP fetching and clean article text extraction.
# trafilatura.fetch_url() manages realistic browser headers internally, which
# resolves most 403 Forbidden errors from news sites that block plain scrapers.
# Falls back to curl_cffi (Chrome TLS fingerprint impersonation) for sites
# that use CDN-level bot detection that header tricks alone can't defeat.
#
# Usage:
#   %run ../utils/news_downloader
#   text, error = fetch_article_text(url)
#     text  — extracted article body, or None
#     error — error description string, or None on success

import trafilatura

# COMMAND ----------

try:
    from curl_cffi import requests as _curl_requests
    _CURL_CFFI_AVAILABLE = True
except ImportError:
    _CURL_CFFI_AVAILABLE = False

# COMMAND ----------

def fetch_article_text(url: str) -> tuple[str | None, str | None]:
    """
    Fetch a news article URL and return (text, error).

    Attempts two strategies in order:
    1. trafilatura.fetch_url() — handles browser-like headers, avoids most 403s.
    2. curl_cffi with Chrome impersonation — defeats CDN-level TLS fingerprinting
       (Cloudflare etc.) when strategy 1 returns None.

    Returns:
        (text, None)  — success: extracted article body
        (None, error) — failure: text is None, error describes what went wrong
    """
    # Strategy 1: trafilatura built-in fetch
    try:
        html = trafilatura.fetch_url(url)
        if html is not None:
            text = trafilatura.extract(html, include_comments=False, include_tables=False)
            if text is not None:
                return text, None
    except Exception:
        pass

    # Strategy 2: curl_cffi Chrome TLS impersonation
    if _CURL_CFFI_AVAILABLE:
        try:
            resp = _curl_requests.get(url, impersonate="chrome120", timeout=20)
            resp.raise_for_status()
            text = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
            if text is not None:
                return text, None
        except Exception as e:
            return None, f"both fetch strategies failed — last error: {e}"
        return None, "curl_cffi fetched page but no content extracted"

    return None, "fetch failed — site may block automated access (curl_cffi not available for fallback)"

# COMMAND ----------

print("fetch_article_text loaded.")
