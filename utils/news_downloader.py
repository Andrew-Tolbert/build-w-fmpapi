# Databricks notebook source
# News article HTML downloader — reusable helper for other notebooks.
# Uses trafilatura for both HTTP fetching and clean article text extraction.
# trafilatura.fetch_url() manages realistic browser headers internally, which
# resolves most 403 Forbidden errors from news sites that block plain scrapers.
#
# Usage:
#   %run ../utils/news_downloader
#   text, error = fetch_article_text(url)
#     text  — extracted article body, or None
#     error — error description string, or None on success

import trafilatura

# COMMAND ----------

def fetch_article_text(url: str) -> tuple[str | None, str | None]:
    """
    Fetch a news article URL and return (text, error).

    Uses trafilatura.fetch_url() for HTTP (handles browser-like headers,
    avoiding most 403 blocks) and trafilatura.extract() for boilerplate removal.

    Returns:
        (text, None)  — success: extracted article body
        (None, error) — failure: text is None, error describes what went wrong
    """
    try:
        html = trafilatura.fetch_url(url)
        if html is None:
            return None, "fetch failed — site may block automated access"
        text = trafilatura.extract(html, include_comments=False, include_tables=False)
        if text is None:
            return None, "no main content extracted from page"
        return text, None
    except Exception as e:
        return None, str(e)

# COMMAND ----------

print("fetch_article_text loaded.")
