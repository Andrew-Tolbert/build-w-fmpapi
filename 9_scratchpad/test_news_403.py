# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "-r /Workspace/Users/andrew.tolbert@databricks.com/build-w-fmpapi/requirements.txt",
# ]
# ///
# Test approaches for bypassing 403 errors on news article URLs.
# Strategies from POINTERS: trafilatura built-in fetch, full browser headers + google referer.
# Run in Databricks where requirements.txt packages are available.

# COMMAND ----------

import time
import requests
import trafilatura

TEST_URLS = [
    "https://www.geekwire.com/2026/amazon-and-apple-vs-starlink-globalstar-satellite-acquisition-comes-with-a-big-iphone-bonus/",
    "https://www.proactiveinvestors.com/companies/news/1090550/apple-price-target-raised-ahead-of-earnings-on-expectation-of-strong-iphone-sales-services-growth-1090550.html",
]

# COMMAND ----------

# ---------------------------------------------------------------------------
# Strategy 1: trafilatura.fetch_url() — built-in header management
# ---------------------------------------------------------------------------

def strategy_trafilatura(url: str) -> tuple[str | None, str]:
    html = trafilatura.fetch_url(url)
    if html is None:
        return None, "trafilatura.fetch_url returned None"
    text = trafilatura.extract(html, include_comments=False, include_tables=False)
    if text is None:
        return None, "fetched HTML but no content extracted"
    return text, "ok"

# COMMAND ----------

# ---------------------------------------------------------------------------
# Strategy 2: requests with full browser-like headers + Google referer
# ---------------------------------------------------------------------------

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.google.com/",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
}

def strategy_browser_headers(url: str) -> tuple[str | None, str]:
    try:
        session = requests.Session()
        resp = session.get(url, headers=_BROWSER_HEADERS, timeout=20)
        resp.raise_for_status()
        text = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
        if text is None:
            return None, f"HTTP {resp.status_code} ok but no content extracted"
        return text, f"HTTP {resp.status_code} ok"
    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        return None, f"HTTP {status}"
    except Exception as e:
        return None, str(e)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Strategy 3: curl_cffi — browser TLS fingerprint impersonation
# ---------------------------------------------------------------------------

def strategy_curl_cffi(url: str) -> tuple[str | None, str]:
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        return None, "curl_cffi not installed — add to requirements.txt"
    try:
        resp = curl_requests.get(url, impersonate="chrome120", timeout=20)
        resp.raise_for_status()
        text = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
        if text is None:
            return None, f"HTTP {resp.status_code} ok but no content extracted"
        return text, f"HTTP {resp.status_code} ok"
    except Exception as e:
        return None, str(e)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Strategy 4: Wayback Machine CDX API — archived copy fallback
# ---------------------------------------------------------------------------

def strategy_wayback(url: str) -> tuple[str | None, str]:
    try:
        cdx = requests.get(
            "https://web.archive.org/cdx/search/cdx",
            params={
                "url": url,
                "output": "json",
                "limit": "1",
                "fl": "timestamp",
                "filter": "statuscode:200",
                "sort": "desc",
            },
            timeout=15,
        )
        rows = cdx.json()
        if len(rows) < 2:
            return None, "not found in Wayback Machine"
        ts = rows[1][0]
        archive_url = f"https://web.archive.org/web/{ts}/{url}"
        html = trafilatura.fetch_url(archive_url)
        if html is None:
            return None, "wayback fetch returned None"
        text = trafilatura.extract(html, include_comments=False, include_tables=False)
        return (text, "ok") if text else (None, "no content extracted from archive")
    except Exception as e:
        return None, str(e)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Strategy 5: Playwright — full browser rendering with JS execution
# ---------------------------------------------------------------------------

def strategy_playwright(url: str) -> tuple[str | None, str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None, "playwright not installed — pip install playwright && playwright install chromium"
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, timeout=30000, wait_until="networkidle")
            html = page.content()
            browser.close()
        text = trafilatura.extract(html, include_comments=False, include_tables=False)
        return (text, "ok") if text else (None, "rendered but no content extracted")
    except Exception as e:
        return None, str(e)

# COMMAND ----------

# ---------------------------------------------------------------------------
# Run all strategies against all URLs and report results
# ---------------------------------------------------------------------------

STRATEGIES = [
    ("trafilatura.fetch_url (built-in headers)", strategy_trafilatura),
    ("requests + full browser headers + google referer", strategy_browser_headers),
    ("curl_cffi browser TLS impersonation", strategy_curl_cffi),
    ("Wayback Machine CDX API", strategy_wayback),
    ("Playwright full browser rendering", strategy_playwright),
]

for url in TEST_URLS:
    print(f"\n{'='*70}")
    print(f"URL: {url}")
    print('='*70)
    for name, fn in STRATEGIES:
        text, status = fn(url)
        preview = (text[:300].replace("\n", " ") + "...") if text else "(no text)"
        result = "SUCCESS" if text else "FAIL"
        print(f"  [{result}] {name}")
        print(f"    status : {status}")
        print(f"    preview: {preview}")
        time.sleep(1)
