# Databricks notebook source
# FMP API client — reusable wrapper for all Financial Modeling Prep endpoints.
# Usage in other notebooks:
#   %run ../utils/fmp_client
#   client = FMPClient(api_key=FMP_API_KEY)

import requests
import time
from typing import Any

# COMMAND ----------

class FMPClient:
    """Thin wrapper around the FMP REST API with basic rate-limit handling."""

    def __init__(self, api_key: str, max_retries: int = 3, retry_delay: float = 1.5):
        if not api_key:
            raise ValueError("FMP_API_KEY is not set. Add it to your .env or Databricks secret scope.")
        self.api_key = api_key
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._stable = "https://financialmodelingprep.com/stable"
        self._v3     = "https://financialmodelingprep.com/api/v3"

    # ------------------------------------------------------------------
    # Core request
    # ------------------------------------------------------------------

    def get(self, url: str, params: dict | None = None) -> Any:
        """GET a URL, injecting the API key and retrying on transient errors."""
        p = params or {}
        p["apikey"] = self.api_key
        for attempt in range(1, self.max_retries + 1):
            resp = requests.get(url, params=p, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                print(f"  Rate limited — sleeping {self.retry_delay}s (attempt {attempt})")
                time.sleep(self.retry_delay)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Failed after {self.max_retries} retries: {url}")

    # ------------------------------------------------------------------
    # Company & profile
    # ------------------------------------------------------------------

    def get_profile(self, symbol: str) -> dict:
        data = self.get(f"{self._stable}/profile", {"symbol": symbol})
        return data[0] if isinstance(data, list) and data else data

    def get_profiles(self, symbols: list[str]) -> list[dict]:
        results = []
        for symbol in symbols:
            try:
                results.append(self.get_profile(symbol))
            except Exception as e:
                print(f"  {symbol}: ERROR — {e}")
        return results

    # ------------------------------------------------------------------
    # Historical prices
    # ------------------------------------------------------------------

    def get_historical_prices(self, symbol: str, from_date: str, to_date: str) -> list[dict]:
        data = self.get(f"{self._stable}/historical-price-eod/dividend-adjusted",
                        {"symbol": symbol, "from": from_date, "to": to_date})
        return data.get("historical", data) if isinstance(data, dict) else data

    # ------------------------------------------------------------------
    # Financial statements
    # ------------------------------------------------------------------

    def get_income_statement(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/income-statement",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_balance_sheet(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/balance-sheet-statement",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_cash_flow(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/cash-flow-statement",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_income_statement_growth(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/income-statement-growth",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_balance_sheet_growth(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/balance-sheet-statement-growth",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_cash_flow_growth(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/cash-flow-statement-growth",
                        {"symbol": symbol, "period": period, "limit": limit})

    # ------------------------------------------------------------------
    # Metrics & ratios
    # ------------------------------------------------------------------

    def get_key_metrics(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/key-metrics",
                        {"symbol": symbol, "period": period, "limit": limit})

    def get_ratios(self, symbol: str, period: str = "annual", limit: int = 8) -> list[dict]:
        return self.get(f"{self._stable}/ratios",
                        {"symbol": symbol, "period": period, "limit": limit})

    # ------------------------------------------------------------------
    # SEC filings
    # ------------------------------------------------------------------

    def get_sec_filings(self, symbol: str, filing_type: str = "10-Q", limit: int = 20) -> list[dict]:
        return self.get(f"{self._stable}/sec-filings",
                        {"symbol": symbol, "type": filing_type, "limit": limit})

    # ------------------------------------------------------------------
    # ETF data
    # ------------------------------------------------------------------

    def get_etf_info(self, symbol: str) -> dict:
        return self.get(f"{self._stable}/etf/info", {"symbol": symbol})

    def get_etf_holdings(self, symbol: str) -> list[dict]:
        data = self.get(f"{self._stable}/etf/holdings", {"symbol": symbol})
        return data.get("holdings", data) if isinstance(data, dict) else data

    def get_etf_sector_weightings(self, symbol: str) -> list[dict]:
        return self.get(f"{self._stable}/etf/sector-weightings", {"symbol": symbol})

    # ------------------------------------------------------------------
    # Analyst data
    # ------------------------------------------------------------------

    def get_analyst_estimates(self, symbol: str, period: str = "annual" , limit: int = 24) -> list[dict]:
        return self.get(f"{self._stable}/analyst-estimates",
                        {"symbol": symbol, "period": period,"limit": limit})

    def get_price_target_consensus(self, symbol: str) -> dict:
        return self.get(f"{self._stable}/price-target-consensus", {"symbol": symbol})

    def get_grades_summary(self, symbol: str) -> dict:
        return self.get(f"{self._stable}/grades-consensus", {"symbol": symbol})

    # ------------------------------------------------------------------
    # News
    # ------------------------------------------------------------------

    def get_stock_news(self, symbol: str,from_date: str, to_date: str, limit: int = 50) -> list[dict]:
        return self.get(f"{self._stable}/news/stock",
                        {"symbol": symbol, "from": from_date, "to": to_date, "limit": limit})

    # ------------------------------------------------------------------
    # Earnings transcripts
    # ------------------------------------------------------------------

    def get_transcript(self, symbol: str, year: int, quarter: int) -> dict:
        return self.get(f"{self._stable}/earning-call-transcript",
                        {"symbol": symbol, "year": year, "quarter": quarter})

    # ------------------------------------------------------------------
    # Indexes & VIX  (v3 endpoints)
    # ------------------------------------------------------------------

    def get_index_quote(self, symbols: list[str]) -> list[dict]:
        joined = ",".join(symbols)
        return self.get(f"{self._v3}/quote/{joined}")

    def get_index_historical(self, symbol: str, from_date: str, to_date: str) -> list[dict]:
        data = self.get(f"{self._stable}/historical-price-eod/full",
                        {"symbol": symbol,"from": from_date, "to": to_date})
        return data.get("historical", []) if isinstance(data, dict) else data

    def get_sp500_constituents(self) -> list[dict]:
        return self.get(f"{self._v3}/sp500_constituent")

    def get_nasdaq_constituents(self) -> list[dict]:
        return self.get(f"{self._v3}/nasdaq_constituent")

    def get_dowjones_constituents(self) -> list[dict]:
        return self.get(f"{self._v3}/dowjones_constituent")

# COMMAND ----------

print("FMPClient class loaded.")
