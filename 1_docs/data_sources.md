# Data Sources

**Goldman EBC — Art of the Possible**  
**Sources:** Financial Modeling Prep (FMP) API · SEC EDGAR (edgartools) · Synthetic (Faker)

---

## Overview

### Data Architecture: Two Distinct Layers

This pipeline is organized into two tiers that serve different purposes:

**Raw Data Layer** — Files stored in UC Volumes (`/Volumes/{catalog}/{schema}/raw_fmapi/`). These are the unmodified responses from external APIs and EDGAR: JSON files from FMP endpoints, HTML documents from EDGAR, and CSV exports from XBRL parsing. The `pull_data/` notebooks produce this layer. Raw data is the source of truth for re-ingestion; nothing in this layer is transformed or enriched.

**Lakehouse Data Layer** — Delta tables registered in Unity Catalog (`{catalog}.{schema}.*`). These are built from the raw volume files by the `ingest_data/` notebooks using Spark and Autoloader. All analytics, agent queries, and Genie workspaces query this layer. Follows medallion architecture: **Bronze** tables are direct representations of the raw files (type-cast and schema-enforced but otherwise unmodified); Silver and Gold layers are planned for enrichment and cross-source joins.

---

### Source Content Overview

Data for this demo falls into three content areas:

| Content Area | Source | Rationale |
|---|---|---|
| **Market & Financial Data** | Real — FMP API (11 notebooks) | Live prices, ratios, filings, and transcripts for real securities. LLM extracts genuine covenant language, real management tone, real earnings numbers. |
| **BDC Early-Warning Signals** | Real — SEC EDGAR via XBRL (1 notebook) | Direct XBRL parsing of 10-K/10-Q filings for 17 BDCs. Produces PIK, NII, NAV/share, and unrealized depreciation time series — the early-warning layer for private credit risk. |
| **Client & Portfolio Intelligence** | Synthetic — Python Faker + notebooks | No public source for client IPS targets, holdings allocations, or advisor communication style. Generated as Delta tables in Unity Catalog. |

**Securities in scope (full load):** ~60 equities across 7 sectors, 31 ETFs (broad market, fixed income, sector, international, commodities), and 16 BDCs. Configurable via `LIMITED_LOAD` widget. All tickers are defined as the single source of truth in `utils/ingest_config.py → TICKER_CONFIG`.

---

## Part 1 — FMP API Data (`pull_data/1_FMAPI/`)

All FMP notebooks share the same config pattern: they `%run ../../utils/ingest_config` to receive ticker lists, UC Volume paths, and a pre-authenticated `FMPClient` instance. Rate-limit handling (3 retries, 1.5s backoff on HTTP 429) is built into the client.

**Base URLs:**
- Stable API: `https://financialmodelingprep.com/stable`
- v3 API: `https://financialmodelingprep.com/api/v3`

---

### F1. Company Profiles (`01_company_profiles.py`)

**Endpoint:** `GET /stable/profile`  
**Query params:** `symbol={ticker}`, `apikey={key}`  
**Ticker scope:** `EQUITY_TICKERS` (all equities + BDCs, respects `LIMITED_LOAD`)  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/company_profiles/{TICKER}/{YYYY_MM_DD_HH}_profile.json`  
**Idempotency:** 30-day file recency check (no log table) — skips re-fetch if file is < 30 days old  
**Refresh config:** `full_refresh: True` (wipes directory on each refresh run)  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Response schema — array[0]:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker symbol |
| `companyName` | string | Legal entity name |
| `price` | float | Current market price |
| `mktCap` | float | Market capitalization |
| `beta` | float | Market beta |
| `sector` | string | GICS sector (e.g., "Financial Services") |
| `industry` | string | GICS industry sub-sector |
| `description` | string | Business description (multi-paragraph) |
| `ceo` | string | CEO name |
| `country` | string | Country code |
| `exchangeShortName` | string | Listing exchange (e.g., "NYSE") |
| `dcf` | float | DCF intrinsic value estimate |
| `isEtf` | boolean | ETF flag |
| `cik` | string | SEC CIK (10-digit, zero-padded) |

**Derived fields added:** `ingested_at` (ISO 8601 timestamp)

**Why it matters:** Foundation record for every holding card in the dashboard. `cik` is also the lookup key used by the EDGAR early-warning notebook to fetch XBRL filings without a manual CIK mapping.

---

### F2. Historical Prices (`02_historical_prices.py`)

**Endpoint:** `GET /stable/historical-price-eod/dividend-adjusted`  
**Query params:** `symbol={ticker}`, `from={HISTORY_START_DATE}`, `to={today}`, `apikey={key}`  
**Ticker scope:** `EQUITY_TICKERS`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/historical_prices/{TICKER}/{YYYY_MM_DD_HH}_prices.json`  
**Idempotency:** Full refresh — overwrites on every run  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Daily (`GS AWM | Data Ingest | Daily` — 6 AM UTC, Mon–Fri)

**Response schema — `response["historical"]` array:**

| Field | Type | Description |
|---|---|---|
| `date` | string (YYYY-MM-DD) | Trading date |
| `open` | float | Open price |
| `high` | float | Intraday high |
| `low` | float | Intraday low |
| `close` | float | Close price |
| `adjClose` | float | Dividend-adjusted close |
| `volume` | integer | Shares traded |
| `change` | float | Absolute price change vs. prior close |
| `changePercent` | float | Daily % change |
| `changeOverTime` | float | Cumulative return from `from` date (decimal) |

**Derived fields added:** `symbol` (ticker), `ingested_at`

**Why it matters:** `changeOverTime` is the raw input for YTD Alpha when differenced against the `^GSPC` benchmark series from the same start date. Used to populate per-holding performance charts and the top-level Alpha KPI.

---

### F3. Financial Statements (`03_financials.py`)

Six endpoints called per ticker — quarterly, 24 periods each (~6 years).

**Endpoints:**

| Call | Endpoint | Output filename |
|---|---|---|
| Income Statement | `GET /stable/income-statement?period=quarterly&limit=24` | `{ts}_income_statements.json` |
| Balance Sheet | `GET /stable/balance-sheet-statement?period=quarterly&limit=24` | `{ts}_balance_sheets.json` |
| Cash Flow | `GET /stable/cash-flow-statement?period=quarterly&limit=24` | `{ts}_cash_flows.json` |
| Income Growth | `GET /stable/income-statement-growth?period=quarterly&limit=24` | `{ts}_income_growth.json` |
| Balance Growth | `GET /stable/balance-sheet-statement-growth?period=quarterly&limit=24` | `{ts}_balance_growth.json` |
| Cash Flow Growth | `GET /stable/cash-flow-statement-growth?period=quarterly&limit=24` | `{ts}_cashflow_growth.json` |

**Ticker scope:** `EQUITY_TICKERS`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/financials/{TICKER}/{YYYY_MM_DD_HH}_{filename}.json`  
**Idempotency:** Full refresh  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Income Statement schema (array of period objects):**

| Field | Type | Description |
|---|---|---|
| `date` | string (YYYY-MM-DD) | Period end date |
| `period` | string | "Q1", "Q2", "Q3", "Q4" |
| `revenue` | float | Total revenue |
| `ebitda` | float | **Leverage covenant denominator** |
| `operatingIncome` | float | EBIT |
| `netIncome` | float | Net income |
| `eps` | float | Basic EPS |
| `epsDiluted` | float | Diluted EPS |
| `interestExpense` | float | **Interest coverage input** |
| `grossProfit` | float | Gross profit |
| `grossProfitRatio` | float | Gross margin |
| `costOfRevenue` | float | COGS |
| `operatingExpenses` | float | Total opex |

**Balance Sheet schema (key fields):**

| Field | Type | Description |
|---|---|---|
| `date` | string | Period end date |
| `totalAssets` | float | Total assets |
| `totalLiabilities` | float | Total liabilities |
| `totalStockholdersEquity` | float | Book equity |
| `cashAndCashEquivalents` | float | Cash |
| `totalDebt` | float | Total debt (short + long) |
| `longTermDebt` | float | Long-term debt |
| `shortTermDebt` | float | Current portion of LTD |
| `netDebt` | float | **Total debt minus cash — covenant numerator** |
| `goodwillAndIntangibleAssets` | float | Goodwill + intangibles |
| `retainedEarnings` | float | Retained earnings |

**Cash Flow schema (key fields):**

| Field | Type | Description |
|---|---|---|
| `date` | string | Period end date |
| `operatingCashFlow` | float | Cash from operations |
| `capitalExpenditure` | float | Capex (negative) |
| `freeCashFlow` | float | **Operating CF + Capex — DSCR proxy input** |
| `depreciationAndAmortization` | float | D&A |
| `dividendsPaid` | float | Dividends paid |

**Growth statements:** Same field structure as their base statements but values are YoY % changes (e.g., `revenueGrowth`, `ebitdaGrowth`, `netDebtGrowth`).

**Derived fields added:** `symbol`, `ingested_at`

---

### F4. Key Metrics & Financial Ratios (`04_key_metrics.py`)

Two endpoints per ticker, 24 quarterly periods each.

**Endpoints:**

| Call | Endpoint | Output filename |
|---|---|---|
| Key Metrics | `GET /stable/key-metrics?period=quarterly&limit=24` | `{ts}_key_metrics.json` |
| Ratios | `GET /stable/ratios?period=quarterly&limit=24` | `{ts}_financial_ratios.json` |

**Ticker scope:** `EQUITY_TICKERS`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/key_metrics/{TICKER}/{YYYY_MM_DD_HH}_{filename}.json`  
**Idempotency:** Full refresh  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Key Metrics schema (key fields):**

| Field | Type | Description |
|---|---|---|
| `date` | string | Period end date |
| `period` | string | Quarter |
| `netDebtToEBITDA` | float | **Primary leverage covenant proxy** |
| `interestCoverage` | float | **EBIT / Interest expense — secondary covenant proxy** |
| `debtToEquity` | float | Leverage ratio |
| `currentRatio` | float | Current assets / current liabilities |
| `quickRatio` | float | (Current assets − inventory) / current liabilities |
| `peRatio` | float | Price-to-earnings |
| `priceToBooksRatio` | float | Price-to-book |
| `earningsYield` | float | Inverse P/E |
| `freeCashFlowPerShare` | float | FCF per share |
| `dividendYield` | float | Dividend yield |
| `enterpriseValue` | float | EV |
| `marketCap` | float | Market cap |

**Financial Ratios schema (key fields):**

| Field | Type | Description |
|---|---|---|
| `date` | string | Period end date |
| `netProfitMargin` | float | Net income / revenue |
| `grossProfitMargin` | float | Gross profit / revenue |
| `operatingProfitMargin` | float | EBIT / revenue |
| `returnOnAssets` | float | ROA |
| `returnOnEquity` | float | ROE |
| `debtRatio` | float | Total debt / total assets |
| `interestCoverage` | float | EBIT / interest expense |
| `enterpriseValueMultiple` | float | EV / EBITDA |
| `operatingCashFlowRatio` | float | Operating CF / current liabilities |

**Derived fields added:** `symbol`, `ingested_at`

**Why it matters:** `netDebtToEBITDA` and `interestCoverage` are pre-computed here — Agent 1 can scan all holdings for covenant threshold breaches in a single query without reconstructing the ratios from raw statements.

---

### F5. SEC Filings (`05_sec_filings.py`)

**Endpoint:** `GET /stable/sec-filings-search/symbol`  
**Query params:** `symbol`, `from`, `to`, `page`, `limit=200`, `apikey`  
**Ticker scope:** All equities + BDCs (from `TICKER_CONFIG` with non-empty `sec_forms`)  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/sec_filings/{form_type_dir}/{TICKER}/{FORM_TYPE}_{filing_date}_{accession}.htm`  
**Idempotency:** Delta MERGE on `(symbol, accession)` — never re-downloads a fetched filing  
**Refresh config:** `full_refresh: False` — log table is NOT dropped on re-run  
**Log table:** `{catalog}.{schema}.sec_filings_log`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Response schema — array of filing objects:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker |
| `formType` | string | "10-K", "10-Q", "8-K", "424B2", "424B5" |
| `filingDate` | string (YYYY-MM-DD) | Date filed with SEC |
| `acceptedDate` | string (ISO 8601) | SEC acceptance timestamp |
| `link` | string | Original EDGAR HTML URL |
| `finalLink` | string | **Canonical document URL — used for HTML download** |
| `cik` | string | SEC CIK number |

**Pagination:** Fetches up to 100 pages (20,000 filings) but stops early when all target form counts (per `TICKER_CONFIG.sec_forms`) are satisfied or the API returns an empty batch.

**Downloaded file content:** Raw HTML from `finalLink` — the full EDGAR filing document.

**Delta log table schema:**

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `form_type` | STRING | e.g., "10-K" |
| `accession` | STRING | 18-digit EDGAR accession number |
| `filing_date` | STRING | YYYY-MM-DD |
| `link` | STRING | Original FMP link |
| `final_link` | STRING | Canonical EDGAR document URL |
| `subdir` | STRING | Form type as directory name (e.g., "10k") |
| `filename` | STRING | Output .htm filename |
| `downloaded_at` | TIMESTAMP | When the file was fetched |

**Natural key:** `symbol + accession`

**Form types by ticker:** Configured per-ticker in `TICKER_CONFIG.sec_forms`. Banks/BDCs include 424B2/424B5 (prospectus supplements). All equities get 10-K, 10-Q, 8-K.

**Why it matters:** 8-Ks carry material events (non-accrual designations, PIK toggles) that trigger the agent chain. 10-Qs contain loan-level schedules with borrower names, principal outstanding, interest rates, and fair value marks — the raw material for covenant headroom extraction.

---

### F6. ETF Data (`06_etf_data.py`)

Three endpoints per ETF ticker.

**Endpoints:**

| Call | Endpoint | Output filename |
|---|---|---|
| ETF Info | `GET /stable/etf/info` | `{ts}_etf_info.json` |
| ETF Holdings | `GET /stable/etf/holdings` | `{ts}_etf_holdings.json` |
| Sector Weightings | `GET /stable/etf/sector-weightings` | `{ts}_etf_sectors.json` |

**Ticker scope:** `ETF_TICKERS` — 31 ETFs: SPY, QQQ, IWM, VTI, VOO, DIA, AGG, TLT, LQD, HYG, JNK, EMB, BIL, SHY, BKLN, XLF, XLK, XLE, XLV, XLI, XLY, XLU, XLP, XLRE, EFA, EEM, VEU, GLD, SLV, DBC, VNQ  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/etf_data/{TICKER}/{YYYY_MM_DD_HH}_{filename}.json`  
**Idempotency:** Full refresh  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**ETF Info schema:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | ETF ticker |
| `name` | string | Full fund name |
| `description` | string | Fund objective/mandate |
| `aum` | float | Assets under management |
| `expenseRatio` | float | Annual fee as decimal (e.g., 0.0003) |
| `ytdReturn` | float | Year-to-date return % |
| `oneYearReturn` | float | 1-year return % |
| `threeYearReturn` | float | 3-year return % |
| `fiveYearReturn` | float | 5-year return % |
| `holdingsCount` | integer | Number of underlying securities |

**ETF Holdings schema — `response["holdings"]` array:**

| Field | Type | Description |
|---|---|---|
| `asset` | string | Underlying security ticker |
| `name` | string | Security name |
| `isin` | string | ISIN |
| `cusip` | string | CUSIP |
| `weightPercentage` | float | Portfolio weight (%) |
| `marketValue` | float | Dollar value in ETF |
| `exchange` | string | Listing exchange |

**Sector Weightings schema:**

| Field | Type | Description |
|---|---|---|
| `sector` | string | Sector name |
| `weightPercentage` | float | Allocation (%) |

**Derived fields added:** `symbol` (or `etf_symbol` for holdings), `ingested_at`

**Why it matters:** Look-through exposure: aggregating ETF holdings across all client positions surfaces real concentration risk. Sector weights join directly to `client_ips_targets` to detect IPS allocation drift (e.g., client's tech cap exceeded because two ETFs both overweight XLK).

---

### F7. Analyst Data (`07_analyst_data.py`)

Three endpoints per ticker.

**Endpoints:**

| Call | Endpoint | Output filename |
|---|---|---|
| Analyst Estimates | `GET /stable/analyst-estimates?period=quarterly&limit=25` | `{ts}_analyst_estimates.json` |
| Price Target Consensus | `GET /stable/price-target-consensus` | `{ts}_price_targets.json` |
| Grades Consensus | `GET /stable/grades-consensus` | `{ts}_analyst_ratings.json` |

**Ticker scope:** `EQUITY_TICKERS`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/analyst_data/{TICKER}/{YYYY_MM_DD_HH}_{filename}.json`  
**Idempotency:** Full refresh  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Daily (`GS AWM | Data Ingest | Daily` — 6 AM UTC, Mon–Fri)

**Analyst Estimates schema (array of forward periods):**

| Field | Type | Description |
|---|---|---|
| `date` | string | Estimate as-of date |
| `period` | string | "Q1", "Q2", etc. |
| `estimatedRevenueAvg` | float | Consensus revenue forecast |
| `estimatedRevenueLow` / `High` | float | Bear/bull revenue range |
| `estimatedEbitdaAvg` | float | **Forward EBITDA — covenant headroom projection** |
| `estimatedEbitdaLow` / `High` | float | Bear/bull EBITDA range |
| `estimatedEpsAvg` | float | Consensus EPS |
| `estimatedEpsLow` / `High` | float | EPS range |
| `numberAnalystEstimatedRevenue` | integer | Number of estimates |

**Price Target Consensus schema:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker |
| `targetConsensus` | float | Mean analyst price target |
| `targetMedian` | float | Median target |
| `targetHigh` | float | Bull case target |
| `targetLow` | float | Bear case target |
| `analystCount` | integer | Number of analysts |
| `lastUpdate` | string | Last update timestamp |

**Grades Consensus schema:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker |
| `strongBuy` / `buy` / `hold` / `sell` / `strongSell` | integer | Analyst counts per rating |
| `consensus` | string | Overall label (e.g., "Buy", "Hold") |

**Derived fields added:** `symbol`, `ingested_at`

**Why it matters:** Forward EBITDA estimates project covenant headroom 2–4 quarters out. If consensus EBITDA is declining while total debt is flat, the agent can flag a future breach before it appears in any filing. A concurrent analyst downgrade alongside deteriorating covenant metrics is a compounding signal that Agent 1 surfaces together.

---

### F8. Stock News (`08_news.py`)

**Endpoint:** `GET /stable/news/stock`  
**Query params:** `symbol`, `from` (first of current month), `to` (today), `limit=50`, `apikey`  
**Secondary fetch:** Full article text scraped from `url` via `fetch_article_text()` (custom HTTP downloader)  
**Ticker scope:** `EQUITY_TICKERS`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/stock_news/{TICKER}/{YYYY-MM-DD}_{8-char-md5}.json`  
**Idempotency:** Delta MERGE on `(symbol, url)` — retries errors, skips prior successes  
**Refresh config:** `full_refresh: False`  
**Log table:** `{catalog}.{schema}.stock_news_log`  
**Refresh cadence:** Daily (`GS AWM | Data Ingest | Daily` — 6 AM UTC, Mon–Fri)

**FMP response schema — array of article objects:**

| Field | Type | Description |
|---|---|---|
| `url` | string | Article hyperlink |
| `title` | string | Headline |
| `text` | string | Article snippet (~100–200 words) |
| `publishedDate` | string | Publication timestamp |
| `symbol` | string | Ticker |
| `site` | string | Publication name |
| `sentiment` | string | Sentiment label (optional) |

**Output JSON file schema (merged fields):**

| Field | Source | Description |
|---|---|---|
| `symbol` | derived | Ticker |
| `url` | FMP | Article URL |
| `publishedDate` | FMP | Publish timestamp |
| `title` | FMP | Headline |
| `summary` | FMP (`text`) | Article snippet (renamed) |
| `full_text` | web-scraped | Full article body |
| `site` | FMP | Publication name |
| `sentiment` | FMP | Sentiment label |
| `ingested_at` | derived | Fetch timestamp |

**Delta log table schema:**

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `url` | STRING | Article URL |
| `published_date` | STRING | YYYY-MM-DD |
| `filename` | STRING | Output JSON filename (NULL if error) |
| `fetch_status` | STRING | "success" or "error" |
| `error_message` | STRING | NULL if success, error text if failed |
| `downloaded_at` | TIMESTAMP | Attempt timestamp |

**Natural key:** `symbol + url`

**Why it matters:** News events can precede 8-K filings. Headlines are citation-linked in the advisor workspace alongside document paragraphs. Agent 3 includes relevant recent headlines in client alert drafts for market context.

---

### F9. Indexes & VIX (`09_indexes_and_vix.py`)

**Endpoint:** `GET /stable/historical-price-eod/full`  
**Query params:** `symbol`, `from={HISTORY_START_DATE}`, `to={today}`, `apikey`  
**Symbols:** `^GSPC` (S&P 500), `^DJI` (Dow Jones), `^IXIC` (Nasdaq), `^VIX` (volatility) — from `INDEX_SYMBOLS` + `VIX_SYMBOL` in `ingest_config`  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/indexes/{SYMBOL_CLEAN}/{YYYY_MM_DD_HH}_history.json` (symbol cleaned: `^` removed)  
**Idempotency:** Full refresh  
**Refresh config:** `full_refresh: True`  
**Refresh cadence:** Daily (`GS AWM | Data Ingest | Daily` — 6 AM UTC, Mon–Fri)

**Response schema — `response["historical"]` array:**

| Field | Type | Description |
|---|---|---|
| `date` | string (YYYY-MM-DD) | Trading date |
| `open` | float | Open |
| `high` | float | Intraday high |
| `low` | float | Intraday low |
| `close` | float | Close |
| `volume` | integer | Session volume |
| `change` | float | Absolute point change |
| `changePercent` | float | Daily % change |

**Derived fields added:** `symbol` (with `^` prefix), `ingested_at`

**Why it matters:** `^GSPC` `close` series is the authoritative benchmark for YTD Alpha calculations. VIX level provides market stress context — the same covenant headroom number carries different urgency at VIX 15 vs. VIX 30. Both are embedded in agent-drafted client alerts.

---

### F10. Earnings Call Transcripts (`10_transcripts.py`)

**Endpoint:** `GET /stable/earning-call-transcript`  
**Query params:** `symbol`, `year={year}`, `quarter={1|2|3|4}`, `apikey`  
**Ticker scope:** `get_tickers()` — all tickers respecting `LIMITED_LOAD` (intended: BDC/private credit focus)  
**Periods fetched:** Last 8 quarters (2 years) — current year + prior year, Q1–Q4  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/transcripts/{TICKER}/Q{quarter}_{year}.json`  
**Idempotency:** Delta MERGE on `(symbol, year, quarter)`  
**Refresh config:** `full_refresh: False`  
**Log table:** `{catalog}.{schema}.transcripts_log`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Response schema:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker |
| `date` | string (YYYY-MM-DD) | Earnings call date |
| `quarter` | integer | 1–4 |
| `year` | integer | Fiscal year |
| `title` | string | Call title/description |
| `content` | string | **Full transcript text** (often 10,000+ words) |

**Delta log table schema:**

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `year` | INT | Fiscal year |
| `quarter` | INT | Quarter (1–4) |
| `filename` | STRING | e.g., "Q1_2024.json" |
| `downloaded_at` | TIMESTAMP | Download timestamp |

**Natural key:** `symbol + year + quarter`

**Why it matters:** Powers "Management Tone" sentiment analysis. The delta between consecutive transcripts — tone shifting from "comfortable liquidity position" to "monitoring select credits closely" — is the signal Agent 1 extracts and presents to the advisor alongside covenant metrics.

---

### F11. Financial Reports JSON (`11_financial_reports.py`)

Two endpoints per ticker — availability check followed by selective report download.

**Endpoints:**

| Call | Endpoint | Purpose |
|---|---|---|
| Report Dates | `GET /stable/financial-reports-dates?symbol` | List available periods |
| Report JSON | `GET /stable/financial-reports-json?symbol&year={FY}&period={FY|Q1..Q4}` | Download full structured report |

**Ticker scope:** `EQUITY_TICKERS`  
**Periods fetched:** Top 5 most-recent periods per ticker, sorted `(fiscal_year DESC, period_order DESC)` where period order is FY=0 → Q4=4 → Q3=3 → Q2=2 → Q1=1  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/financial_reports/{TICKER}/{fiscal_year}_{period}.json` (e.g., `2024_FY.json`, `2024_Q3.json`)  
**Idempotency:** Delta MERGE on `(symbol, fiscal_year, period)`  
**Refresh config:** `full_refresh: False`  
**Log table:** `{catalog}.{schema}.financial_reports_log`  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**Report Dates response schema (array):**

| Field | Type | Description |
|---|---|---|
| `fiscalYear` | string | Fiscal year (e.g., "2024") |
| `period` | string | "FY", "Q1", "Q2", "Q3", "Q4" |
| `linkJson` | string | URL to structured JSON report |
| `linkXlsx` | string | URL to XLSX report |

**Financial Reports JSON — top-level structure:**

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Ticker |
| `financialStatements.incomeStatement` | array | Full income statement by period |
| `financialStatements.balanceSheet` | array | Full balance sheet by period |
| `financialStatements.cashFlowStatement` | array | Full cash flow by period |
| `ratios` | object | Pre-computed ratio history |
| `keyMetrics` | object | Pre-computed key metrics history |
| `growthRates` | object | YoY growth rates |

**Delta log table schema:**

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `fiscal_year` | STRING | e.g., "2024" |
| `period` | STRING | e.g., "Q1", "FY" |
| `link_json` | STRING | FMP JSON report URL |
| `link_xlsx` | STRING | FMP XLSX report URL |
| `filename` | STRING | Output filename |
| `downloaded_at` | TIMESTAMP | Download timestamp |

**Natural key:** `symbol + fiscal_year + period`

**Why it matters:** Pre-parsed structured JSON alternative to raw EDGAR HTML. Enables rapid covenant analysis and structured data extraction without OCR or NLP. The complete multi-period financial statements are consolidated in a single download per reporting period.

---

## Part 2 — SEC EDGAR Data (`pull_data/3_EDGAR/`)

### E1. BDC Early-Warning Signals (`01_bdc_early_warning.py`)

**Primary data source:** SEC EDGAR via `edgartools` Python library (direct XBRL parsing — not FMP)  
**Secondary source:** FMP `/stable/profile` (to resolve ticker → CIK for tickers not found by name)  
**Ticker scope:** `BDC_TICKERS` — 16 BDCs: ARCC, MAIN, GBDC, FSK, BXSL, OBDC, HTGC, NMFC, PSEC, SLRC, GSBD, CGBD, AINV, OCSL, TCPC, CSWC  
**UC Volume path:** `/Volumes/{catalog}/{schema}/raw_fmapi/bdc_early_warning/` (full directory cleared on every run)  
**Idempotency:** Full refresh — directory cleared at start of each run. No log table.  
**Output format:** CSV (not JSON)  
**Refresh cadence:** Monthly (`GS AWM | Data Ingest | Monthly` — 3 AM UTC, 1st of month)

**XBRL concepts extracted per BDC (from SEC 10-K/10-Q filings):**

| Metric key | XBRL concept | Description | Financial statement |
|---|---|---|---|
| `pik` | `us-gaap:InterestIncomeOperatingPaidInKind` | PIK interest accrued but not yet received in cash | Income statement |
| `nii` | `us-gaap:NetInvestmentIncome` | Net investment income — primary earnings proxy for BDCs | Income statement |
| `nii_ps` | `us-gaap:InvestmentCompanyInvestmentIncomeLossPerShare` | NII per share — dividend capacity indicator | Income statement |
| `div_ps` | `us-gaap:CommonStockDividendsPerShareDeclared` | Dividends declared per share | Income statement |
| `nav_ps` | `us-gaap:NetAssetValuePerShare` | NAV per share — intrinsic value benchmark | Balance sheet |
| `deprec` | `us-gaap:TaxBasisOfInvestmentsGrossUnrealizedDepreciation` | Unrealized losses on portfolio — stress indicator | Balance sheet |
| `net_assets` | `us-gaap:AssetsNet` | Total net assets — leverage denominator | Balance sheet |
| `realized_gl` | `us-gaap:RealizedInvestmentGainsLosses` | Realized gains/losses — P&L impact | Cash flow / other |
| `gl_ps` | `us-gaap:InvestmentCompanyGainLossOnInvestmentPerShare` | Cumulative gain/loss per share | Cash flow / other |

**edgartools data retrieval pattern:**
```python
facts = Company(int(cik)).get_facts()          # All XBRL facts for this CIK
ts = facts.time_series(xbrl_concept)            # Time series for one concept
# Returns: DataFrame with period_start, period_end, fiscal_period, numeric_value, accession_number, form
```

**Output files:**

**`bdc_time_series.csv` — long format (one row per BDC × metric × reporting period):**

| Column | Type | Description |
|---|---|---|
| `ticker` | string | BDC symbol |
| `cik` | string | 10-digit SEC CIK (zero-padded) |
| `metric` | string | Short key (e.g., "pik", "nii", "nav_ps") |
| `period_end` | date | Reporting period end date |
| `fiscal_period` | string | "Q1", "Q2", "Q3", "Q4", or "FY" |
| `year_quarter` | string | "{year}-{fiscal_period}" (e.g., "2024-Q4") |
| `numeric_value` | float | Reported XBRL value |
| `accession_number` | string | EDGAR accession ID (when available) |
| `form` | string | "10-K" or "10-Q" (when available) |
| `period_start` | date | Period start date (when available) |
| `ingested_at` | string | ISO 8601 fetch timestamp |

**`bdc_fy_snapshot.csv` — wide format (one row per BDC, most recent FY only):**

| Column | Type | Description |
|---|---|---|
| `ticker` | string | BDC symbol |
| `cik` | string | SEC CIK |
| `year_quarter` | string | Most recent FY label (e.g., "2024-FY") |
| `pik` | float | PIK income |
| `nii` | float | Net investment income |
| `nii_ps` | float | NII per share |
| `div_ps` | float | Dividends per share |
| `nav_ps` | float | NAV per share |
| `deprec` | float | Unrealized depreciation |
| `net_assets` | float | Total net assets |
| `realized_gl` | float | Realized gains/losses |
| `gl_ps` | float | Gain/loss per share |
| `ingested_at` | string | Fetch timestamp |

**Fiscal year handling:** Some BDCs use non-calendar fiscal years (e.g., AINV ends in March). `edgartools` provides `fiscal_year` metadata when available; otherwise it is derived from `period_end.year`. The `year_quarter` column uses fiscal year so AINV's Q4 ending 2024-03-31 is labeled "2024-Q4" not "2023-Q4".

**Why it matters:** These nine XBRL metrics are the early-warning layer that no FMP endpoint provides. Rising PIK (deferred cash income), compressing NAV/share, and growing unrealized depreciation are the specific signals that precede BDC covenant stress — they appear in EDGAR filings quarters before they surface in analyst commentary. The `bdc_fy_snapshot.csv` feeds directly into Agent 1's threshold scanning.

---

## Part 3 — Synthetic Structured Data (`synthetic/`)

Generated using Python Faker in Databricks. Registered directly in Unity Catalog as Delta tables (no raw volume layer). Notebooks run in order: `01_clients` → `02_accounts` → `03_ips_targets` → `04_holdings` → `05_transactions`. Two maintenance notebooks keep the dataset current after each daily price refresh.

---

### S1. Clients (`{catalog}.{schema}.clients`)

**Primary Key:** `client_id`  
**Generated by:** `synthetic/01_clients.py`  
**Row count:** 250 clients (150 UHNW + 100 HNW; total AUM ~$100B)  
**Demo component:** Every agent and Genie query is scoped to clients by advisor.

| Field | Description |
|---|---|
| `client_id` | Sequential ID (CLT0001–CLT0250) |
| `client_name` | e.g. "Smith Family Office", "Chen Trust", "Rodriguez Family" |
| `tier` | UHNW (150 clients) / HNW (100 clients) |
| `risk_profile` | UHNW: Growth / Balanced / Income / Alternatives-Heavy; HNW: Moderate / Growth-HNW / Conservative |
| `total_aum` | Total AUM across all accounts — log-normal, normalized to ~$95B UHNW / ~$5B HNW |
| `base_currency` | USD |
| `advisor_id` | Assigned wealth advisor (ADV001–ADV012; 12 advisors) |
| `bdc_eligible` | Boolean — 25 randomly selected UHNW clients board-approved for BDC / private credit exposure |
| `tone_profile` | Formal / Conversational / Relationship-first |
| `contact_pref` | Email / Call / Secure Message |
| `inception_date` | Client relationship start date (spread across price history window) |
| `ingested_at` | Timestamp |

**Why it matters:** `tone_profile` is what Agent 3 consults when drafting reallocation proposals — the same covenant event generates a different email for "Chen Trust" (formal, concise) vs. "Rodriguez Family Office" (conversational, relationship-first). `bdc_eligible` gates which clients receive Private Credit / BDC allocations in holdings.

---

### S2. Accounts (`{catalog}.{schema}.accounts`)

**Primary Key:** `account_id`  
**Generated by:** `synthetic/02_accounts.py`  
**Row count:** ~500–650 accounts (UHNW: 2–4 per client; HNW: 1–2 per client)  
**Demo component:** Account-level position granularity and fee attribution.

| Field | Description |
|---|---|
| `account_id` | Sequential ID (ACC00001–...) |
| `client_id` | Parent client reference |
| `account_name` | e.g. "Smith Family Office — PWM Discretionary" |
| `account_type` | PWM Discretionary / Separately Managed Account (SMA) / Alternative Investment Vehicle / Trust Account / Foundation/Endowment (latter three UHNW only) |
| `account_aum` | Account-level AUM; client `total_aum` split via Dirichlet (primary account gets ~5× weight vs. secondary) |
| `inception_date` | Account open date; primary ≤30 days after client inception; secondary accounts lag 30–180 days |
| `base_currency` | USD |
| `ingested_at` | Timestamp |

**Why it matters:** Holdings and transactions are keyed at the account level, enabling per-account IPS compliance checks, fee attribution by account type, and account-type filtering in Genie (e.g., "show only SMA accounts").

---

### S3. IPS Targets (`{catalog}.{schema}.ips_targets`)

**Primary Key:** `risk_profile` + `asset_class`  
**Generated by:** `synthetic/03_ips_targets.py`  
**Row count:** 42 rows (7 risk profiles × 6 asset classes)  
**Demo component:** Allocation drift detection; Genie query "overweight private credit relative to IPS"

| Field | Description |
|---|---|
| `risk_profile` | Risk profile name (linked to `clients.risk_profile`) |
| `asset_class` | Equity / Fixed Income / ETF / Alternatives / Private Credit / Cash |
| `target_allocation_pct` | IPS target (e.g., 10.0%) |
| `min_allocation_pct` | Lower bound |
| `max_allocation_pct` | Upper bound |
| `rebalance_trigger_pct` | Drift threshold — `max − target` |
| `ingested_at` | Timestamp |

**Profile summary (target %):**

| Profile | Tier | Equity | Fixed Income | ETF | Alternatives | Private Credit | Cash |
|---|---|---|---|---|---|---|---|
| Growth | UHNW | 45 | 15 | 10 | 20 | 5 | 5 |
| Balanced | UHNW | 35 | 20 | 10 | 20 | 10 | 5 |
| Income | UHNW | 25 | 30 | 10 | 15 | 15 | 5 |
| Alternatives-Heavy | UHNW | 25 | 10 | 10 | 35 | 15 | 5 |
| Moderate | HNW | 50 | 25 | 15 | 5 | 0 | 5 |
| Growth-HNW | HNW | 55 | 20 | 15 | 5 | 0 | 5 |
| Conservative | HNW | 35 | 40 | 15 | 5 | 0 | 5 |

**Key design decision:** HNW profiles carry 0% Private Credit target (max 5%). Only `bdc_eligible` UHNW clients receive actual BDC allocations in holdings. This table is profile-level — joined to clients via `clients.risk_profile`.

---

### S4. Holdings (`{catalog}.{schema}.holdings`)

**Primary Key:** `account_id` + `ticker`  
**Generated by:** `synthetic/04_holdings.py`; marked to market daily by `synthetic/06_update.py`  
**Single-date snapshot:** Positions as of the latest date in `bronze_historical_prices`.  
**Demo component:** Per-account position view; Agent 2 cross-account exposure scan.

| Field | Description |
|---|---|
| `account_id` | Account reference |
| `ticker` | Security ticker (e.g., "AINV", "SPY") or "CASH" |
| `asset_class` | Equity / Fixed Income / ETF / Alternatives / Private Credit / Cash |
| `quantity` | Units held (shares, or dollar amount for CASH) |
| `price` | Current price per share (latest `adjClose`) |
| `market_value` | `quantity × price` |
| `cost_basis_per_share` | Price at or before account inception date (historical `adjClose`) |
| `total_cost_basis` | `quantity × cost_basis_per_share` |
| `unrealized_gl` | `quantity × (price − cost_basis_per_share)` — negative values identify tax-loss harvesting candidates |
| `date` | Price date (latest in `bronze_historical_prices`) |
| `ingested_at` | Timestamp |

**Holdings construction:**
- **Equity:** 8–24 names from `equity_universe`; Dirichlet-weighted within IPS budget
- **ETF (broad/sector):** 4–9 broad ETFs (SPY, QQQ, IWM, sector ETFs, etc.)
- **Fixed Income:** 3–6 fixed income ETFs (AGG, TLT, LQD, HYG, JNK, EMB, BIL, SHY, BKLN)
- **Alternatives:** 2–4 real-asset ETFs (GLD, SLV, DBC, VNQ)
- **Private Credit / BDC:** 1–3 BDC tickers; only `bdc_eligible` accounts; hard cap 4% of account AUM
- **Cash:** IPS target + rounding remainder; hard cap at IPS `max_allocation_pct`
- ~10% of accounts have at least one asset class outside IPS min/max band (Gaussian drift applied to targets)

**Why it matters:** `SELECT account_id FROM holdings WHERE ticker = 'AINV'` is Agent 2's entire function — identifying which accounts are exposed to the name approaching covenant breach. Negative `unrealized_gl` identifies tax-loss harvesting candidates that Agent 3 includes in reallocation proposals.

---

### S5. Transactions (`{catalog}.{schema}.transactions`)

**Primary Key:** `trade_id` (UUID)  
**Generated by:** `synthetic/05_transactions.py`; new quarterly entries appended by `synthetic/06_update.py`  
**Demo component:** Transaction history for P&L attribution, income tracking, and fee reporting.

| Field | Description |
|---|---|
| `trade_id` | UUID (unique per row) |
| `date` | Transaction date |
| `account_id` | Account reference |
| `ticker` | Security ticker, "CASH", or "ADVISORY_FEE" |
| `action` | BUY / DIVIDEND / DRIP / FEE |
| `quantity` | Shares (BUY/DRIP), shares held at payment date (DIVIDEND), 1.0 (FEE) |
| `price` | Price per share (`adjClose` at trade date); 1.0 for CASH and FEE |
| `gross_amount` | Dollar value of trade (always positive) |
| `fee_amount` | Commission (0.05% of gross for BUY; 0.0 for all others) |
| `net_amount` | Net cash flow — negative for BUY/DRIP/FEE (outflows); positive for DIVIDEND (inflow) |
| `ingested_at` | Timestamp |

**Transaction types:**
- **BUY:** Each holding established via 3–5 tranches spread over the first 90 days after account inception; priced from `bronze_historical_prices` at trade date.
- **DIVIDEND:** Quarterly (Jan/Apr/Jul/Oct quarter-starts). Yield sourced from `bronze_financial_ratios.dividendYield` (actual) or 1.5% default for ETFs; non-dividend-paying equities (yield ≤ 0) skipped.
- **DRIP:** Dividend reinvestment — whole shares purchased from each DIVIDEND payment at zero commission (`int(dividend_amount / price)` shares).
- **FEE:** Quarterly GS PWM advisory fee; `account_aum × fee_rate / 4`; rate is 0.75%–1.25% annualized, deterministic per account.
- **CASH BUY:** Single transaction on inception date representing the initial cash deposit.

**Why it matters:** Transactions are the authoritative source of record. `07_validate_and_rebuild_holdings.py` reconstructs `holdings` from `transactions` as an integrity check. The ledger supports income reporting (DIVIDEND aggregates), fee attribution (FEE rows), and weighted-average cost basis reconciliation (BUY + DRIP).

---

### Synthetic Maintenance Notebooks

**`synthetic/06_update.py` — Daily mark-to-market**  
Run after each `bronze_historical_prices` refresh. Two steps:
1. **Append new transactions:** Detects quarters elapsed since the last DIVIDEND/FEE date and appends new DIVIDEND, DRIP, and FEE rows — BUY history is never modified.
2. **Mark holdings to market:** Updates `price`, `market_value`, and `unrealized_gl` from the latest prices; `quantity` and `cost_basis` columns are unchanged.

**`synthetic/07_validate_and_rebuild_holdings.py` — Holdings reconciliation**  
On-demand integrity tool. Reconstructs `holdings` from the `transactions` ledger: position quantities from BUY + DRIP sums; weighted-average cost basis from BUY tranches; CASH rebuilt from initial deposit + net DIVIDEND − FEE − DRIP outflows. Overwrites `holdings` with the reconciled values.

---

### C1. Advisor Communication History (planned — not yet generated)

**Format:** Small text dataset — 3–4 sample emails per advisor  
**Demo component:** Agent 3 tone mimicry for client communication drafts

| Field | Description |
|---|---|
| `advisor_id` | Advisor reference |
| `example_email_text` | Verbatim past email (sanitized) |
| `tone_label` | Formal / Conversational / Relationship-first |
| `communication_context` | Situation that prompted the email |

---

### S6. Asset Master & Performance (planned — not yet generated)

**Primary Key:** `asset_id`  
**Demo component:** Top-line KPIs (AUM, Net Flows, Revenue Yield, Alpha)

| Field | Description |
|---|---|
| `asset_id` | Ticker or internal ID |
| `asset_name` | Display name |
| `asset_class` | Equity / Fixed Income / ETF / Alternative / Private Credit |
| `ytd_return_pct` | Year-to-date return |
| `ytd_alpha_bps` | Alpha vs. benchmark in basis points |
| `net_flows_ytd` | Net new assets YTD |
| `revenue_yield_bps` | Fee revenue in basis points |
| `benchmark_id` | Reference benchmark ticker (e.g., "SPY", "AGG") |
| `gp_name` | For alternatives: general partner name |
| `vintage_year` | For PE funds: vintage year |
| `dpi` | Distributions to paid-in (PE positions) |
| `net_irr` | Net IRR (PE positions) |
| `moic` | Multiple on invested capital (PE positions) |

---

## Part 4 — Lakehouse Data (`{catalog}.{schema}` in Unity Catalog)

All tables live in the configured catalog and schema. Bronze tables are built by the `ingest_data/` notebooks from the raw volume files. Two ingestion patterns are used:

- **Overwrite** (`CREATE OR REPLACE TABLE` + `df.write.mode("overwrite")`) — used for full-refresh sources where every run replaces the entire table.
- **Autoloader** (`spark.readStream.format("cloudFiles")` + `trigger(availableNow=True)`) — used for append-only sources where new files must be merged without reprocessing the full history. Checkpoints are stored at `UC_VOLUME_PATH/_checkpoints/{table_name}`.

---

### B1. `bronze_historical_prices`

**Source volume:** `raw_fmapi/historical_prices/{TICKER}/*.json`  
**Built by:** `ingest_data/01_historical_prices.py`  
**Ingestion pattern:** Overwrite (full refresh on every run)  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker symbol |
| `date` | DATE | Trading date |
| `adjOpen` | DOUBLE | Dividend-adjusted open price |
| `adjHigh` | DOUBLE | Dividend-adjusted intraday high |
| `adjLow` | DOUBLE | Dividend-adjusted intraday low |
| `adjClose` | DOUBLE | Dividend-adjusted close price |
| `volume` | LONG | Shares traded |
| `ingested_at` | STRING | ISO 8601 fetch timestamp |

---

### B2. `bronze_company_profiles`

**Source volume:** `raw_fmapi/company_profiles/{TICKER}/*.json`  
**Built by:** `ingest_data/02_company_profiles.py`  
**Ingestion pattern:** Overwrite (full refresh on every run)  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker symbol |
| `companyName` | STRING | Legal entity name |
| `price` | DOUBLE | Current market price |
| `marketCap` | LONG | Market capitalization |
| `beta` | DOUBLE | Market beta |
| `sector` | STRING | GICS sector |
| `industry` | STRING | GICS industry sub-sector |
| `description` | STRING | Business description |
| `ceo` | STRING | CEO name |
| `cik` | STRING | SEC CIK (10-digit, zero-padded) |
| `isin` | STRING | ISIN |
| `cusip` | STRING | CUSIP |
| `exchange` | STRING | Exchange short name |
| `exchangeFullName` | STRING | Full exchange name |
| `country` | STRING | Country code |
| `currency` | STRING | Reporting currency |
| `isEtf` | BOOLEAN | ETF flag |
| `isActivelyTrading` | BOOLEAN | Active trading flag |
| `ipoDate` | STRING | IPO date |
| `fullTimeEmployees` | STRING | Headcount |
| `ingested_at` | STRING | Fetch timestamp |

---

### B3. `bronze_income_statements`

**Source volume:** `raw_fmapi/financials/{TICKER}/*_income_statements.json`  
**Built by:** `ingest_data/03_financials.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Period end date |
| `fiscalYear` | STRING | Fiscal year |
| `period` | STRING | "Q1"–"Q4" |
| `revenue` | DOUBLE | Total revenue |
| `grossProfit` | DOUBLE | Gross profit |
| `ebitda` | DOUBLE | EBITDA — leverage covenant denominator |
| `ebit` | DOUBLE | EBIT |
| `operatingIncome` | DOUBLE | Operating income |
| `netIncome` | DOUBLE | Net income |
| `interestExpense` | DOUBLE | Interest expense — coverage covenant input |
| `eps` | DOUBLE | Basic EPS |
| `epsDiluted` | DOUBLE | Diluted EPS |
| `cik` | STRING | SEC CIK |
| `filingDate` | STRING | SEC filing date |
| `ingested_at` | STRING | Fetch timestamp |

---

### B4. `bronze_balance_sheets`

**Source volume:** `raw_fmapi/financials/{TICKER}/*_balance_sheets.json`  
**Built by:** `ingest_data/03_financials.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Period end date |
| `fiscalYear` | STRING | Fiscal year |
| `period` | STRING | Quarter |
| `totalAssets` | DOUBLE | Total assets |
| `totalLiabilities` | DOUBLE | Total liabilities |
| `totalStockholdersEquity` | DOUBLE | Book equity |
| `cashAndCashEquivalents` | DOUBLE | Cash and equivalents |
| `totalDebt` | DOUBLE | Total debt (short + long) |
| `longTermDebt` | DOUBLE | Long-term debt |
| `shortTermDebt` | DOUBLE | Current portion of LTD |
| `netDebt` | DOUBLE | Total debt minus cash — covenant numerator |
| `goodwillAndIntangibleAssets` | DOUBLE | Goodwill + intangibles |
| `retainedEarnings` | DOUBLE | Retained earnings |
| `ingested_at` | STRING | Fetch timestamp |

---

### B5. `bronze_cash_flows`

**Source volume:** `raw_fmapi/financials/{TICKER}/*_cash_flows.json`  
**Built by:** `ingest_data/03_financials.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Period end date |
| `fiscalYear` | STRING | Fiscal year |
| `period` | STRING | Quarter |
| `operatingCashFlow` | DOUBLE | Cash from operations |
| `capitalExpenditure` | DOUBLE | Capex (negative) |
| `freeCashFlow` | DOUBLE | Operating CF + Capex — DSCR proxy input |
| `depreciationAndAmortization` | DOUBLE | D&A |
| `netDividendsPaid` | DOUBLE | Dividends paid |
| `ingested_at` | STRING | Fetch timestamp |

---

### B6. `bronze_income_growth` / `bronze_balance_growth` / `bronze_cashflow_growth`

**Source volumes:** `raw_fmapi/financials/{TICKER}/*_income_growth.json`, `*_balance_growth.json`, `*_cashflow_growth.json`  
**Built by:** `ingest_data/03_financials.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

Same `(symbol, date, fiscalYear, period)` key as the base statements. All value columns are YoY % change fields prefixed with `growth` (e.g., `growthRevenue`, `growthEBITDA`, `growthNetDebt`, `growthFreeCashFlow`). Used for trend analysis without requiring self-joins on base tables.

---

### B7. `bronze_key_metrics`

**Source volume:** `raw_fmapi/key_metrics/{TICKER}/*_key_metrics.json`  
**Built by:** `ingest_data/04_key_metrics.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Period end date |
| `fiscalYear` | STRING | Fiscal year |
| `period` | STRING | Quarter |
| `netDebtToEBITDA` | DOUBLE | Primary leverage covenant proxy |
| `evToEBITDA` | DOUBLE | EV / EBITDA multiple |
| `currentRatio` | DOUBLE | Liquidity ratio |
| `returnOnEquity` | DOUBLE | ROE |
| `returnOnAssets` | DOUBLE | ROA |
| `returnOnInvestedCapital` | DOUBLE | ROIC |
| `marketCap` | DOUBLE | Market cap |
| `enterpriseValue` | DOUBLE | Enterprise value |
| `earningsYield` | DOUBLE | Inverse P/E |
| `freeCashFlowYield` | DOUBLE | FCF / market cap |
| `workingCapital` | DOUBLE | Working capital |
| `ingested_at` | STRING | Fetch timestamp |

---

### B8. `bronze_financial_ratios`

**Source volume:** `raw_fmapi/key_metrics/{TICKER}/*_financial_ratios.json`  
**Built by:** `ingest_data/04_key_metrics.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Period end date |
| `fiscalYear` | STRING | Fiscal year |
| `period` | STRING | Quarter |
| `netProfitMargin` | DOUBLE | Net income / revenue |
| `grossProfitMargin` | DOUBLE | Gross profit / revenue |
| `ebitdaMargin` | DOUBLE | EBITDA / revenue |
| `operatingProfitMargin` | DOUBLE | EBIT / revenue |
| `returnOnEquity` | DOUBLE | ROE |
| `returnOnAssets` | DOUBLE | ROA |
| `debtToAssetsRatio` | DOUBLE | Total debt / total assets |
| `interestCoverageRatio` | DOUBLE | EBIT / interest expense — secondary covenant proxy |
| `debtServiceCoverageRatio` | DOUBLE | DSCR |
| `currentRatio` | DOUBLE | Current assets / current liabilities |
| `quickRatio` | DOUBLE | (Current assets − inventory) / current liabilities |
| `freeCashFlowPerShare` | DOUBLE | FCF per share |
| `dividendYield` | DOUBLE | Dividend yield |
| `enterpriseValueMultiple` | DOUBLE | EV / EBITDA |
| `priceToEarningsRatio` | DOUBLE | P/E |
| `priceToBookRatio` | DOUBLE | P/B |
| `ingested_at` | STRING | Fetch timestamp |

---

### B9. `bronze_etf_info`

**Source volume:** `raw_fmapi/etf_data/{TICKER}/*_etf_info.json`  
**Built by:** `ingest_data/06_etf_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | ETF ticker |
| `name` | STRING | Full fund name |
| `description` | STRING | Fund objective/mandate |
| `assetClass` | STRING | Asset class (e.g., "Equity", "Fixed Income") |
| `assetsUnderManagement` | DOUBLE | AUM |
| `expenseRatio` | DOUBLE | Annual fee as decimal |
| `nav` | DOUBLE | Net asset value |
| `holdingsCount` | LONG | Number of underlying securities |
| `isActivelyTrading` | BOOLEAN | Active trading flag |
| `sectorsList` | ARRAY<STRUCT<industry:STRING, exposure:DOUBLE>> | Sector breakdown |
| `ingested_at` | STRING | Fetch timestamp |

---

### B10. `bronze_etf_holdings`

**Source volume:** `raw_fmapi/etf_data/{TICKER}/*_etf_holdings.json`  
**Built by:** `ingest_data/06_etf_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `etf_symbol` | STRING | Parent ETF ticker |
| `symbol` | STRING | Underlying security ticker |
| `asset` | STRING | Asset name |
| `name` | STRING | Security name |
| `isin` | STRING | ISIN |
| `weightPercentage` | DOUBLE | Portfolio weight (%) |
| `marketValue` | DOUBLE | Dollar value in ETF |
| `sharesNumber` | DOUBLE | Number of shares held |
| `ingested_at` | STRING | Fetch timestamp |

---

### B11. `bronze_etf_sectors`

**Source volume:** `raw_fmapi/etf_data/{TICKER}/*_etf_sectors.json`  
**Built by:** `ingest_data/06_etf_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `etf_symbol` | STRING | Parent ETF ticker |
| `symbol` | STRING | ETF ticker (repeated for join convenience) |
| `sector` | STRING | Sector name |
| `weightPercentage` | DOUBLE | Allocation (%) |
| `ingested_at` | STRING | Fetch timestamp |

---

### B12. `bronze_analyst_estimates`

**Source volume:** `raw_fmapi/analyst_data/{TICKER}/*_analyst_estimates.json`  
**Built by:** `ingest_data/07_analyst_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `date` | DATE | Estimate as-of date |
| `revenueAvg` / `revenueLow` / `revenueHigh` | DOUBLE | Consensus revenue forecast + range |
| `ebitdaAvg` / `ebitdaLow` / `ebitdaHigh` | DOUBLE | Forward EBITDA — covenant headroom projection |
| `ebitAvg` / `ebitLow` / `ebitHigh` | DOUBLE | Forward EBIT range |
| `netIncomeAvg` / `netIncomeLow` / `netIncomeHigh` | DOUBLE | Forward net income range |
| `epsAvg` / `epsLow` / `epsHigh` | DOUBLE | Consensus EPS + range |
| `numAnalystsRevenue` | LONG | Number of revenue estimates |
| `numAnalystsEps` | LONG | Number of EPS estimates |
| `ingested_at` | STRING | Fetch timestamp |

---

### B13. `bronze_price_targets`

**Source volume:** `raw_fmapi/analyst_data/{TICKER}/*_price_targets.json`  
**Built by:** `ingest_data/07_analyst_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `targetConsensus` | DOUBLE | Mean analyst price target |
| `targetMedian` | DOUBLE | Median target |
| `targetHigh` | DOUBLE | Bull case target |
| `targetLow` | DOUBLE | Bear case target |
| `ingested_at` | STRING | Fetch timestamp |

---

### B14. `bronze_analyst_ratings`

**Source volume:** `raw_fmapi/analyst_data/{TICKER}/*_analyst_ratings.json`  
**Built by:** `ingest_data/07_analyst_data.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `strongBuy` / `buy` / `hold` / `sell` / `strongSell` | LONG | Analyst counts per rating bucket |
| `consensus` | STRING | Overall label (e.g., "Buy", "Hold") |
| `ingested_at` | STRING | Fetch timestamp |

---

### B15. `bronze_stock_news`

**Source volume:** `raw_fmapi/stock_news/{TICKER}/*.json`  
**Built by:** `ingest_data/08_news.py`  
**Ingestion pattern:** Autoloader (append-only — new files added incrementally, checkpoint at `raw_fmapi/_checkpoints/bronze_stock_news`)  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `url` | STRING | Article hyperlink |
| `publishedDate` | STRING | Publication timestamp |
| `title` | STRING | Headline |
| `summary` | STRING | Article snippet (~100–200 words) |
| `full_text` | STRING | Full scraped article body |
| `site` | STRING | Publication name |
| `sentiment` | STRING | Sentiment label |
| `publisher` | STRING | Publisher name |
| `ingested_at` | STRING | Fetch timestamp |

---

### B16. `bronze_indexes_and_vix`

**Source volume:** `raw_fmapi/indexes/{SYMBOL_CLEAN}/*.json`  
**Built by:** `ingest_data/09_indexes_and_vix.py`  
**Ingestion pattern:** Overwrite  
**Refresh cadence:** Daily

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Index symbol (e.g., "^GSPC", "^VIX") |
| `index` | STRING | Index name label |
| `date` | DATE | Trading date |
| `open` | DOUBLE | Open |
| `high` | DOUBLE | Intraday high |
| `low` | DOUBLE | Intraday low |
| `close` | DOUBLE | Close |
| `volume` | LONG | Session volume |
| `change` | DOUBLE | Absolute point change |
| `changePercent` | DOUBLE | Daily % change |
| `vwap` | DOUBLE | Volume-weighted average price |
| `ingested_at` | STRING | Fetch timestamp |

---

### B17. `bronze_transcripts`

**Source volume:** `raw_fmapi/transcripts/{TICKER}/Q{q}_{year}.json`  
**Built by:** `ingest_data/10_transcripts.py`  
**Ingestion pattern:** Autoloader (append-only — new quarters added incrementally, checkpoint at `raw_fmapi/_checkpoints/bronze_transcripts`)  
**Refresh cadence:** Monthly

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `year` | INT | Fiscal year |
| `quarter` | INT | Quarter (1–4) |
| `date` | STRING | Earnings call date |
| `title` | STRING | Call title/description |
| `content` | STRING | Full transcript text (often 10,000+ words) |

---

### B18. `bronze_financial_reports`

**Source volume:** `raw_fmapi/financial_reports/{TICKER}/{year}_{period}.json`  
**Built by:** `ingest_data/11_financial_reports.py`  
**Ingestion pattern:** Autoloader (append-only — new reports added incrementally, checkpoint at `raw_fmapi/_checkpoints/bronze_financial_reports`; schema inferred because section keys are dynamic)  
**Refresh cadence:** Monthly

Schema is flexible (inferred by Autoloader with `mergeSchema=true`) because FMP financial report JSON files use dynamic section names as keys (e.g., "Cover Page", "CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS"). Core fields include `symbol`, `year`, `period`, plus nested financial statement and ratio arrays. Full schema available via `DESCRIBE TABLE {catalog}.{schema}.bronze_financial_reports`.

---

### SEC Processing Tables

These tables are produced by a separate SEC parsing pipeline that processes the raw HTML filing documents from `raw_fmapi/sec_filings/` into structured, queryable chunks for LLM retrieval.

#### `sec_filing_chunks`

Parsed and chunked SEC filing text. Each row is one chunk of a filing section.

| Column | Type | Description |
|---|---|---|
| `chunk_id` | STRING | Unique chunk identifier (UUID) |
| `symbol` | STRING | Ticker |
| `form_type` | STRING | "10-K", "10-Q", "8-K", etc. |
| `filing_date` | STRING | Date filed with SEC |
| `accession` | STRING | EDGAR accession number |
| `section_name` | STRING | Section heading (e.g., "Risk Factors", "MD&A") |
| `chunk_index` | INT | Sequential position within the section |
| `chunk_text` | STRING | Chunk text content |
| `char_count` | INT | Character count of chunk |
| `parsed_at` | TIMESTAMP | Parse timestamp |

#### `sec_filing_index` (FOREIGN — Vector Search Index)

Mosaic AI Vector Search index over `sec_filing_chunks`. Table type is FOREIGN, managed by the Vector Search service. Contains all columns from `sec_filing_chunks` plus:

| Column | Type | Description |
|---|---|---|
| `__db_chunk_text_vector` | ARRAY<FLOAT> | Embedding vector for `chunk_text` |

Used for semantic similarity search: given a query like "covenant breach" or "non-accrual designation", returns the most relevant filing chunks across all tickers and form types.

#### `_sec_chunks_staging`

Temporary staging table used during the SEC parsing pipeline to accumulate chunks before upsert into `sec_filing_chunks`. Shares the same schema as `sec_filing_chunks`.

#### `sec_parsed_log`

Tracks which filings have been parsed to avoid re-processing.

| Column | Type | Description |
|---|---|---|
| `symbol` | STRING | Ticker |
| `form_type` | STRING | Filing type |
| `accession` | STRING | EDGAR accession number |
| `filename` | STRING | Source .htm filename |
| `sections_found` | STRING | Comma-separated list of sections extracted |
| `chunks_written` | INT | Number of chunks written |
| `parsed_at` | TIMESTAMP | Parse timestamp |

---

### Ingestion Log Tables

Four Delta tables track incremental download state for sources where re-fetching is expensive. Described fully in the Raw Data sections above; listed here for completeness.

| Table | Tracks | Natural Key |
|---|---|---|
| `sec_filings_log` | HTML filing downloads from EDGAR | `symbol + accession` |
| `stock_news_log` | News article fetch status (including errors for retry) | `symbol + url` |
| `transcripts_log` | Earnings call transcript downloads | `symbol + year + quarter` |
| `financial_reports_log` | Structured financial report JSON downloads | `symbol + fiscal_year + period` |

---

### Silver & Gold Layers (Planned)

No Silver or Gold tables exist yet. Planned transformations include:

- **Silver** — Cleansed, normalized, and type-corrected views of bronze tables. Joining `bronze_company_profiles` with `bronze_key_metrics` to produce a `silver_holdings_enriched` view; normalizing `bronze_transcripts` content for LLM chunking; standardizing date formats across all financial statement tables.
- **Gold** — Cross-source aggregations for agent consumption. `gold_covenant_scorecard` joining key metrics, analyst estimates, and BDC XBRL signals; `gold_client_exposure` joining synthetic holdings with enriched asset data; `gold_advisor_workspace` as the unified view powering the Genie workspace and dashboard KPIs.

---

## Pipeline Architecture

### Unity Catalog Volume Layout

All raw data lands under a single UC Volume root:

```
/Volumes/{uc_catalog}/{uc_schema}/raw_fmapi/
├── company_profiles/       {TICKER}/{ts}_profile.json
├── historical_prices/      {TICKER}/{ts}_prices.json
├── financials/             {TICKER}/{ts}_{statement}.json   (6 files)
├── key_metrics/            {TICKER}/{ts}_{type}.json        (2 files)
├── sec_filings/            {form_dir}/{TICKER}/{FORM}_{date}_{accession}.htm
├── etf_data/               {TICKER}/{ts}_{type}.json        (3 files)
├── analyst_data/           {TICKER}/{ts}_{type}.json        (3 files)
├── stock_news/             {TICKER}/{date}_{url_hash}.json
├── indexes/                {SYMBOL_CLEAN}/{ts}_history.json
├── transcripts/            {TICKER}/Q{q}_{year}.json
├── financial_reports/      {TICKER}/{fiscal_year}_{period}.json
└── bdc_early_warning/      bdc_time_series.csv
                            bdc_fy_snapshot.csv
```

Timestamp prefix format: `YYYY_MM_DD_HH` (hour-level, generated by `ts_prefix()` in `ingest_config.py`).

### Delta Log Tables

Four tables in `{uc_catalog}.{uc_schema}` track incremental downloads for the four notebooks where re-fetching is expensive:

| Table | Notebook | Natural key | Notes |
|---|---|---|---|
| `sec_filings_log` | 05_sec_filings | `symbol + accession` | Tracks HTML download success |
| `stock_news_log` | 08_news | `symbol + url` | Tracks success *and* errors (retries errors on re-run) |
| `transcripts_log` | 10_transcripts | `symbol + year + quarter` | Skips already-downloaded quarters |
| `financial_reports_log` | 11_financial_reports | `symbol + fiscal_year + period` | Skips already-downloaded reports |

All four use the `MERGE INTO ... WHEN MATCHED THEN UPDATE / WHEN NOT MATCHED THEN INSERT` pattern.

### Idempotency Summary

| Notebook | Strategy | Trigger | Cadence |
|---|---|---|---|
| 01 company_profiles | 30-day file recency check | `mtime` of existing file | Monthly |
| 02 historical_prices | Full refresh | Every run | Daily |
| 03 financials | Full refresh | Every run | Monthly |
| 04 key_metrics | Full refresh | Every run | Monthly |
| 05 sec_filings | Delta MERGE on accession | Log table | Monthly |
| 06 etf_data | Full refresh | Every run | Monthly |
| 07 analyst_data | Full refresh | Every run | Daily |
| 08 news | Delta MERGE on URL | Log table (retries errors) | Daily |
| 09 indexes_and_vix | Full refresh | Every run | Daily |
| 10 transcripts | Delta MERGE on year+quarter | Log table | Monthly |
| 11 financial_reports | Delta MERGE on fiscal_year+period | Log table | Monthly |
| E1 bdc_early_warning | Full refresh | Directory cleared at start | Monthly |

### Job Execution Order

Defined in `databricks.yml` and deployed via Databricks Asset Bundles to e2-demo. Two separate jobs with independent schedules:

```
──────────────────────────────────────────────────────────────────────
GS AWM | Data Ingest | Daily  (6:00 AM UTC, Mon–Fri)
──────────────────────────────────────────────────────────────────────
Sequential (no log table):
  02_historical_prices → 07_analyst_data → 09_indexes_and_vix

Parallel fan-out after 09 (log table):
  08_news ─── depends on fmapi_09_indexes_and_vix

──────────────────────────────────────────────────────────────────────
GS AWM | Data Ingest | Monthly  (3:00 AM UTC, 1st of month)
──────────────────────────────────────────────────────────────────────
Sequential (no log table):
  01_company_profiles → 03_financials → 04_key_metrics → 06_etf_data

Parallel fan-out after 06 (log table):
  05_sec_filings    ─┐
  10_transcripts     ├─ all depend on fmapi_06_etf_data
  11_financial_reports ─┘

Independent (run in parallel with FMAPI chain throughout):
  factset_01_filings
  edgar_01_bdc_early_warning
```

---

## Source-to-Component Mapping

| Demo Component | FMP Real Data | EDGAR Real Data | Synthetic |
|---|---|---|---|
| **Portfolio Dashboard** | F1 Profile, F2 Prices, F4 Key Metrics, F6 ETF, F9 Indexes/VIX | — | S4 Holdings, S3 IPS Targets, S2 Accounts |
| **Document Intelligence** | F5 SEC Filings (HTML), F10 Transcripts, F11 Financial Reports JSON, F3 Financials | E1 BDC time series / snapshot | S1 Clients (scoping) |
| **Agent 1 — Covenant Detection** | F4 `netDebtToEBITDA`, `interestCoverage`; F5 8-K triggers; F7 Forward EBITDA estimates | E1 PIK, NII, NAV/share, unrealized deprec | S6 Asset Master covenant thresholds (planned) |
| **Agent 2 — Cross-Account Exposure** | F1 Profile (name resolution) | — | S4 Holdings (`ticker` cross-reference), S2 Accounts |
| **Agent 3 — Client Communication** | F8 News, F7 Ratings & targets, F9 VIX (market context) | — | S1 Clients (`tone_profile`), C1 Advisor history (planned), S4 Holdings (`unrealized_gl` for TLH) |
| **Genie Chat** | All FMP sources + EDGAR BDC metrics | E1 BDC snapshot | S3 IPS Targets, S4 Holdings, S5 Transactions, S2 Accounts |

---

## Key Metrics Glossary

| Term | Definition | Source |
|---|---|---|
| `netDebtToEBITDA` | Net debt / trailing EBITDA — primary leverage covenant | F4 key-metrics |
| `interestCoverage` | EBIT / interest expense — secondary covenant proxy | F4 key-metrics |
| **YTD Alpha (bps)** | Holding `changeOverTime` minus `^GSPC` `changeOverTime` from same start date | F2 vs. F9 |
| **Covenant Headroom** | Cushion before breaching threshold, e.g., "0.3x to limit" | F4 `netDebtToEBITDA` vs. threshold |
| **PIK** | Payment-In-Kind interest — accrued but not received in cash; BDC stress signal | E1 `pik` (XBRL) |
| **NII** | Net Investment Income — BDC earnings proxy and dividend coverage metric | E1 `nii` (XBRL) |
| **NAV/Share** | Net Asset Value per share — BDC intrinsic value benchmark | E1 `nav_ps` (XBRL) |
| **IPS Target** | Legally binding allocation target per risk profile per asset class | S3 `target_allocation_pct` |
| **Allocation Drift** | Current weight − IPS target | S4 vs. S3 |
| **TLH** | Tax-Loss Harvesting — selling a position at a loss for tax benefit | S4 `unrealized_gl` (negative = loss position) |
| **DPI / Net IRR / MOIC** | PE performance metrics: distributions/paid-in, net internal rate of return, multiple on invested capital | S6 Asset Master (planned) |

---

## Coverage Gaps

| Asset Class | Coverage | Gap / Approach |
|---|---|---|
| Public equities | Full via FMP | None |
| ETFs (31 funds) | Full via FMP `/etf/*` endpoints | None |
| Major indices + VIX | Full via FMP `/historical-price-eod/full` | None |
| BDCs (16 tickers) | FMP for market data; EDGAR XBRL for fund-specific metrics | XBRL availability varies by filer; some older periods may be missing |
| Corporate bonds | No bond price feed in FMP | Represent as positions in `daily_holdings`; use issuer equity data for intelligence |
| Private credit (bilateral loans) | None in public sources | Fully synthetic via `asset_master` + covenant fields |
| Private equity / alternatives | None in public sources | Fully synthetic — DPI, IRR, MOIC in `asset_master` |
| Client / accounts / IPS / holdings / transactions | None in public sources | Fully synthetic — clients, accounts, ips_targets, holdings, transactions tables |
| Advisor communication history | None in public sources | Planned — manually authored (3–4 sample emails per advisor) |
