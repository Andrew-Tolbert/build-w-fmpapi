# Demo Data Sources

**Goldman EBC 5/12/26 — Art of the Possible**  
**Source:** Financial Modeling Prep (FMP) API + SEC EDGAR + Synthetic (Faker)

---

## Overview

Data for this demo falls into three layers:

| Layer | Source | Rationale |
|---|---|---|
| **Unstructured Documents** | Real — SEC EDGAR / FMP | Real financial jargon makes citation features credible. LLM extracts genuine covenant language, not fake text. |
| **Market & Financial Data** | Real — FMP API | Live prices, ratios, and transcripts for the same BDC names used in documents. |
| **Client & Portfolio Intelligence** | Synthetic — Python Faker + notebooks | No public source for client IPS targets, holdings allocations, or advisor tone. Generated as Delta tables and registered in Unity Catalog. |

**Anchor securities for the demo:** Apollo Investment Corp (`AINV`) and Oaktree Specialty Lending (`OCSL`) are the two named private credit vehicles. All document intelligence and covenant detection is built around these tickers.

---

## Part 1 — Real Unstructured Data (Document Intelligence Layer)

Stored as PDFs or plain text files in a **Unity Catalog Volume** (e.g., `uc.wealth.documents`).

---

### D1. Quarterly Financial Filings (10-Q, 8-K)

**Source:** SEC EDGAR directly or via FMP `/stable/sec-filings` endpoint  
**Tickers:** `AINV` (Apollo Investment Corp), `OCSL` (Oaktree Specialty Lending)  
**Primary Key:** `ticker` + `filing_type` + `period_of_report`

| Field | Description |
|---|---|
| `ticker` | Issuer symbol |
| `filing_type` | 10-Q or 8-K |
| `period_of_report` | Quarter end date |
| `filing_date` | Date filed with SEC |
| `document_url` | Direct link to full filing text |
| `raw_text` / `pdf_path` | Local path in UC Volume |

**Why it matters:** These are the source documents the Research Agent (Agent 1) processes. BDC 10-Qs contain loan-level schedules that explicitly state borrower names, principal outstanding, interest rates, and fair value marks — the raw material for covenant headroom extraction. 8-Ks carry material events (non-accrual designations, PIK toggles) that trigger the agent chain.

---

### D2. Credit Agreements / CIMs

**Source:** Real redacted credit agreements or sample loan documents  
**Primary Key:** `agreement_id` + `borrower_name`

| Field | Description |
|---|---|
| `agreement_id` | Internal document identifier |
| `borrower_name` | Portfolio company name |
| `facility_type` | Term Loan B, Revolver, Second Lien |
| `document_date` | Agreement execution date |
| `covenant_clause_text` | Raw clause text, e.g. "Maximum Total Debt to EBITDA shall not exceed 3.50 to 1.00" |
| `pdf_path` | UC Volume path |

**Why it matters:** CIMs and credit agreements are where explicit numeric covenant thresholds live. The advisor clicking into a private credit position should see: "Covenant limit: 3.50x — current: 3.20x — headroom: 0.30x" with the actual clause text citation-linked. Real documents make this completely credible.

---

### D3. Earnings Call Transcripts (Document Store)

**Source:** FMP API — `GET /stable/earning-call-transcript?symbol={ticker}&year={year}&quarter={quarter}`  
**Tickers:** `AINV`, `OCSL`  
**Primary Key:** `symbol` + `year` + `quarter`

| Field | Description |
|---|---|
| `symbol` | Ticker |
| `quarter` / `year` | Earnings period |
| `date` | Call date |
| `content` | Full transcript text |

**Why it matters:** Powers the "Management Tone" sentiment analysis. The delta between two consecutive transcripts — tone shifting from "comfortable liquidity position" to "monitoring select credits closely" — is the signal the system extracts and presents to the advisor. 1–2 recent transcripts per fund are sufficient.

---

## Part 2 — Real Market & Financial Data (FMP API)

All endpoints are keyed by `symbol` (ticker). Base URL: `https://financialmodelingprep.com/stable/`

---

### F1. Company Profile

**Endpoint:** `GET /stable/profile?symbol={ticker}`  
**Primary Key:** `symbol`  
**Demo Component:** Portfolio Intelligence Dashboard — holding cards, asset master

| Field | Description |
|---|---|
| `symbol` / `companyName` | Ticker and legal name |
| `price` | Current market price |
| `mktCap` | Market capitalization |
| `beta` | Market volatility |
| `sector` / `industry` | Classification |
| `description` | Business description |
| `ceo` | Chief Executive |
| `country` / `exchangeShortName` | Domicile and exchange |
| `dcf` | DCF intrinsic value estimate |
| `isEtf` | ETF flag |

**Why it matters:** Foundation record for every displayed holding. Provides the name, sector bucket, and intrinsic value used in the bento grid cards.

---

### F2. Historical Price Data

**Endpoint:** `GET /stable/historical-price-eod/{symbol}?from={date}&to={date}`  
**Primary Key:** `symbol` + `date`  
**Demo Component:** Portfolio Intelligence Dashboard — YTD Alpha, performance vs. benchmark

| Field | Description |
|---|---|
| `date` | Trading date |
| `open` / `high` / `low` / `close` | OHLC |
| `adjClose` | Adjusted close |
| `volume` | Shares traded |
| `changePercent` | Daily % change |
| `changeOverTime` | Cumulative return from base date |

**Why it matters:** `changeOverTime` drives YTD Alpha when compared against a benchmark return series (e.g., S&P 500 via `SPY`). Also populates the time-series performance chart behind each holding card.

---

### F3. Income Statement

**Endpoint:** `GET /stable/income-statement?symbol={ticker}&period=annual&limit=5`  
**Primary Key:** `symbol` + `date` + `period`  
**Demo Component:** Document Intelligence Layer — KPI extraction, revenue delta

| Field | Description |
|---|---|
| `date` / `period` | Report date and period |
| `revenue` | Total revenue |
| `ebitda` | EBITDA |
| `operatingIncome` | Operating income |
| `netIncome` | Net income |
| `eps` / `epsDiluted` | Earnings per share |
| `interestExpense` | Interest expense (covenant input) |
| `grossProfitRatio` | Gross margin |

**Why it matters:** EBITDA is the denominator of the most common leverage covenant (Net Debt / EBITDA). Quarterly EBITDA trend from this table, combined with debt from the balance sheet, produces the `covenant_headroom` value the system monitors.

---

### F4. Balance Sheet

**Endpoint:** `GET /stable/balance-sheet-statement?symbol={ticker}&period=annual&limit=5`  
**Primary Key:** `symbol` + `date` + `period`  
**Demo Component:** Document Intelligence Layer — net leverage, covenant breach detection

| Field | Description |
|---|---|
| `totalDebt` | Total debt |
| `netDebt` | Net debt (debt minus cash) |
| `longTermDebt` | Long-term debt |
| `cashAndCashEquivalents` | Cash on hand |
| `totalAssets` / `totalLiabilities` | Balance totals |
| `totalStockholdersEquity` | Book equity |
| `goodwillAndIntangibleAssets` | Goodwill |
| `retainedEarnings` | Retained earnings |

**Why it matters:** `netDebt / EBITDA` is the live covenant metric. Fetching this quarterly for `AINV` and `OCSL` lets the agent reconstruct "headroom compressed from 0.7x to 0.3x" using real reported figures.

---

### F5. Cash Flow Statement

**Endpoint:** `GET /stable/cash-flow-statement?symbol={ticker}&period=annual&limit=5`  
**Primary Key:** `symbol` + `date` + `period`  
**Demo Component:** Document Intelligence Layer — DSCR, interest coverage proxy

| Field | Description |
|---|---|
| `operatingCashFlow` | Cash from operations |
| `freeCashFlow` | FCF (operating CF minus capex) |
| `capitalExpenditure` | Capex |
| `depreciationAndAmortization` | D&A |
| `dividendsPaid` | Dividends |
| `netCashUsedForInvestingActivities` | Investment activity |

**Why it matters:** Free cash flow relative to debt service gives a proxy DSCR. Declining FCF ahead of a debt maturity is a secondary early-warning signal the Research Agent surfaces alongside the leverage ratio.

---

### F6. Key Metrics

**Endpoint:** `GET /stable/key-metrics?symbol={ticker}&period=annual&limit=5`  
**Primary Key:** `symbol` + `date` + `period`  
**Demo Component:** Portfolio Dashboard risk panel; Agent 1 threshold scanning

| Field | Description |
|---|---|
| `netDebtToEBITDA` | **Primary covenant proxy** |
| `interestCoverage` | **Secondary covenant proxy (DSCR input)** |
| `debtToEquity` | Leverage ratio |
| `currentRatio` | Liquidity |
| `peRatio` | Valuation |
| `marketCap` / `enterpriseValue` | Size |
| `freeCashFlowPerShare` | FCF per share |
| `dividendYield` | Yield |
| `earningsYield` | Inverse P/E |

**Why it matters:** `netDebtToEBITDA` and `interestCoverage` are pre-computed here — Agent 1 can scan all holdings for these crossing alert thresholds in a single query without doing raw statement math. This is the trigger for the full multi-agent chain.

---

### F7. Financial Ratios

**Endpoint:** `GET /stable/ratios?symbol={ticker}&period=annual&limit=5`  
**Primary Key:** `symbol` + `date` + `period`  
**Demo Component:** Portfolio Dashboard — allocation drift flags, risk tier classification

| Field | Description |
|---|---|
| `netProfitMargin` / `grossProfitMargin` | Profitability |
| `returnOnEquity` / `returnOnAssets` | ROE, ROA |
| `debtRatio` / `debtEquityRatio` | Leverage |
| `currentRatio` / `quickRatio` | Liquidity |
| `priceEarningsRatio` / `priceToBookRatio` | Valuation multiples |
| `enterpriseValueMultiple` | EV/EBITDA |
| `operatingProfitMargin` | Operational efficiency |

**Why it matters:** Cross-holding ratio comparison enables the bento grid's risk-tier color coding. Deteriorating margins or rising leverage flag positions for advisor review before they become client-visible problems.

---

### F8. SEC Filings (Links & Metadata)

**Endpoint:** `GET /stable/sec-filings?symbol={ticker}&type=10-Q` (also `10-K`, `8-K`)  
**Primary Key:** `symbol` + `fillingDate` + `type`  
**Demo Component:** Document Intelligence Layer — filing ingestion trigger, material event detection

| Field | Description |
|---|---|
| `symbol` | Ticker |
| `fillingDate` / `acceptedDate` | Filing timestamps |
| `cik` | SEC CIK number |
| `type` | Filing type |
| `link` | EDGAR HTML link |
| `finalLink` | **Direct document URL for download and chunking** |

**Why it matters:** `finalLink` is what the ingestion pipeline uses to fetch and embed document chunks into the Unity Catalog Volume. 8-Ks are the real-time trigger — when `AINV` or `OCSL` files an 8-K flagging a non-accrual designation, Agent 1 fires immediately.

---

### F9. ETF Information

**Endpoint:** `GET /stable/etf-info?symbol={ticker}`  
**Primary Key:** `symbol` (e.g. `SPY`, `AGG`, `BKLN`, `HYG`)  
**Demo Component:** Portfolio Dashboard — ETF positions panel, benchmark data

| Field | Description |
|---|---|
| `symbol` / `name` | ETF identifier and name |
| `description` | Fund mandate |
| `expenseRatio` | Annual fee |
| `aum` | AUM |
| `ytdReturn` / `oneYearReturn` / `threeYearReturn` | Performance |
| `sectorsList` | Top sector exposures |
| `holdingsCount` | Underlying positions count |

**Why it matters:** ETFs appear as a first-class asset class in the portfolio. Performance fields (`ytdReturn`) feed the top-line AUM and Revenue Yield KPIs. `SPY` and `AGG` also serve as benchmark series for the YTD Alpha calculation.

---

### F10. ETF Holdings

**Endpoint:** `GET /stable/etf-holdings?symbol={ticker}`  
**Primary Key:** ETF `symbol` → underlying `asset`  
**Demo Component:** Portfolio Dashboard — ETF drill-down, look-through concentration risk

| Field | Description |
|---|---|
| `asset` | Underlying holding ticker |
| `name` | Holding name |
| `isin` / `cusip` | Security identifiers |
| `weightPercentage` | Portfolio weight |
| `marketValue` | Dollar value |

**Why it matters:** Look-through exposure: if a client holds two ETFs that both contain the same issuer, the system aggregates the real exposure. This is what powers the "top client concentration risks" metric at the top of the dashboard.

---

### F11. ETF Sector Weighting

**Endpoint:** `GET /stable/etf-sector-weightings?symbol={ticker}`  
**Primary Key:** ETF `symbol` + `sector`  
**Demo Component:** Portfolio Dashboard — allocation drift vs. IPS targets

| Field | Description |
|---|---|
| `sector` | Sector name |
| `weightPercentage` | Allocation % |

**Why it matters:** When a client's IPS specifies a 20% technology cap and ETF sector weights push them over, this table surfaces the drift. Joins directly to the synthetic `client_ips_targets` table.

---

### F12. Analyst Estimates

**Endpoint:** `GET /stable/analyst-estimates?symbol={ticker}`  
**Primary Key:** `symbol` + `date`  
**Demo Component:** Document Intelligence Layer — forward covenant projection

| Field | Description |
|---|---|
| `estimatedEbitdaLow` / `Avg` / `High` | EBITDA forecast range |
| `estimatedRevenueLow` / `Avg` / `High` | Revenue forecast range |
| `estimatedEpsLow` / `Avg` / `High` | EPS forecast |
| `numberAnalystEstimatedRevenue` | Analyst count |

**Why it matters:** Forward EBITDA estimates project covenant headroom 2–4 quarters out. If consensus EBITDA is declining while total debt is fixed, the agent can flag a future breach before it appears in any filing.

---

### F13. Price Target Consensus

**Endpoint:** `GET /stable/price-target-consensus?symbol={ticker}`  
**Primary Key:** `symbol`  
**Demo Component:** Portfolio Dashboard — upside/downside to target on holding cards

| Field | Description |
|---|---|
| `targetHigh` / `targetLow` | Bull/bear price targets |
| `targetConsensus` | Mean analyst target |
| `targetMedian` | Median target |

**Why it matters:** "Upside to target" appears in the holding card tooltip. When a position has fallen significantly below consensus target after a covenant event, the system flags it as a potential add opportunity for the re-allocation scenario Agent 3 drafts.

---

### F14. Analyst Ratings

**Endpoint:** `GET /stable/grades-summary?symbol={ticker}`  
**Primary Key:** `symbol`  
**Demo Component:** Portfolio Dashboard — rating badge; Agent 1 secondary signal

| Field | Description |
|---|---|
| `strongBuy` / `buy` / `hold` / `sell` / `strongSell` | Count of ratings |
| `consensus` | Overall label |

**Why it matters:** A concurrent analyst downgrade alongside a deteriorating covenant metric is a compounding signal. The agent surfaces both together in its alert, not just the covenant number.

---

### F15. Stock News

**Endpoint:** `GET /stable/stock-news?symbol={ticker}&limit=50`  
**Primary Key:** `symbol` + `publishedDate`  
**Demo Component:** Portfolio Dashboard — news feed; Agent 1 signal enrichment

| Field | Description |
|---|---|
| `publishedDate` | Timestamp |
| `title` | Headline |
| `text` | Article snippet |
| `site` | Source publication |
| `url` | Full article link |

**Why it matters:** News events complement filing-based signals and can precede an 8-K. Headlines are citation-linked in the advisor workspace alongside the document paragraphs. The agent includes relevant recent headlines in its client alert draft.

---

### F16. Index Quotes (DOW, NASDAQ, S&P 500)

**Endpoint:** `GET /api/v3/quote/^DJI,^GSPC,^IXIC ^VIX`  
**Primary Key:** `symbol` — `^DJI` (Dow Jones), `^GSPC` (S&P 500), `^IXIC` (Nasdaq Composite)  
**Demo Component:** Portfolio Intelligence Dashboard — market context header bar

| Field | Description |
|---|---|
| `symbol` | Index identifier (`^DJI`, `^GSPC`, `^IXIC`,`^VIX`) |
| `name` | Full index name |
| `price` | Current index level |
| `change` | Absolute point change |
| `changesPercentage` | Percentage change |
| `dayHigh` / `dayLow` | Intraday range |
| `52WeekHigh` / `52WeekLow` | 52-week range |
| `open` / `previousClose` | Session open and prior close |
| `volume` | Session volume |
| `timestamp` | Quote timestamp |

**Why it matters:** The dashboard header shows the three major indices as market context — the advisor sees at a glance whether today's portfolio movements are idiosyncratic or driven by broad market conditions. A covenant-breach alert landing on a down-market day reads very differently than one on a flat day.

---

### F17. Historical Index Prices (DOW, NASDAQ, S&P 500)

**Endpoint:** `GET /api/v3/historical-price-full/{symbol}?from={date}&to={date}`  
**Symbols:** `^GSPC`, `^DJI`, `^IXIC`,`^VIX`  
**Primary Key:** `symbol` + `date`  
**Demo Component:** Portfolio Intelligence Dashboard — YTD Alpha benchmark series; performance chart overlays

| Field | Description |
|---|---|
| `date` | Trading date |
| `open` / `high` / `low` / `close` | OHLC |
| `volume` | Session volume |
| `change` | Absolute point change |
| `changePercent` | Daily % change |
| `vwap` | Volume-weighted average price |

**Why it matters:** This is the authoritative benchmark series for YTD Alpha. Each holding's `changeOverTime` (from F2) is compared against `^GSPC` `changeOverTime` from the same start date to compute basis-point alpha. The S&P 500 series also overlays on the portfolio performance chart so the advisor can see exactly where they outran or lagged the market.

---

### F18. VIX (Cboe Volatility Index)

**Endpoint:** `GET /api/v3/quote/%5EVIX` (real-time) and `GET /api/v3/historical-price-full/%5EVIX` (historical)  
**Primary Key:** `symbol` (`^VIX`) + `date` for historical  
**Demo Component:** Portfolio Intelligence Dashboard — market stress indicator; risk context for agent alerts

| Field | Description |
|---|---|
| `symbol` | `^VIX` |
| `price` | Current VIX level |
| `change` / `changesPercentage` | Daily move |
| `dayHigh` / `dayLow` | Intraday range |
| `52WeekHigh` / `52WeekLow` | Annual range |
| `date` | For historical series |
| `close` | Daily VIX close (historical) |

**Why it matters:** VIX above 20 is a market stress regime — the same covenant headroom number carries different urgency at VIX 15 vs. VIX 30. The agent alert drafted for UHNW clients includes market context ("amid elevated volatility…") that is grounded in the live VIX level, making the communication feel timely and professionally calibrated rather than generic. Note: FMP does not have a dedicated VIX endpoint; `^VIX` is accessed via the standard index quote and historical price endpoints.

---

### F19. Index Constituents (S&P 500, Nasdaq 100, Dow Jones)

**Endpoints:**
- `GET /api/v3/sp500_constituent`
- `GET /api/v3/nasdaq_constituent`
- `GET /api/v3/dowjones_constituent`

**Primary Key:** `symbol`  
**Demo Component:** Portfolio Intelligence Dashboard — index membership badge on holding cards; concentration risk analysis

| Field | Description |
|---|---|
| `symbol` | Stock ticker |
| `companyName` | Full company name |
| `sector` | GICS sector |
| `subSector` | GICS sub-sector |
| `headQuarter` | Headquarters location |
| `dateAdded` | Date added to index |
| `cik` | SEC CIK number |

**Why it matters:** Knowing which holdings are S&P 500 constituents enables two things: (1) an index-membership badge on the holding card ("S&P 500 component") which is a quality signal for UHNW clients, and (2) look-through concentration analysis — if a client's ETF holdings plus direct equity holdings overweight a single S&P 500 sector, the system flags it against the IPS sector cap.

---

## Part 3 — Synthetic Structured Data (Client & Portfolio Intelligence)

Generated using Python Faker in Databricks notebooks. Registered in Unity Catalog under `uc.wealth.*`.

---

### S1. Client CRM (`uc.wealth.clients`)

**Primary Key:** `client_id`  
**Demo Component:** All components — every query and agent action is scoped to clients

| Field | Description |
|---|---|
| `client_id` | UUID |
| `client_name` | e.g. "Smith Family Office", "Chen Trust" |
| `tier` | UHNW / HNW |
| `total_aum` | Total AUM across all accounts |
| `advisor_id` | Assigned wealth advisor |
| `share_of_wallet_pct` | % of client's total wealth managed by GS |
| `contact_method_pref` | Email / Call / Secure Message |
| `tone_profile` | Formal / Conversational / Relationship-first |
| `relationship_start_date` | Tenure with advisor |

**Why it matters:** Every Genie query is prefixed with "my clients" or "my top 20 clients." Without this table, there is no personalization layer. `tone_profile` is what Agent 3 consults when drafting the reallocation proposal — the same event generates a different email for "Chen Trust" (formal, concise) vs. "Rodriguez Family Office" (conversational, relationship-first).

---

### S2. Client IPS Targets (`uc.wealth.client_ips_targets`)

**Primary Key:** `client_id` + `asset_class`  
**Demo Component:** Portfolio Dashboard — allocation drift; Genie query "overweight private credit relative to IPS"

| Field | Description |
|---|---|
| `client_id` | Client reference |
| `asset_class` | Equity / Fixed Income / Alternatives / Private Credit / ETF / Cash |
| `target_allocation_pct` | IPS target (e.g. 10.0%) |
| `min_allocation_pct` | Lower bound |
| `max_allocation_pct` | Upper bound |
| `rebalance_trigger_pct` | Drift threshold that triggers alert |

**Why it matters:** This is the table that makes the Genie demo query answerable. "Overweight private credit relative to IPS target" is a join between `daily_holdings.current_allocation_pct` and `client_ips_targets.max_allocation_pct`. Without this table the query has no answer.

---

### S3. Daily Holdings (`uc.wealth.daily_holdings`)

**Primary Key:** `client_id` + `asset_id` + `as_of_date`  
**Demo Component:** Portfolio Dashboard — per-client position view; Agent 2 cross-account exposure scan

| Field | Description |
|---|---|
| `client_id` | Client reference |
| `asset_id` | Matches ticker (e.g. `AINV`) or internal fund ID |
| `asset_class` | Equity / Fixed Income / ETF / Alternative / Private Credit |
| `holding_name` | Display name |
| `market_value` | Current dollar value |
| `current_allocation_pct` | Current weight in client portfolio (e.g. 14.2%) |
| `cost_basis` | Average cost |
| `quantity` | Units held |
| `as_of_date` | Position date |
| `tax_loss_harvesting_eligible` | **Boolean — asset currently held at a loss** |

**Why it matters:** This is the join table between clients and securities. Agent 2's entire function is: `SELECT client_id FROM daily_holdings WHERE asset_id = 'AINV'` — which clients have exposure to the name approaching covenant breach. The `tax_loss_harvesting_eligible` flag gives Agent 3 a specific action to include in the reallocation email ("this position also presents a TLH opportunity").

---

### S4. Asset Master & Performance (`uc.wealth.asset_master`)

**Primary Key:** `asset_id`  
**Demo Component:** Portfolio Dashboard — top-level KPIs (AUM, Net Flows, Revenue Yield, Alpha)

| Field | Description |
|---|---|
| `asset_id` | Ticker or internal ID |
| `asset_name` | Display name |
| `asset_class` | Equity / Fixed Income / ETF / Alternative / Private Credit |
| `strategy` | Buyout / Growth / Credit / Liquid Alternatives |
| `aum` | Total AUM for this position across all clients |
| `ytd_return_pct` | Year-to-date return |
| `ytd_alpha_bps` | Alpha vs. benchmark in basis points |
| `net_flows_ytd` | Net new assets YTD |
| `revenue_yield_bps` | Fee revenue in basis points |
| `benchmark_id` | Reference benchmark ticker (e.g. `SPY`, `AGG`) |
| `gp_name` | For alternatives: general partner name |
| `vintage_year` | For PE funds: vintage year |
| `dpi` | Distributions to paid-in (PE positions) |
| `net_irr` | Net IRR (PE positions) |
| `moic` | Multiple on invested capital (PE positions) |

**Why it matters:** This table powers the four top-line KPIs surfaced at the top of the dashboard: Total AUM, YTD Alpha, Net Flows (NNA), and Revenue Yield. It also provides the PE-specific metrics (DPI, Net IRR, MOIC) needed when the advisor drills into an alternatives position.

---

## Part 4 — Synthetic Contextual Data (Personalization Agent)

---

### C1. Advisor Communication History

**Format:** Small text dataset or system prompt (3–4 email examples per advisor)  
**Primary Key:** `advisor_id`  
**Demo Component:** Agent 3 — tone mimicry for client communication drafts

| Field | Description |
|---|---|
| `advisor_id` | Advisor reference |
| `example_email_text` | Verbatim past email (sanitized) |
| `tone_label` | Formal / Conversational / Relationship-first |
| `communication_context` | What situation prompted the email |

**Why it matters:** Agent 3 does not generate a generic "dear client" email. It reads 3–4 of the advisor's real past emails, extracts their sentence structure, vocabulary, and sign-off style, and produces a draft that sounds like the advisor wrote it. This is the personalization demo moment — 14 different reallocation proposals, each matching a different advisor's voice.

---

### C2. Tax-Loss Harvesting Status

**Location:** Column within `uc.wealth.daily_holdings` — `tax_loss_harvesting_eligible BOOLEAN`  
**Demo Component:** Agent 3 — action rationale in reallocation email

**Why it matters:** When a position is both (a) exposed to a covenant-breach name and (b) held at a loss, the reallocation email has a concrete, client-specific reason to act: "Trimming this position also realizes a tax loss before year-end." This makes the draft actionable, not generic.

---

## Source-to-Component Mapping

| Demo Component | Real (FMP / SEC) | Synthetic (UC Tables) |
|---|---|---|
| **Portfolio Intelligence Dashboard** | F1 Profile, F2 Prices, F6 Key Metrics, F9 ETF Info, F11 ETF Sectors, F16 Index Quotes, F17 Index History, F18 VIX, F19 Constituents | S3 Daily Holdings, S2 IPS Targets, S4 Asset Master |
| **Document Intelligence Layer** | D1 10-Q/8-K filings, D2 Credit Agreements, D3 Transcripts, F3 Income Stmt, F4 Balance Sheet, F5 Cash Flow, F8 SEC Filing links | S1 Client CRM (for scoping) |
| **Agent 1 — Covenant Detection** | F6 Key Metrics (`netDebtToEBITDA`), F8 SEC 8-K filings, D1 BDC 10-Qs, F18 VIX (risk context) | S4 Asset Master (covenant thresholds) |
| **Agent 2 — Cross-Account Exposure** | F1 Profile (name resolution) | S3 Daily Holdings (`asset_id` cross-reference) |
| **Agent 3 — Client Communication** | F15 News, F14 Analyst Ratings, F18 VIX (market context in email) | S1 CRM (`tone_profile`), C1 Advisor History, C2 TLH Status |
| **Genie Chat Interface** | All FMP sources | S2 IPS Targets, S3 Holdings, S4 Asset Master |

---

## Key Metrics Glossary

| Term | Definition | Source |
|---|---|---|
| **Total AUM** | Assets under management across all clients | `uc.wealth.asset_master.aum` |
| **YTD Alpha (bps)** | Return generated above benchmark, in basis points | F2 holding `changeOverTime` minus F17 `^GSPC` `changeOverTime` from same start date — stored in `asset_master.ytd_alpha_bps` |
| **Net Flows / NNA** | Net New Assets: inflows minus outflows YTD | `uc.wealth.asset_master.net_flows_ytd` |
| **Revenue Yield (bps)** | Fee revenue as basis points of AUM | `uc.wealth.asset_master.revenue_yield_bps` |
| **IPS Target** | Legally binding target allocation per client per asset class | `uc.wealth.client_ips_targets.target_allocation_pct` |
| **Allocation Drift** | Current allocation % minus IPS target % | `daily_holdings.current_allocation_pct` − `client_ips_targets.target_allocation_pct` |
| **UHNW / Family Office** | Ultra-High-Net-Worth — target client segment | `uc.wealth.clients.tier = 'UHNW'` |
| **Share of Wallet** | % of client's total wealth managed by GS | `uc.wealth.clients.share_of_wallet_pct` |
| **TLH / Tax-Loss Harvesting** | Selling a position held at a loss to realize a tax deduction | `uc.wealth.daily_holdings.tax_loss_harvesting_eligible` |
| **Covenant Headroom** | Cushion before breaching debt terms, e.g. 0.3x | F6 `netDebtToEBITDA` vs. covenant threshold in `asset_master` |
| **Net Debt / EBITDA** | Primary leverage covenant metric | F4 `netDebt` ÷ F3 `ebitda` |
| **DSCR** | Debt Service Coverage Ratio — cash available / debt service | F5 `operatingCashFlow` ÷ (interest + principal) |
| **DPI** | Distributions to Paid-In — capital returned vs. invested | `uc.wealth.asset_master.dpi` (PE positions only) |
| **Net IRR** | Internal rate of return net of fees (PE funds) | `uc.wealth.asset_master.net_irr` |
| **MOIC** | Multiple on Invested Capital (PE funds) | `uc.wealth.asset_master.moic` |

---

## Coverage Gaps

| Asset Class | FMP Coverage | Gap / Approach |
|---|---|---|
| Public Equities | Full — 70,000+ securities | None |
| Major Indices (DOW, NASDAQ, S&P 500) | Full — real-time quotes and full daily history via `^DJI`, `^GSPC`, `^IXIC` | None |
| VIX | Accessible via standard index endpoints using `^VIX` symbol — no dedicated endpoint | Use `/api/v3/quote/%5EVIX` and `/api/v3/historical-price-full/%5EVIX` |
| ETFs | Full — holdings, sectors, performance | None |
| BDC Securities (AINV, OCSL) | Full — treated as public equities | 10-Q loan schedules downloaded separately from EDGAR |
| Corporate Bonds (public) | Limited — no bond price feed | Represent as positions in `daily_holdings`; use issuer equity data for intelligence |
| Private Credit (bilateral loans) | None | Fully synthetic via `asset_master` + covenant fields |
| Private Equity / Alternatives | None | Fully synthetic — DPI, IRR, MOIC in `asset_master` |
| Client / IPS Data | None | Fully synthetic — `clients`, `client_ips_targets`, `daily_holdings` |
| Advisor Tone / Communication History | None | Manually authored — 3–4 sample emails per advisor |
