# GS AWM Genie Space — Setup & Sample Query Guide

**Catalog:** `ahtsa` · **Schema:** `awm` · **Warehouse:** Shared Unity Catalog Serverless

This guide documents every table to register in the Genie space, the rationale for including it, and a battery of tested sample queries organized by the dashboard page they mirror. All queries were verified against live data on 2026-05-03.

---

## 1. Tables to Tag in the Genie Space

### Gold Layer — Primary Query Targets

These are the tables Genie should answer questions from directly. They are pre-joined and analyst-ready.

| Table | Grain | Purpose |
|---|---|---|
| `gold_ips_drift` | (account × asset class) | IPS allocation drift, breach status, rebalance amounts |
| `gold_financial_fundamentals` | (symbol × date × period) | All financials: leverage, margins, growth, valuation, estimates |
| `gold_financials_vs_estimates` | (symbol × period) | EPS and revenue beat/miss vs analyst consensus |
| `gold_unified_signals` | one row per signal event | AI-generated signals from news, SEC filings, transcripts, BDC XBRL |
| `gold_portfolio_sector_exposure` | (account × ticker × sector) | Sector look-through including ETF holdings |
| `gold_account_ips_drift` | (account × asset class) | Account-grain IPS drift (alternate view) |
| `gold_client_ips_drift` | (client × asset class) | Client-grain IPS drift rollup |
| `gold_bdc_early_warnings` | per BDC signal | Structured BDC covenant warning output |

### Synthetic Portfolio Tables — Context for "Who Holds What"

| Table | Grain | Purpose |
|---|---|---|
| `holdings` | (account × ticker) | Current portfolio positions, market value, unrealized G/L |
| `clients` | per client | Client metadata: name, tier, risk profile, advisor, tone |
| `accounts` | per account | Account name, type, AUM |
| `transactions` | per trade | Full ledger: BUY, DRIP, DIVIDEND, FEE |
| `ips_targets` | (risk profile × asset class) | IPS target, min, max allocation bands |

### BDC / Private Credit Tables — Early Warning Layer

| Table | Grain | Purpose |
|---|---|---|
| `bdc_fy_snapshot` | per BDC ticker (latest FY) | PIK, NII, NAV/share, depreciation — snapshot for covenant scanning |
| `bdc_time_series` | (ticker × metric × period) | Quarterly XBRL time series for trend analysis |

### Bronze Reference Tables — Enrichment

| Table | Grain | Purpose |
|---|---|---|
| `bronze_company_profiles` | per symbol | Sector, industry, company name, CIK, ETF flag |
| `bronze_historical_prices` | (symbol × date) | Daily adjusted close prices for all holdings |
| `bronze_indexes_and_vix` | (symbol × date) | S&P 500, DJIA, NASDAQ, VIX daily closes |

---

## 2. Dashboard Page Map → Genie Query Coverage

The dashboard has five pages. The sample queries below mirror each page's functionality so Genie can answer the same questions in natural language.

---

## Page 1: Advisor 360 (Portfolio Performance)

**What it shows:** P&L, % return vs benchmark, fee drag, alpha timeseries, asset class and ticker return bars.  
**Genie data sources:** `holdings`, `transactions`, `clients`, `accounts`, `bronze_historical_prices`, `bronze_indexes_and_vix`, `bronze_company_profiles`

---

### [GENIE] "What is the total unrealized gain/loss across all portfolios?"

```sql
SELECT
  c.advisor_id,
  c.tier,
  SUM(h.market_value)    AS total_market_value,
  SUM(h.total_cost_basis) AS total_cost_basis,
  SUM(h.unrealized_gl)    AS total_unrealized_gl,
  ROUND(SUM(h.unrealized_gl) / NULLIF(SUM(h.total_cost_basis), 0) * 100, 2)
                          AS unrealized_gl_pct
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
JOIN ahtsa.awm.clients  c ON a.client_id  = c.client_id
WHERE h.ticker != 'CASH'
GROUP BY c.advisor_id, c.tier
ORDER BY total_market_value DESC
```

---

### [GENIE] "Which accounts hold Private Credit (BDC) positions?"

```sql
SELECT
  c.advisor_id,
  c.client_name,
  c.tier,
  a.account_id,
  a.account_name,
  a.account_type,
  h.ticker,
  cp.companyName,
  ROUND(h.market_value, 2)       AS market_value,
  ROUND(h.unrealized_gl, 2)      AS unrealized_gl,
  ROUND(h.market_value / NULLIF(SUM(h.market_value) OVER (PARTITION BY h.account_id), 0) * 100, 2)
                                 AS pct_of_account
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts          a  ON h.account_id = a.account_id
JOIN ahtsa.awm.clients           c  ON a.client_id  = c.client_id
LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON h.ticker = cp.symbol
WHERE h.asset_class = 'Private Credit'
ORDER BY market_value DESC
```

---

### [GENIE] "Show me unrealized losses — candidates for tax-loss harvesting"

```sql
SELECT
  c.advisor_id,
  c.client_name,
  a.account_id,
  a.account_name,
  h.ticker,
  h.asset_class,
  ROUND(h.market_value, 2)          AS market_value,
  ROUND(h.unrealized_gl, 2)         AS unrealized_gl,
  ROUND(h.unrealized_gl_pct, 2)     AS unrealized_gl_pct,
  ROUND(h.cost_basis_per_share, 4)  AS cost_basis,
  ROUND(h.price, 4)                 AS current_price
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
JOIN ahtsa.awm.clients  c ON a.client_id  = c.client_id
WHERE h.unrealized_gl < 0
  AND h.ticker != 'CASH'
ORDER BY h.unrealized_gl ASC
LIMIT 50
```

---

### [GENIE] "What is total AUM by advisor?"

```sql
SELECT
  c.advisor_id,
  COUNT(DISTINCT c.client_id)    AS client_count,
  COUNT(DISTINCT a.account_id)   AS account_count,
  ROUND(SUM(h.market_value) / 1e9, 3) AS aum_b
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts a ON h.account_id = a.account_id
JOIN ahtsa.awm.clients  c ON a.client_id  = c.client_id
GROUP BY c.advisor_id
ORDER BY aum_b DESC
```

---

## Page 2: Exposure + Drift (IPS Compliance)

**What it shows:** Asset class allocation vs IPS bands, rebalance amounts by account, scatter of drift severity vs AUM, Private Credit spotlight.  
**Genie data sources:** `gold_ips_drift`, `gold_portfolio_sector_exposure`, `holdings`

---

### [GENIE] "Which clients have the worst IPS drift?"

```sql
SELECT
  client_id,
  client_name,
  advisor_id,
  tier,
  risk_profile,
  ROUND(SUM(actual_market_value) / 1e6, 3)               AS client_aum_m,
  ROUND(AVG(ABS(drift_from_target_pct)), 4)               AS drift_score,
  SUM(CASE WHEN drift_status != 'Within Band' THEN 1 ELSE 0 END) AS breach_count,
  ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 3)             AS total_rebalance_abs_m
FROM ahtsa.awm.gold_ips_drift
GROUP BY client_id, client_name, advisor_id, tier, risk_profile
ORDER BY drift_score DESC
LIMIT 30
```

> **Tested result:** Returns 30 clients ranked by drift score. Top client had drift_score = 8.18 and breach_count = 8, with $289M of rebalancing needed.

---

### [GENIE] "Which accounts are overweight in Private Credit?"

```sql
SELECT
  advisor_id,
  client_id,
  client_name,
  account_id,
  account_name,
  account_type,
  tier,
  risk_profile,
  ROUND(actual_allocation_pct, 2)     AS actual_pct,
  target_allocation_pct               AS target_pct,
  max_allocation_pct                  AS max_pct,
  ROUND(out_of_bounds_pct, 4)         AS over_band_by_pct,
  ROUND(actual_market_value / 1e6, 3) AS actual_mv_m,
  ROUND(total_account_value / 1e6, 3) AS account_aum_m,
  ROUND(rebalance_to_band   / 1e6, 3) AS rebalance_to_band_m
FROM ahtsa.awm.gold_ips_drift
WHERE asset_class = 'Private Credit'
  AND drift_status = 'Over Band'
ORDER BY over_band_by_pct DESC
```

---

### [GENIE] "Show me all accounts outside their IPS bounds"

```sql
SELECT
  client_name,
  advisor_id,
  account_id,
  account_name,
  account_type,
  asset_class,
  drift_status,
  drift_severity,
  ROUND(actual_allocation_pct, 2)     AS actual_pct,
  target_allocation_pct               AS target_pct,
  min_allocation_pct                  AS min_pct,
  max_allocation_pct                  AS max_pct,
  out_of_bounds_pct,
  ROUND(actual_market_value / 1e6, 3) AS actual_mv_m,
  ROUND(total_account_value / 1e6, 3) AS account_aum_m,
  ROUND(rebalance_to_band   / 1e6, 3) AS rebalance_to_band_m
FROM ahtsa.awm.gold_ips_drift
WHERE drift_status != 'Within Band'
ORDER BY out_of_bounds_pct DESC
```

---

### [GENIE] "What is the total rebalance amount needed by asset class?"

```sql
SELECT
  asset_class,
  drift_status,
  COUNT(DISTINCT account_id)              AS accounts_impacted,
  COUNT(DISTINCT client_id)               AS clients_impacted,
  ROUND(SUM(actual_market_value) / 1e9, 3)  AS total_actual_b,
  ROUND(SUM(target_market_value) / 1e9, 3)  AS total_target_b,
  ROUND(SUM(rebalance_to_band)   / 1e6, 2)  AS rebalance_to_band_m,
  ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 2) AS rebalance_abs_m
FROM ahtsa.awm.gold_ips_drift
WHERE drift_status != 'Within Band'
GROUP BY asset_class, drift_status
ORDER BY rebalance_abs_m DESC
```

> **Tested result:** Private Credit Under Band accounts dominate (329 accounts, $4.4B rebalance), followed by Equity Over Band (325 accounts, $2.2B). This is the systemic tilt that drives the demo story.

---

### [GENIE] "Which advisors have the most clients with IPS drift?"

```sql
SELECT
  advisor_id,
  COUNT(DISTINCT client_id)                                         AS total_clients,
  COUNT(DISTINCT account_id)                                        AS total_accounts,
  ROUND(SUM(total_account_value) / COUNT(DISTINCT asset_class) / 1e9, 3) AS book_aum_b,
  COUNT(DISTINCT CASE WHEN drift_status != 'Within Band' THEN client_id END) AS clients_with_drift,
  ROUND(
    COUNT(DISTINCT CASE WHEN drift_status != 'Within Band' THEN client_id END)
    / NULLIF(COUNT(DISTINCT client_id), 0) * 100, 1)                AS pct_clients_drifted,
  ROUND(SUM(ABS(rebalance_to_band)) / 1e6, 2)                       AS total_rebalance_abs_m
FROM ahtsa.awm.gold_ips_drift
GROUP BY advisor_id
ORDER BY clients_with_drift DESC
```

> **Tested result:** ADV002 leads with 27 drifted clients (77.1%), $983M rebalance needed.

---

### [GENIE] "What is the average allocation vs target by asset class?"

```sql
SELECT
  asset_class,
  ROUND(AVG(actual_allocation_pct), 4)  AS avg_actual_pct,
  ROUND(AVG(target_allocation_pct), 4)  AS avg_target_pct,
  ROUND(AVG(drift_from_target_pct), 4)  AS avg_drift_pct,
  COUNT(DISTINCT account_id)             AS total_accounts,
  SUM(CASE WHEN drift_status = 'Over Band'  THEN 1 ELSE 0 END) AS over_band_count,
  SUM(CASE WHEN drift_status = 'Under Band' THEN 1 ELSE 0 END) AS under_band_count,
  ROUND(SUM(actual_market_value) / 1e9, 3) AS total_actual_b,
  ROUND(SUM(target_market_value) / 1e9, 3) AS total_target_b
FROM ahtsa.awm.gold_ips_drift
GROUP BY asset_class
ORDER BY ABS(avg_drift_pct) DESC
```

> **Tested result:** Equity is systematically overweight (+10.3 pp avg). Private Credit is systematically underweight (−7.5 pp avg). This sets up the demo narrative.

---

### [GENIE] "What is the portfolio's largest sector exposure?"

```sql
SELECT
  sector,
  ROUND(SUM(exposure_market_value) / 1e9, 4)         AS total_exposure_b,
  ROUND(SUM(exposure_market_value) / NULLIF(SUM(SUM(exposure_market_value)) OVER (), 0) * 100, 2)
                                                      AS pct_of_total,
  ROUND(SUM(CASE WHEN source_type = 'Direct' THEN exposure_market_value ELSE 0 END) / 1e9, 4)
                                                      AS direct_b,
  ROUND(SUM(CASE WHEN source_type = 'ETF'    THEN exposure_market_value ELSE 0 END) / 1e9, 4)
                                                      AS via_etf_b,
  COUNT(DISTINCT client_id)                           AS clients_exposed
FROM ahtsa.awm.gold_portfolio_sector_exposure
WHERE sector != 'Cash'
GROUP BY sector
ORDER BY total_exposure_b DESC
```

---

### [GENIE] "How much of the Financials exposure is direct vs through ETFs?"

```sql
SELECT
  source_type,
  source_ticker,
  source_name,
  ROUND(SUM(exposure_market_value) / 1e6, 3)    AS exposure_m,
  ROUND(SUM(exposure_market_value) / NULLIF(SUM(SUM(exposure_market_value)) OVER (), 0) * 100, 2)
                                                 AS pct_of_financials,
  COUNT(DISTINCT account_id)                     AS accounts
FROM ahtsa.awm.gold_portfolio_sector_exposure
WHERE sector IN ('Financials', 'Financial Services')
GROUP BY source_type, source_ticker, source_name
ORDER BY exposure_m DESC
```

---

## Page 3: Signals Hub (Intelligence Feed)

**What it shows:** Index return counters, signal volume over time, signal feed table, sector exposure for action-needed signals, detail panel.  
**Genie data sources:** `gold_unified_signals`, `holdings`, `clients`, `accounts`, `bronze_company_profiles`, `bronze_indexes_and_vix`

---

### [GENIE] "Which tickers have the most action-needed signals?"

```sql
SELECT
  g.symbol,
  cp.companyName,
  cp.sector,
  COUNT(*)                                                              AS signal_count,
  ROUND(
    (SUM(CASE WHEN sentiment = 'Positive'  THEN  1.0 ELSE 0 END)
   + SUM(CASE WHEN sentiment = 'Mixed'     THEN -0.5 ELSE 0 END)
   + SUM(CASE WHEN sentiment = 'Negative'  THEN -1.0 ELSE 0 END))
    / NULLIF(COUNT(*), 0), 3)                                          AS net_sentiment_score,
  SUM(CASE WHEN advisor_action_needed THEN 1 ELSE 0 END)               AS action_needed_count,
  MAX(signal_date)                                                      AS latest_signal_date
FROM ahtsa.awm.gold_unified_signals g
LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON g.symbol = cp.symbol
GROUP BY g.symbol, cp.companyName, cp.sector
ORDER BY action_needed_count DESC, net_sentiment_score ASC
LIMIT 20
```

> **Tested result:** AAPL leads with 61 action-needed signals (148 total). BDC names like TCPC, OCSL, NMFC dominate negative sentiment (−0.4 to −0.6 scores).

---

### [GENIE] "Show me all negative sentiment signals from the last 30 days"

```sql
SELECT
  signal_date,
  symbol,
  source_type,
  signal_type,
  sentiment,
  severity_score,
  signal_value,
  advisor_action_needed,
  signal,
  rationale
FROM ahtsa.awm.gold_unified_signals
WHERE sentiment = 'Negative'
  AND signal_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY signal_date DESC, severity_score DESC
```

---

### [GENIE] "Which client accounts are exposed to tickers with action-needed signals?"

```sql
SELECT
  c.advisor_id,
  c.client_name,
  c.tier,
  a.account_id,
  a.account_name,
  h.ticker,
  cp.companyName,
  cp.sector,
  s.signal_count,
  s.net_sentiment_score,
  s.high_severity_count,
  s.advisor_action_count,
  s.latest_signal_date,
  ROUND(h.market_value, 2) AS exposure
FROM ahtsa.awm.holdings h
JOIN ahtsa.awm.accounts  a  ON h.account_id = a.account_id
JOIN ahtsa.awm.clients   c  ON a.client_id  = c.client_id
LEFT JOIN ahtsa.awm.bronze_company_profiles cp ON h.ticker = cp.symbol
JOIN (
  SELECT
    symbol,
    COUNT(*)                                                            AS signal_count,
    ROUND(
      (SUM(CASE WHEN sentiment = 'Positive'  THEN  1.0 ELSE 0 END)
     + SUM(CASE WHEN sentiment = 'Mixed'     THEN -0.5 ELSE 0 END)
     + SUM(CASE WHEN sentiment = 'Negative'  THEN -1.0 ELSE 0 END))
      / NULLIF(COUNT(*), 0), 3)                                        AS net_sentiment_score,
    SUM(CASE WHEN signal_value = 'High' THEN 1 ELSE 0 END)            AS high_severity_count,
    SUM(CASE WHEN advisor_action_needed THEN 1 ELSE 0 END)             AS advisor_action_count,
    MAX(signal_date)                                                    AS latest_signal_date
  FROM ahtsa.awm.gold_unified_signals
  WHERE advisor_action_needed = true
  GROUP BY symbol
) s ON h.ticker = s.symbol
ORDER BY s.advisor_action_count DESC, s.net_sentiment_score ASC
```

---

### [GENIE] "Show me the signal breakdown by source type"

```sql
SELECT
  CASE
    WHEN source_type LIKE 'sec_filing_%' THEN CONCAT('SEC Filing (', SUBSTRING(source_type, 12), ')')
    WHEN source_type = 'news'                THEN 'News'
    WHEN source_type = 'bdc_early_warning'   THEN 'BDC Early Warning'
    WHEN source_type = 'earnings_transcript' THEN 'Earnings Transcript'
    ELSE INITCAP(REPLACE(source_type, '_', ' '))
  END                                                   AS source_type_display,
  COUNT(*)                                              AS signal_count,
  SUM(CASE WHEN advisor_action_needed THEN 1 ELSE 0 END) AS action_needed_count,
  SUM(CASE WHEN sentiment = 'Positive'  THEN 1 ELSE 0 END) AS positive,
  SUM(CASE WHEN sentiment = 'Negative'  THEN 1 ELSE 0 END) AS negative,
  ROUND(
    (SUM(CASE WHEN sentiment = 'Positive'  THEN  1.0 ELSE 0 END)
   + SUM(CASE WHEN sentiment = 'Mixed'     THEN -0.5 ELSE 0 END)
   + SUM(CASE WHEN sentiment = 'Negative'  THEN -1.0 ELSE 0 END))
    / NULLIF(COUNT(*), 0), 3)                           AS net_sentiment_score
FROM ahtsa.awm.gold_unified_signals
GROUP BY source_type
ORDER BY signal_count DESC
```

---

## Page 4: Financial Fundamentals (Covenant / Credit Intelligence)

**What it shows (Genie equivalent of dashboard page functionality):** Leverage trends, margins, growth rates, BDC early warning metrics.  
**Genie data sources:** `gold_financial_fundamentals`, `gold_financials_vs_estimates`, `bdc_fy_snapshot`, `bdc_time_series`

---

### [GENIE] "Show me the leverage trend for ARCC over the last 8 quarters"

```sql
SELECT
  symbol,
  date,
  fiscal_year,
  period,
  ROUND(net_debt  / 1e6, 2)  AS net_debt_m,
  ROUND(ebitda    / 1e6, 2)  AS ebitda_m,
  net_debt_to_ebitda,
  interest_coverage,
  dscr,
  leverage_flag,
  ROUND(forward_nd_ebitda, 2) AS forward_nd_ebitda
FROM ahtsa.awm.gold_financial_fundamentals
WHERE symbol = 'ARCC'
ORDER BY date DESC
LIMIT 8
```

> **Tested result:** Returns 8 quarters. net_debt_to_ebitda is consistently "High" (>5x for a BDC, which is expected). forward_nd_ebitda trends between 23x–29x.

---

### [GENIE] "Which tickers have the highest leverage right now?"

```sql
SELECT
  symbol,
  company_name,
  sector,
  date               AS latest_period,
  period,
  net_debt_to_ebitda,
  interest_coverage,
  leverage_flag,
  ROUND(net_debt  / 1e6, 2) AS net_debt_m,
  ROUND(ebitda    / 1e6, 2) AS ebitda_m,
  analyst_consensus
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
  FROM ahtsa.awm.gold_financial_fundamentals
  WHERE is_etf = false OR is_etf IS NULL
)
WHERE _rn = 1
  AND net_debt_to_ebitda IS NOT NULL
ORDER BY net_debt_to_ebitda DESC
LIMIT 25
```

> **Tested result:** C (Citigroup) leads at 96x (as expected for a bank), followed by PFE, FSK, PSEC, GS — all in Financial Services/BDC. Confirms the data is finance-sector heavy and that BDC leverage math reflects loan portfolio structure.

---

### [GENIE] "Show me revenue and EPS growth for AAPL over the last 2 years"

```sql
SELECT
  symbol,
  date,
  fiscal_year,
  period,
  ROUND(revenue    / 1e6, 2) AS revenue_m,
  ROUND(net_income / 1e6, 2) AS net_income_m,
  eps_diluted,
  revenue_growth_yoy,
  net_income_growth_yoy,
  eps_diluted_growth_yoy,
  ebitda_growth_yoy
FROM ahtsa.awm.gold_financial_fundamentals
WHERE symbol = 'AAPL'
ORDER BY date DESC
LIMIT 8
```

---

### [GENIE] "Compare margins across Financial Services companies"

```sql
SELECT
  symbol,
  company_name,
  date             AS latest_period,
  gross_profit_margin,
  ebitda_margin,
  net_profit_margin,
  operating_margin,
  roe,
  roic,
  ROUND(market_cap / 1e9, 2) AS market_cap_b
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
  FROM ahtsa.awm.gold_financial_fundamentals
  WHERE sector = 'Financial Services'
    AND (is_etf = false OR is_etf IS NULL)
)
WHERE _rn = 1
ORDER BY market_cap_b DESC NULLS LAST
LIMIT 30
```

---

### [GENIE] "Which tickers have had 3+ consecutive quarters of sequential revenue growth?"

```sql
WITH quarterly AS (
  SELECT
    symbol, company_name, sector, date, period, revenue,
    LAG(revenue, 1) OVER (PARTITION BY symbol ORDER BY date) AS prev_q,
    LAG(revenue, 2) OVER (PARTITION BY symbol ORDER BY date) AS prev_2q,
    LAG(revenue, 3) OVER (PARTITION BY symbol ORDER BY date) AS prev_3q
  FROM ahtsa.awm.gold_financial_fundamentals
  WHERE is_etf = false OR is_etf IS NULL
),
latest AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
  FROM quarterly
)
SELECT
  symbol, company_name, sector,
  date                                           AS latest_period,
  ROUND(revenue / 1e6, 2)                        AS revenue_m,
  ROUND(prev_q  / 1e6, 2)                        AS prev_q_m,
  ROUND((revenue - prev_3q) / NULLIF(prev_3q, 0) * 100, 1) AS growth_over_3q_pct
FROM latest
WHERE _rn = 1
  AND revenue > prev_q
  AND prev_q  > prev_2q
  AND prev_2q > prev_3q
ORDER BY growth_over_3q_pct DESC NULLS LAST
```

---

### [GENIE] "Show credit quality metrics for BDC and Financial Services holdings"

```sql
SELECT
  f.symbol,
  f.company_name,
  f.date               AS latest_period,
  f.period,
  f.net_debt_to_ebitda,
  f.interest_coverage,
  f.dscr,
  f.fcf_yield,
  ROUND(f.free_cash_flow / 1e6, 2)  AS fcf_m,
  ROUND(f.net_debt       / 1e6, 2)  AS net_debt_m,
  f.dividend_yield,
  f.book_value_per_share,
  f.leverage_flag,
  f.analyst_consensus
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS _rn
  FROM ahtsa.awm.gold_financial_fundamentals
  WHERE sector = 'Financial Services'
    AND (is_etf = false OR is_etf IS NULL)
)  f
WHERE _rn = 1
ORDER BY f.net_debt_to_ebitda DESC NULLS LAST
```

---

### [GENIE] "Which companies have both declining margins AND rising leverage vs a year ago?"

```sql
WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
  FROM ahtsa.awm.gold_financial_fundamentals
  WHERE is_etf = false OR is_etf IS NULL
),
latest   AS (SELECT * FROM ranked WHERE rn = 1),
prior_yr AS (SELECT * FROM ranked WHERE rn = 5)
SELECT
  l.symbol,
  l.company_name,
  l.sector,
  l.date                                                AS latest_period,
  ROUND(l.ebitda_margin    * 100, 2)                    AS ebitda_margin_pct_now,
  ROUND(p.ebitda_margin    * 100, 2)                    AS ebitda_margin_pct_1yr_ago,
  ROUND((l.ebitda_margin - p.ebitda_margin) * 100, 2)  AS margin_change_pp,
  l.net_debt_to_ebitda                                  AS nd_ebitda_now,
  p.net_debt_to_ebitda                                  AS nd_ebitda_1yr_ago,
  ROUND(l.net_debt_to_ebitda - p.net_debt_to_ebitda, 2) AS leverage_change,
  l.leverage_flag,
  l.analyst_consensus
FROM latest l
JOIN prior_yr p ON l.symbol = p.symbol
WHERE l.ebitda_margin < p.ebitda_margin
  AND l.net_debt_to_ebitda > p.net_debt_to_ebitda
  AND l.net_debt_to_ebitda IS NOT NULL
  AND p.net_debt_to_ebitda IS NOT NULL
ORDER BY leverage_change DESC
```

---

## Page 5: Beat/Miss (Earnings vs Estimates)

**What it shows:** Which companies beat or missed EPS/revenue consensus.  
**Genie data sources:** `gold_financials_vs_estimates`

---

### [GENIE] "Which companies beat EPS estimates by the most last quarter?"

```sql
SELECT
  symbol,
  company_name,
  sector,
  period_end,
  period,
  actual_eps,
  est_eps,
  eps_surprise_pct,
  eps_beat_miss,
  revenue_beat_miss,
  combined_beat_miss
FROM ahtsa.awm.gold_financials_vs_estimates
WHERE eps_beat_miss IS NOT NULL
ORDER BY period_end DESC, ABS(eps_surprise_pct) DESC
LIMIT 25
```

> **Tested result:** KO (Coca-Cola) leads with a double beat Q1 2026. ARCC shows a miss. Results include 12 recent quarter records with Beat/Miss/Double Beat classification.

---

### [GENIE] "Show the beat/miss history for ARCC over the last 8 quarters"

```sql
SELECT
  symbol,
  company_name,
  period_end,
  fiscal_year,
  period,
  actual_eps,
  est_eps,
  eps_surprise_pct,
  eps_beat_miss,
  actual_revenue_m,
  est_revenue_m,
  revenue_surprise_pct,
  revenue_beat_miss,
  combined_beat_miss
FROM ahtsa.awm.gold_financials_vs_estimates
WHERE symbol = 'ARCC'
ORDER BY period_end DESC
LIMIT 8
```

---

### [GENIE] "Which Financial Services companies consistently beat EPS estimates?"

```sql
SELECT
  symbol,
  company_name,
  COUNT(*)                                                             AS periods_reported,
  SUM(CASE WHEN eps_beat_miss = 'Beat'     THEN 1 ELSE 0 END)         AS beats,
  SUM(CASE WHEN eps_beat_miss = 'Miss'     THEN 1 ELSE 0 END)         AS misses,
  SUM(CASE WHEN eps_beat_miss = 'In-Line'  THEN 1 ELSE 0 END)         AS in_line,
  ROUND(SUM(CASE WHEN eps_beat_miss = 'Beat' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0) * 100, 1)                                AS beat_rate_pct,
  ROUND(AVG(eps_surprise_pct), 2)                                      AS avg_eps_surprise_pct
FROM ahtsa.awm.gold_financials_vs_estimates
WHERE sector = 'Financial Services'
  AND eps_beat_miss IS NOT NULL
  AND (is_etf = false OR is_etf IS NULL)
GROUP BY symbol, company_name
HAVING COUNT(*) >= 4
ORDER BY beat_rate_pct DESC
```

---

### [GENIE] "Which companies had a double beat last quarter?"

```sql
SELECT
  symbol,
  company_name,
  sector,
  period_end,
  period,
  actual_eps,
  est_eps,
  eps_surprise_pct,
  actual_revenue_m,
  est_revenue_m,
  revenue_surprise_pct,
  combined_beat_miss
FROM ahtsa.awm.gold_financials_vs_estimates
WHERE combined_beat_miss = 'Double Beat'
  AND period_end = (SELECT MAX(period_end) FROM ahtsa.awm.gold_financials_vs_estimates)
ORDER BY eps_surprise_pct DESC
```

---

## BDC Early Warning — Private Credit Specific

**What it shows:** Tier 1 (PIK, dividend coverage), Tier 2 (NAV declines, depreciation), Tier 3 (realized losses) signals for 16 BDCs.  
**Genie data sources:** `bdc_fy_snapshot`, `bdc_time_series`

---

### [GENIE] "Show the PIK and NII metrics for all BDCs"

```sql
SELECT
  ticker,
  cik,
  ROUND(pik, 2)         AS pik,
  ROUND(nii, 2)         AS nii,
  ROUND(nii_ps, 4)      AS nii_per_share,
  ROUND(div_ps, 4)      AS div_per_share,
  ROUND(nav_ps, 4)      AS nav_per_share,
  ROUND(deprec, 2)      AS unrealized_depreciation,
  ROUND(COALESCE(pik, 0) / NULLIF(nii, 0) * 100, 1) AS pik_nii_pct,
  ROUND(nii_ps / NULLIF(div_ps, 0) * 100, 1)         AS div_coverage_pct
FROM ahtsa.awm.bdc_fy_snapshot
ORDER BY pik_nii_pct DESC NULLS LAST
```

> **Tested result:** Returns 16 BDC rows. ARCC, FSK, GSBD show PIK entries. Dividend coverage and NAV/share columns are populated for most tickers.

---

### [GENIE] "Which BDCs have the worst dividend coverage ratio?"

```sql
SELECT
  ticker,
  ROUND(nii_ps, 4)      AS nii_per_share,
  ROUND(div_ps, 4)      AS div_per_share,
  ROUND(nii_ps / NULLIF(div_ps, 0) * 100, 1) AS div_coverage_pct,
  CASE
    WHEN nii_ps / NULLIF(div_ps, 0) <= 1.00 THEN 'Critical — dividend exceeds NII'
    WHEN nii_ps / NULLIF(div_ps, 0) <= 1.05 THEN 'Warning — thin coverage'
    ELSE 'OK'
  END                   AS coverage_flag,
  ROUND(nav_ps, 4)      AS nav_per_share
FROM ahtsa.awm.bdc_fy_snapshot
WHERE div_ps IS NOT NULL AND div_ps > 0
ORDER BY div_coverage_pct ASC
```

---

### [GENIE] "Show NAV per share trend for ARCC over the last 8 quarters"

```sql
SELECT
  ticker,
  period_end,
  fiscal_period,
  ROUND(numeric_value, 4) AS nav_per_share,
  LAG(ROUND(numeric_value, 4)) OVER (PARTITION BY ticker ORDER BY period_end) AS prev_nav_ps,
  ROUND(
    (numeric_value - LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end))
    / NULLIF(LAG(numeric_value) OVER (PARTITION BY ticker ORDER BY period_end), 0) * 100, 2
  ) AS qoq_change_pct
FROM ahtsa.awm.bdc_time_series
WHERE ticker = 'ARCC'
  AND metric = 'nav_ps'
ORDER BY period_end DESC
LIMIT 8
```

---

## 3. Suggested Genie Space Configuration

### Space Name
`GS AWM — Advisor Intelligence`

### Description
> Natural language access to the Goldman Sachs AWM demo dataset. Ask about IPS drift, portfolio performance, financial fundamentals, analyst estimates, signals and alerts, BDC covenant metrics, and sector exposure across ~250 synthetic UHNW/HNW clients.

### Tables to Register (in priority order)

1. `ahtsa.awm.gold_ips_drift` — Primary IPS drift view
2. `ahtsa.awm.gold_financial_fundamentals` — Unified financials
3. `ahtsa.awm.gold_unified_signals` — Signal feed
4. `ahtsa.awm.gold_financials_vs_estimates` — Beat/miss
5. `ahtsa.awm.gold_portfolio_sector_exposure` — Sector look-through
6. `ahtsa.awm.holdings` — Current positions
7. `ahtsa.awm.clients` — Client profiles
8. `ahtsa.awm.accounts` — Account metadata
9. `ahtsa.awm.transactions` — Trade ledger
10. `ahtsa.awm.ips_targets` — IPS target bands
11. `ahtsa.awm.bdc_fy_snapshot` — BDC annual metrics
12. `ahtsa.awm.bdc_time_series` — BDC quarterly time series
13. `ahtsa.awm.bronze_company_profiles` — Company/sector metadata
14. `ahtsa.awm.bronze_historical_prices` — Daily price history
15. `ahtsa.awm.bronze_indexes_and_vix` — Index benchmark data

### Trusted Assets / Verified Queries to Paste as Context

Paste each `[GENIE]` block above into the Genie space as a trusted query alongside the quoted natural language question. This trains Genie on the correct SQL pattern for each question type.

### Key Terminology to Include in Space Description

- **IPS** — Investment Policy Statement. Each client has a risk profile (e.g., Growth, Balanced, Income) that maps to target allocation bands per asset class.
- **Drift** — The gap between a client's actual allocation and their IPS target. `drift_status` is 'Over Band', 'Under Band', or 'Within Band'.
- **Rebalance to band** — The dollar amount needed to bring an account back inside its IPS min/max bounds (not to exact target).
- **BDC** — Business Development Company. Publicly traded private credit vehicles. 16 BDCs are in scope: ARCC, MAIN, GBDC, FSK, BXSL, OBDC, HTGC, NMFC, PSEC, SLRC, GSBD, CGBD, AINV, OCSL, TCPC, CSWC.
- **PIK** — Payment-in-Kind interest. BDC stress signal: interest accruing but not received in cash.
- **NII** — Net Investment Income. Primary BDC earnings proxy.
- **NAV/share** — Net Asset Value per share. BDC intrinsic value benchmark.
- **Signal** — An AI-generated alert from news, SEC filings, earnings transcripts, or BDC XBRL data. `advisor_action_needed = true` means the signal warrants advisor review.
- **Gold tables** — Pre-joined, analyst-ready tables. Always prefer these over raw bronze tables.
- **Sector look-through** — ETF positions are decomposed into their underlying sector weights via `bronze_etf_sectors`, so exposure queries reflect true economic exposure.

---

## 4. Data Freshness Reference

| Category | Tables | Refresh Cadence |
|---|---|---|
| Prices | `bronze_historical_prices` | Daily (Mon–Fri) |
| Signals | `gold_unified_signals` | Daily |
| Analyst data | `bronze_analyst_estimates`, `bronze_price_targets` | Daily |
| Holdings mark-to-market | `holdings` | Daily (after price refresh) |
| Financials / ratios | `gold_financial_fundamentals`, `gold_financials_vs_estimates` | Monthly |
| BDC XBRL | `bdc_fy_snapshot`, `bdc_time_series` | Monthly |
| Company profiles | `bronze_company_profiles` | Monthly |
| IPS drift | `gold_ips_drift` | Daily (view — live on query) |
