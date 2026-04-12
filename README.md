# AssetEra

AssetEra is a multi-page Streamlit portfolio intelligence platform that combines:
- market analytics and technical indicators,
- institutional-style backtesting,
- ML-based investor risk profiling,
- and an OpenAI-powered portfolio Q&A assistant.

The app supports a PostgreSQL-backed market data layer for production, with scheduled daily sync from Yahoo Finance.

## Table of Contents
- [Architecture](#architecture)
- [Codebase Walkthrough](#codebase-walkthrough)
- [Feature Pages](#feature-pages)
- [Quant and ML Methods](#quant-and-ml-methods)
- [Setup and Run](#setup-and-run)
- [Docker Run](#docker-run)
- [Environment Variables](#environment-variables)
- [Data and Model Artifacts](#data-and-model-artifacts)
- [Troubleshooting](#troubleshooting)
- [Disclaimer](#disclaimer)

## Architecture

```mermaid
flowchart LR
  U[User Browser]

  subgraph S[Streamlit App]
    A[app.py\nLanding + navigation]
    P1[pages/1_Market_Watch.py]
    P2[pages/2_Fund_Backtester.py]
    P3[pages/3_Risk_Profiler.py]
    P4[pages/4_AI_Advisor.py]
  end

  subgraph B[Backend Modules]
    M[backend/market.py\nFetch + cache + metrics]
    PG[backend/postgres_store.py\nPostgreSQL adapter]
    I[backend/indicators.py\nTech + risk functions]
    R[backend/risk_model.py\nGBT risk model + fund mapping]
    AI[backend/ai_advisor.py\nPrompt + streaming wrapper]
    UI[backend/ui.py\nDesign system + reusable components]
    DC[backend/data_catalog.py\nTicker catalog]
  end

  subgraph L[Data Stores]
    DB[(PostgreSQL\nmarket_prices)]
    C[data_cache/*.csv\noptional one-time import]
    PM[models/risk_model.pkl]
  end

  subgraph X[External Services]
    YF[Yahoo Finance via yfinance]
    OAI[OpenAI Chat Completions]
  end

  U --> S
  S --> B
  M <--> DB
  C --> PG
  M --> PG
  M --> YF
  R <--> PM
  AI --> OAI
```

### Market data freshness flow

```mermaid
sequenceDiagram
  participant UI as Streamlit Page
  participant MK as backend.market.fetch_prices
  participant DB as PostgreSQL market_prices
  participant YF as yfinance

  UI->>MK: fetch_prices(tickers, period, interval)
  MK->>DB: read cached 1d OHLCV
  MK->>MK: check last cached business day
  alt cache is stale
    MK->>YF: download missing date range only
    YF-->>MK: new rows
    MK->>DB: upsert deduped rows
  else cache is fresh
    MK-->>MK: reuse DB data
  end
  MK->>MK: optional 1d->1wk/1mo resample
  MK->>MK: trim by period
  MK-->>UI: DataFrame + per-ticker errors
```

### Risk and advisor context flow

```mermaid
flowchart TD
  IN[User risk questionnaire] --> PR[predict_risk()]
  PR --> RM[GradientBoosting model]
  RM --> RP[Risk score 1-5 + probabilities]
  RP --> RF[recommend_funds()]
  RP --> SS[st.session_state.user_risk_profile]
  SS --> AD[AI Advisor page]
  AD --> SP[get_system_prompt(user_risk)]
  SP --> OA[OpenAI streamed response]
```

## Codebase Walkthrough

### Top-level structure

```text
.
├── app.py
├── backend/
│   ├── ai_advisor.py
│   ├── data_catalog.py
│   ├── indicators.py
│   ├── market.py
│   ├── postgres_store.py
│   ├── risk_model.py
│   └── ui.py
├── pages/
│   ├── 1_Market_Watch.py
│   ├── 2_Fund_Backtester.py
│   ├── 3_Risk_Profiler.py
│   └── 4_AI_Advisor.py
├── scripts/
│   ├── load_data_to_postgres.py
│   └── daily_market_sync.py
├── data_cache/
├── models/
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

### Module responsibilities

| Module | Responsibility |
|---|---|
| `app.py` | Landing page, ticker tape, feature cards, navigation to all pages. |
| `backend/market.py` | Allowlisted ticker validation, PostgreSQL-first caching, incremental yfinance updates, period trimming, metrics/correlation helpers. |
| `backend/postgres_store.py` | PostgreSQL connection/schema setup, ticker reads, upserts, and CSV import helpers. |
| `backend/indicators.py` | RSI, Bollinger Bands, MACD, ATR, drawdown, beta/alpha, Sortino/Calmar, Monte Carlo GBM utilities. |
| `backend/risk_model.py` | Synthetic training data generation, Gradient Boosting model persistence, risk prediction, fund recommendation mapping. |
| `backend/ai_advisor.py` | Fund-aware system prompt construction and streaming OpenAI chat response wrapper. |
| `backend/ui.py` | Shared CSS theme and reusable UI components (page headers, ticker tape, KPI strips, cards). |
| `backend/data_catalog.py` | Grouped ticker catalog for sidebar selectors. |

## Feature Pages

### 1) Market Watch (`pages/1_Market_Watch.py`)
- Multi-ticker selection from categorized universe.
- Candlestick chart with optional overlays:
  - Bollinger Bands (20,2)
  - RSI (14)
  - MACD (12,26,9)
  - Volume bars
- Normalized performance comparison (index base = 100).
- Correlation heatmap.
- CSV export (summary + timeseries).

### 2) Fund Backtester (`pages/2_Fund_Backtester.py`)
- Simulates predefined funds (F1-F5) using historical returns.
- Optional annual rebalance and fee application.
- Benchmarks: single-ticker and blended (60/40, 80/20, All-Weather).
- Metrics include:
  - Final value, total return, CAGR
  - Sharpe, Sortino, Calmar
  - Max drawdown
  - Beta, annualized alpha
  - VaR/CVaR, upside/downside capture
- Visuals: equity curve, drawdown area, rolling 12M, yearly returns, distribution charts.

### 3) Risk Profiler (`pages/3_Risk_Profiler.py`)
- Collects demographic and behavioral inputs.
- Predicts risk profile (1 to 5) with class probabilities.
- Recommends matching funds from `FUND_PROFILES`.
- Runs Monte Carlo GBM simulation with percentile fan chart.
- Shows model explainability and feature importances.

### 4) AI Advisor (`pages/4_AI_Advisor.py`)
- Chat assistant for fund/risk education and portfolio Q&A.
- Injects fund definitions and optional user risk score into system context.
- Streams responses from OpenAI (`gpt-4o-mini` in current code).
- Maintains conversation history in session state.

## Quant and ML Methods

### Technical analysis
- Relative Strength Index (RSI)
- Bollinger Bands
- MACD
- ATR (utility present in backend)

### Portfolio analytics
- Max drawdown and drawdown series
- Beta and Jensen alpha (annualized)
- Sharpe, Sortino, Calmar
- Rolling/yearly return views
- VaR and CVaR from monthly returns
- Upside/downside capture ratios

### ML risk model
- Algorithm: `GradientBoostingClassifier`
- Training source: synthetic dataset generated from financial planning heuristics
- Training samples: 3,000
- Features: age, income, dependents, marital/employment encoding, horizon, loss tolerance, experience
- Persistence: `models/risk_model.pkl`
- Load behavior: cached resource; retrains only if model file is absent/corrupted

## Setup and Run

### Prerequisites
- Python 3.10+
- `pip`

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Configure environment

```bash
cp .env.example .env
```

Set at least:

```env
OPENAI_API_KEY=sk-proj-...
POSTGRES_URL=postgresql://user:password@your-host:5432/assetera?sslmode=require
```

### Optional one-time CSV import (bootstrap only)

```bash
python scripts/load_data_to_postgres.py --dir data_cache --pattern "*_1d.csv"
```

### Daily batch sync to PostgreSQL

```bash
python scripts/daily_market_sync.py --strict
```

Example cron (UTC 23:00 daily):

```bash
0 23 * * * cd /path/to/assetera && /usr/bin/env python3 scripts/daily_market_sync.py --strict >> /var/log/assetera_sync.log 2>&1
```

### Start app

```bash
streamlit run app.py
```

Default URL: `http://localhost:8501`

## Docker Run

```bash
docker compose up --build
```

Notes:
- Uses remote PostgreSQL from `POSTGRES_URL` in `.env`.
- Ensure `POSTGRES_URL` is set before running compose.
- Port mapping: `8501:8501`
- Environment variables are loaded from `.env`

## Environment Variables

Current `.env.example` includes:
- `OPENAI_API_KEY` (required for AI Advisor)
- `POSTGRES_URL`/`DATABASE_URL` (required for PostgreSQL-backed market store)
- commented Snowflake placeholders (legacy workflow only)

## Data and Model Artifacts

- `data_cache/*.csv`
  - Optional bootstrap source for first import.
- `market_prices` (PostgreSQL table)
  - Canonical OHLCV store used by `backend/market.py` when `POSTGRES_URL` is set.
  - Incrementally upserted when stale via yfinance.
- `models/risk_model.pkl`
  - Persisted ML model for risk profiling.
  - Rebuilt automatically if missing.

## Troubleshooting

- `OPENAI_API_KEY not configured` on AI page:
  - Add `OPENAI_API_KEY` to `.env` and restart Streamlit.

- Empty charts/backtests:
  - Verify selected tickers are in allowlist and date window has overlap.
  - Check internet connectivity if local cache is missing and refresh is needed.

- PostgreSQL errors:
  - Verify `POSTGRES_URL` in `.env`.
  - Ensure the DB is reachable and credentials are valid.
  - Run daily sync using `python scripts/daily_market_sync.py --strict`.
  - If using Supabase shared pooler and you see `prepared statement "_pg3_0" already exists`, ensure psycopg prepared statements are disabled (already configured in `backend/postgres_store.py`).

- First run feels slow:
  - Initial model training and/or first sync can add startup latency.
  - Subsequent runs are faster due to `st.cache_data`, `st.cache_resource`, and DB/cache reuse.

## Disclaimer

AssetEra is an educational analytics project. It does not provide licensed investment advice. Past performance and simulated outcomes do not guarantee future results.
