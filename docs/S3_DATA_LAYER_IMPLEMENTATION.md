# AssetEra S3-Only Data Layer Implementation Plan

## 1) Scope and Decisions

This plan defines a single reusable **S3-first data layer** for multiple future projects.

Hard decisions (from your requirements):

- No PostgreSQL metadata database.
- No `serving/project_a` or `serving/project_b` zones.
- Data must be separated by asset class in **every** zone.
- Daily automated refresh with queueing to avoid blocking/rate-limit issues.
- Include macro and fixed-income context from FRED API.


## 2) Target Architecture (S3 + Queue + Worker)

```text
EventBridge Schedule (daily)
  -> Enqueuer Job (EKS CronJob / Lambda / ECS task)
    -> SQS queues (equities, etf, fixed_income, fred)
      -> Worker pods (EKS Deployment)
        -> write to S3 raw
        -> transform to S3 curated
        -> compute indicators/features to S3 features
        -> write run manifests/status to S3 control
```

Optional but recommended:

- AWS Glue Data Catalog + Athena for query/discovery.
- CloudWatch alarms for queue depth and failed jobs.


## 3) S3 Zone Design (Asset-Class Split Everywhere)

Use one bucket, strict zone prefixes:

```text
s3://<DATA_BUCKET>/
  raw/
    equities/
    etf/
    fixed_income/
    macro/fred/
  curated/
    equities/
    etf/
    fixed_income/
    macro/fred/
  features/
    equities/
    etf/
    fixed_income/
    macro/fred/
  control/
    catalog/
    runs/
    manifests/
    checkpoints/
    quality/
```

### 3.1 Raw key pattern

Keep source-faithful payloads, minimal transformation:

```text
raw/<asset_class>/source=<source_name>/symbol_or_series=<id>/dt=<YYYY-MM-DD>/run_id=<run_id>/payload.json.gz
```

Examples:

```text
raw/equities/source=yfinance/symbol_or_series=AAPL/dt=2026-04-19/run_id=20260419T013000Z/payload.json.gz
raw/etf/source=yfinance/symbol_or_series=SPY/dt=2026-04-19/run_id=20260419T013000Z/payload.json.gz
raw/fixed_income/source=yfinance/symbol_or_series=TLT/dt=2026-04-19/run_id=20260419T013000Z/payload.json.gz
raw/macro/fred/source=fred/symbol_or_series=DGS10/dt=2026-04-19/run_id=20260419T013000Z/payload.json.gz
```

### 3.2 Curated key pattern

Standardized typed parquet, partitioned for analytics:

```text
curated/<asset_class>/source=<source_name>/year=<YYYY>/month=<MM>/part-<run_id>-<chunk>.parquet
```

### 3.3 Features key pattern

Derived indicators and macro features:

```text
features/<asset_class>/feature_set=<name>/year=<YYYY>/month=<MM>/part-<run_id>-<chunk>.parquet
```

### 3.4 Control key pattern (no DB replacement layer)

```text
control/catalog/datasets.json
control/runs/date=<YYYY-MM-DD>/run_id=<run_id>/status.json
control/manifests/date=<YYYY-MM-DD>/run_id=<run_id>/written_objects.json
control/checkpoints/<pipeline_name>/<asset_class>.json
control/quality/date=<YYYY-MM-DD>/run_id=<run_id>/quality_report.json
```


## 4) Canonical Schema Contracts

## 4.1 Curated market OHLCV schema (equities/etf/fixed_income ETF proxies)

```text
trade_date        DATE
symbol            STRING
asset_class       STRING   -- equities | etf | fixed_income
open              DOUBLE
high              DOUBLE
low               DOUBLE
close             DOUBLE
adj_close         DOUBLE
volume            DOUBLE
currency          STRING
exchange          STRING
source            STRING   -- yfinance
ingested_at_utc   TIMESTAMP
run_id            STRING
```

## 4.2 Curated FRED schema (macro/fixed-income series)

```text
series_id         STRING   -- e.g., DGS10
observation_date  DATE
value             DOUBLE
realtime_start    DATE
realtime_end      DATE
frequency         STRING   -- Daily, Monthly, Quarterly
units             STRING
seasonal_adjust   STRING
title             STRING
source            STRING   -- fred
ingested_at_utc   TIMESTAMP
run_id            STRING
```

## 4.3 Features schema (all derived analytics)

```text
date              DATE
asset_class       STRING
symbol_or_series  STRING
feature_set       STRING   -- technical_v1 / fixed_income_curve_v1 / macro_v1
feature_name      STRING   -- rsi_14, macd, t10y2y_spread, etc.
feature_value     DOUBLE
window            STRING   -- 14d, 26d, etc.
source_ref        STRING   -- curated object/version reference
generated_at_utc  TIMESTAMP
run_id            STRING
```


## 5) Queueing and Scheduling (to avoid blocking)

Use queue-driven fan-out instead of one large sequential script.

## 5.1 Recommended AWS components

- Scheduler: EventBridge Scheduler (daily).
- Queue: SQS Standard (+ DLQ).
- Compute workers: EKS Deployment (or ECS Service).

## 5.2 Queue layout

- `assetera-ingest-equities`
- `assetera-ingest-etf`
- `assetera-ingest-fixed-income`
- `assetera-ingest-fred`
- `assetera-ingest-dlq` (dead-letter queue)

## 5.3 Message contract

```json
{
  "run_id": "20260419T013000Z",
  "job_type": "ingest",
  "asset_class": "equities",
  "source": "yfinance",
  "symbol_or_series": "AAPL",
  "start_date": "2010-01-01",
  "end_date": "2026-04-19",
  "interval": "1d"
}
```

## 5.4 Worker behavior

1. Receive one message.
2. Pull data from source API.
3. Write raw object to `raw/...`.
4. Normalize and write curated parquet to `curated/...`.
5. Optionally compute immediate features and write to `features/...`.
6. Update run manifest and checkpoint in `control/...`.
7. Acknowledge queue message only after successful S3 writes.

## 5.5 Reliability settings

- Visibility timeout >= 2x worst-case task runtime.
- Max receives = 3-5, then move to DLQ.
- Idempotency key = `run_id + source + symbol_or_series + date_range`.
- Workers must be safe to retry (no data corruption on duplicate runs).


## 6) FRED API: What It Provides and How To Use It

FRED gives macroeconomic and fixed-income related time series (mostly aggregate series, not bond-level tick data).

Common categories:

- US Treasury yields: `DGS1MO`, `DGS3MO`, `DGS2`, `DGS5`, `DGS10`, `DGS30`
- Fed policy / short rates: `DFF`, `SOFR`
- Inflation: `CPIAUCSL`, `PCEPI`
- Growth/labor: `GDPC1`, `UNRATE`, `PAYEMS`
- Credit stress/spreads: `BAMLH0A0HYM2` (high-yield OAS), etc.

## 6.1 Core endpoints

- Series metadata:
  - `GET https://api.stlouisfed.org/fred/series`
- Observations:
  - `GET https://api.stlouisfed.org/fred/series/observations`

Required params:

- `api_key`
- `file_type=json`
- `series_id=<ID>`

Optional params:

- `observation_start=YYYY-MM-DD`
- `observation_end=YYYY-MM-DD`
- `frequency`
- `units`

Example request:

```bash
curl "https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key=${FRED_API_KEY}&file_type=json&observation_start=2000-01-01"
```

## 6.2 FRED ingestion pattern

1. Pull metadata from `/fred/series`.
2. Pull observations from `/fred/series/observations`.
3. Store original response in raw zone.
4. Parse into typed rows and store parquet in curated.
5. Generate macro/fixed-income features into features zone (optional in same run).


## 7) Sequence Plan (Implementation Order)

## Phase 0: Foundation

1. Create bucket and prefixes (`raw/`, `curated/`, `features/`, `control/`).
2. Enable bucket versioning.
3. Add lifecycle policies (raw long retention, intermediate retention as needed).
4. Add IAM roles for workers (least privilege on bucket prefixes + SQS access).

## Phase 1: Contracts and Catalog

1. Freeze canonical schemas in code/docs.
2. Create asset universe lists:
   - equities symbols
   - ETF symbols
   - fixed-income proxies (e.g., TLT/IEF/HYG/LQD) or bond data source list
   - FRED series list
3. Initialize `control/catalog/datasets.json` with dataset definitions.

## Phase 2: Ingestion MVP

1. Build enqueuer job to publish messages per symbol/series.
2. Build worker for yfinance ingestion (equities + etf + fixed_income proxies).
3. Build worker path for FRED ingestion.
4. Write raw + curated objects and run manifest.
5. Add retries + DLQ.

## Phase 3: Daily Automation

1. EventBridge schedule for daily run (example: 07:00 Asia/Kolkata).
2. Run enqueuer daily with run_id timestamp.
3. Autoscale workers by queue depth.
4. Emit status to `control/runs/.../status.json`.

## Phase 4: Features Layer

1. Technical features for market data:
   - RSI, MACD, Bollinger, volatility, returns.
2. Fixed-income/macro features:
   - yield spreads (`DGS10 - DGS2`)
   - regime labels
   - rate momentum
3. Store into `features/...` with versioned feature_set names.

## Phase 5: Data Quality and Governance

1. Add checks:
   - missing dates, duplicate keys, null critical fields, stale series.
2. Write quality reports to `control/quality/...`.
3. Add alerting on failure or staleness.


## 8) Suggested Initial Dataset Set

## 8.1 Equities and ETF

- Start with your existing allowlists from code.
- Keep one symbol per queue message.

## 8.2 Fixed-income market proxies

- `IEF`, `TLT`, `SHY`, `LQD`, `HYG`, `BND`, `AGG` (if available in your universe).

## 8.3 FRED starter pack

- Rates/curve: `DGS1MO`, `DGS3MO`, `DGS2`, `DGS5`, `DGS10`, `DGS30`, `DFF`, `SOFR`
- Inflation: `CPIAUCSL`, `PCEPI`
- Growth/labor: `GDPC1`, `UNRATE`, `PAYEMS`
- Credit spread: `BAMLH0A0HYM2`


## 9) Security and Secrets

- Do not hardcode secrets in code or docs.
- Keep `FRED_API_KEY` in environment variables or secrets manager.
- For EKS, inject secrets via Kubernetes Secret or external secret manager.

Suggested env vars:

```env
DATA_BUCKET=<your-bucket>
AWS_REGION=ap-south-1
FRED_API_KEY=<set-in-runtime-secret>
QUEUE_EQUITIES=assetera-ingest-equities
QUEUE_ETF=assetera-ingest-etf
QUEUE_FIXED_INCOME=assetera-ingest-fixed-income
QUEUE_FRED=assetera-ingest-fred
```


## 10) Definition of Done

The S3 data layer is considered ready when:

1. Daily schedule runs automatically without manual trigger.
2. Queue processes all asset classes without blocking.
3. Raw, curated, and features zones are populated and separated by asset class.
4. FRED series are ingested daily (or by configured frequency) into raw+curated.
5. `control/runs/.../status.json` clearly indicates success/failure and counts.
6. Failed messages land in DLQ with enough context for replay.


## 11) Immediate Next Steps (Execution Checklist)

1. Finalize dataset lists (equities/ETF/fixed-income/FRED series).
2. Create SQS queues + DLQ.
3. Implement enqueuer script.
4. Implement worker script with one-message-at-a-time processing.
5. Wire EventBridge daily schedule.
6. Validate one full end-to-end run and inspect S3 outputs.
7. Add quality checks and alerts.

