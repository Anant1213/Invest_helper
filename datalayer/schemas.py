"""
datalayer.schemas
─────────────────
Canonical schema contracts, asset universe lists, env config,
and SQS message contract.

All public names are importable directly from datalayer.schemas.
"""
from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Final

# ── Env config ──────────────────────────────────────────────────────────
DATA_BUCKET:       str = os.getenv("DATA_BUCKET", "")
AWS_REGION:        str = os.getenv("AWS_REGION", "us-east-1")
FRED_API_KEY:      str = os.getenv("FRED_API_KEY", "")
QUEUE_EQUITIES:    str = os.getenv("QUEUE_EQUITIES",    "assetera-ingest-equities")
QUEUE_ETF:         str = os.getenv("QUEUE_ETF",         "assetera-ingest-etf")
QUEUE_FIXED_INCOME:str = os.getenv("QUEUE_FIXED_INCOME","assetera-ingest-fixed-income")
QUEUE_FRED:        str = os.getenv("QUEUE_FRED",        "assetera-ingest-fred")

# ── Date defaults ───────────────────────────────────────────────────────
TODAY:          str = date.today().isoformat()
HISTORY_START:  str = (date.today() - timedelta(days=365 * 10 + 5)).isoformat()

# ── Asset class constants ───────────────────────────────────────────────
ASSET_EQUITIES:     Final[str] = "equities"
ASSET_ETF:          Final[str] = "etf"
ASSET_FIXED_INCOME: Final[str] = "fixed_income"
ASSET_MACRO:        Final[str] = "macro"

ASSET_CLASS_QUEUE: dict[str, str] = {
    ASSET_EQUITIES:     QUEUE_EQUITIES,
    ASSET_ETF:          QUEUE_ETF,
    ASSET_FIXED_INCOME: QUEUE_FIXED_INCOME,
    ASSET_MACRO:        QUEUE_FRED,
}

ASSET_CLASS_SOURCE: dict[str, str] = {
    ASSET_EQUITIES:     "yfinance",
    ASSET_ETF:          "yfinance",
    ASSET_FIXED_INCOME: "yfinance",
    ASSET_MACRO:        "fred",
}

# ── Equity universe (NYSE + NASDAQ) ────────────────────────────────────
EQUITIES_LARGE: list[str] = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL",
    "META", "TSLA", "AVGO", "JPM",  "LLY",
    "V",    "UNH",  "XOM",  "MA",   "JNJ",
    "PG",   "HD",   "COST", "MRK",  "ABBV",
    "WMT",  "BAC",  "NFLX", "CRM",  "CVX",
    "AMD",  "ORCL", "PEP",  "KO",   "TMO",
    "ACN",  "MCD",  "CSCO", "WFC",  "GE",
    "NOW",  "ADBE", "TXN",  "QCOM", "DHR",
    "PM",   "CAT",  "AMGN", "INTU", "SPGI",
    "MS",   "GS",   "IBM",  "RTX",  "BRK-B",
]

EQUITIES_MID: list[str] = [
    "DECK", "SAIA", "BURL", "GNRC", "CLH",
    "TXRH", "KTOS", "LSTR", "MATX", "BJ",
    "SFM",  "ELS",  "ITT",  "AWI",  "NVT",
    "UFPI", "ATI",  "WMS",  "GATX", "MTB",
    "CINF", "CFG",  "ZION", "RJF",  "NTRS",
    "AIZ",  "STE",  "PNW",  "ATO",  "WTFC",
    "RGEN", "BLD",  "EXPO", "PRI",  "HLI",
    "OHI",  "LCII", "HLNE", "RNR",  "RGLD",
    "CBSH", "BOOT", "CUBE", "LPLA", "NEU",
    "WEX",  "SBCF", "MMSI", "CRVL", "FHB",
]

EQUITIES_SMALL: list[str] = [
    "ABM",  "AMSF", "CAKE", "CATO", "CENX",
    "CHCO", "CSWC", "DLB",  "DXPE", "FCPT",
    "FFIN", "HWKN", "IPAR", "JJSF", "KALU",
    "KFRC", "LGND", "MRTN", "MTRN", "NTGR",
    "OFG",  "PKOH", "PRK",  "PRDO", "RUSHA",
    "YORW", "SMPL", "STBA", "SYBT", "TRMK",
    "UFPT", "WEYS", "ZEUS", "BANF", "HFWA",
    "JELD", "DGII", "ASO",  "BLBD", "CAL",
    "CULP", "FRPH", "HAIN", "NMIH", "LQDT",
    "RAMP", "MGNI", "DIN",  "COFS", "SPFI",
]

EQUITIES_CAP: dict[str, str] = (
    {t: "LARGE" for t in EQUITIES_LARGE}
    | {t: "MID"   for t in EQUITIES_MID}
    | {t: "SMALL" for t in EQUITIES_SMALL}
)

ALL_EQUITIES: list[str] = EQUITIES_LARGE + EQUITIES_MID + EQUITIES_SMALL

# ── ETF universe ────────────────────────────────────────────────────────
ETF_TICKERS: list[str] = [
    # Broad market
    "SPY", "QQQ", "IWM", "DIA", "VTI", "VOO",
    # Sector
    "XLK", "XLF", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB", "XLRE",
    # International
    "EFA", "EEM", "VEA", "VWO",
    # Commodities / alts
    "GLD", "SLV", "USO", "UNG",
    # Multi-asset
    "AOM", "AOR", "AOA",
]

# ── Fixed income proxy universe ─────────────────────────────────────────
FIXED_INCOME_TICKERS: list[str] = [
    "IEF",  # 7-10yr Treasury
    "TLT",  # 20+yr Treasury
    "SHY",  # 1-3yr Treasury
    "LQD",  # IG Corporate
    "HYG",  # High Yield
    "BND",  # Total Bond Market
    "AGG",  # US Aggregate Bond
    "MUB",  # Municipal Bond
    "TIP",  # TIPS
    "EMB",  # Emerging Market Bond
]

# ── FRED series catalog ─────────────────────────────────────────────────
FRED_SERIES: list[dict] = [
    # US Treasury yields / curve
    {"series_id": "DGS1MO",       "title": "1-Month Treasury Rate",      "category": "rates"},
    {"series_id": "DGS3MO",       "title": "3-Month Treasury Rate",      "category": "rates"},
    {"series_id": "DGS2",         "title": "2-Year Treasury Rate",       "category": "rates"},
    {"series_id": "DGS5",         "title": "5-Year Treasury Rate",       "category": "rates"},
    {"series_id": "DGS10",        "title": "10-Year Treasury Rate",      "category": "rates"},
    {"series_id": "DGS30",        "title": "30-Year Treasury Rate",      "category": "rates"},
    # Fed policy / short rates
    {"series_id": "DFF",          "title": "Fed Funds Rate",             "category": "rates"},
    {"series_id": "SOFR",         "title": "SOFR",                       "category": "rates"},
    # Inflation
    {"series_id": "CPIAUCSL",     "title": "CPI All Urban Consumers",    "category": "inflation"},
    {"series_id": "PCEPI",        "title": "PCE Price Index",            "category": "inflation"},
    # Growth / labor
    {"series_id": "GDPC1",        "title": "Real GDP",                   "category": "growth"},
    {"series_id": "UNRATE",       "title": "Unemployment Rate",          "category": "labor"},
    {"series_id": "PAYEMS",       "title": "Nonfarm Payrolls",           "category": "labor"},
    # Credit spreads
    {"series_id": "BAMLH0A0HYM2", "title": "High-Yield OAS (BofA)",     "category": "credit"},
]

FRED_SERIES_IDS: list[str] = [s["series_id"] for s in FRED_SERIES]

# ── Canonical column lists ──────────────────────────────────────────────
OHLCV_COLUMNS: list[str] = [
    "trade_date", "symbol", "asset_class",
    "open", "high", "low", "close", "adj_close", "volume",
    "currency", "exchange", "source", "ingested_at_utc", "run_id",
]

FRED_COLUMNS: list[str] = [
    "series_id", "observation_date", "value",
    "realtime_start", "realtime_end", "frequency",
    "units", "seasonal_adjust", "title",
    "source", "ingested_at_utc", "run_id",
]

FEATURES_COLUMNS: list[str] = [
    "date", "asset_class", "symbol_or_series",
    "feature_set", "feature_name", "feature_value",
    "window", "source_ref", "generated_at_utc", "run_id",
]


# ── SQS message contract ────────────────────────────────────────────────
def make_message(
    asset_class: str,
    symbol_or_series: str,
    source: str,
    run_id: str,
    start_date: str = HISTORY_START,
    end_date: str = TODAY,
    interval: str = "1d",
    cap: str | None = None,
) -> dict:
    """Build a canonical ingest job message."""
    msg: dict = {
        "run_id":           run_id,
        "job_type":         "ingest",
        "asset_class":      asset_class,
        "source":           source,
        "symbol_or_series": symbol_or_series,
        "start_date":       start_date,
        "end_date":         end_date,
        "interval":         interval,
    }
    if cap:
        msg["cap"] = cap
    return msg
