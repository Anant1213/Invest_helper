"""Ticker metadata organised by asset-class category."""

CATALOG = {
    "Core Market": {
        "S&P 500 (SPY)": "SPY",
        "Nasdaq-100 (QQQ)": "QQQ",
        "Dow 30 (DIA)": "DIA",
        "Russell 2000 (IWM)": "IWM",
        "S&P 500 Equal-Weight (RSP)": "RSP",
    },
    "Sectors": {
        "Technology (XLK)": "XLK",
        "Financials (XLF)": "XLF",
        "Energy (XLE)": "XLE",
    },
    "Rates, Credit & Macro": {
        "US Aggregate (AGG)": "AGG",
        "High Yield (HYG)": "HYG",
        "US Dollar (UUP)": "UUP",
        "Gold (GLD)": "GLD",
        "Crude Oil (USO)": "USO",
    },
}

DEFAULT_SELECTION = ["SPY", "QQQ", "DIA", "IWM", "RSP", "XLK", "XLF", "XLE", "AGG", "HYG", "UUP", "GLD", "USO"]

# Flat list of all tickers (for quick lookup)
ALL_TICKERS = sorted(
    {t for grp in CATALOG.values() for t in grp.values()}
)
