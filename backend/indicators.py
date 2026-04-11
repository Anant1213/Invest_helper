"""
Technical Indicators & Quantitative Finance Utilities
======================================================
RSI, Bollinger Bands, MACD — for Market Watch charts.
Monte Carlo (GBM), Max Drawdown, Beta/Alpha, Sortino/Calmar — for Backtester & Risk Profiler.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from typing import Tuple


# ── Technical Indicators ──────────────────────────────────────────────

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index (Wilder smoothing)."""
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(window=period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(window=period, min_periods=period).mean()
    rs = gain / loss.replace(0, np.finfo(float).eps)
    return (100 - 100 / (1 + rs)).rename("RSI")


def bollinger_bands(
    close: pd.Series, period: int = 20, n_std: float = 2.0
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Returns (upper, middle, lower) Bollinger Bands."""
    ma = close.rolling(window=period, min_periods=period).mean()
    std = close.rolling(window=period, min_periods=period).std()
    return (ma + n_std * std).rename("BB_upper"), ma.rename("BB_mid"), (ma - n_std * std).rename("BB_lower")


def macd(
    close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Returns (macd_line, signal_line, histogram)."""
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    macd_line = (ema_fast - ema_slow).rename("MACD")
    signal_line = macd_line.ewm(span=signal, adjust=False).mean().rename("Signal")
    histogram = (macd_line - signal_line).rename("Histogram")
    return macd_line, signal_line, histogram


def atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """Average True Range."""
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    return tr.rolling(window=period).mean().rename("ATR")


# ── Portfolio Risk Metrics ────────────────────────────────────────────

def max_drawdown(equity: pd.Series) -> float:
    """Maximum peak-to-trough drawdown (as a negative fraction, e.g. -0.35)."""
    rolling_max = equity.expanding(min_periods=1).max()
    drawdown = (equity - rolling_max) / rolling_max
    return float(drawdown.min())


def drawdown_series(equity: pd.Series) -> pd.Series:
    """Full drawdown series for plotting."""
    rolling_max = equity.expanding(min_periods=1).max()
    return ((equity - rolling_max) / rolling_max).rename("Drawdown")


def compute_beta_alpha(
    fund_returns: pd.Series,
    market_returns: pd.Series,
    rf_annual: float = 0.0,
) -> Tuple[float, float]:
    """
    Beta (market sensitivity) and annualised Alpha vs a market index.
    Returns (beta, alpha_annual). Alpha = Jensen's alpha.
    """
    aligned = pd.concat([fund_returns, market_returns], axis=1).dropna()
    if len(aligned) < 20:
        return np.nan, np.nan
    f = aligned.iloc[:, 0].values
    m = aligned.iloc[:, 1].values
    cov = np.cov(f, m)
    beta = float(cov[0, 1] / cov[1, 1]) if cov[1, 1] != 0 else np.nan
    rf_daily = rf_annual / 252
    alpha_daily = float(np.mean(f - rf_daily) - beta * np.mean(m - rf_daily))
    return beta, alpha_daily * 252


def sortino_ratio(equity: pd.Series, rf_annual: float = 0.0) -> float:
    """Sortino ratio using downside deviation (returns < rf)."""
    returns = equity.pct_change(fill_method=None).dropna()
    rf_daily = rf_annual / 252
    excess = returns - rf_daily
    downside = returns[returns < rf_daily]
    if len(downside) < 2:
        return np.nan
    downside_std = float(downside.std()) * np.sqrt(252)
    return float(excess.mean() * 252 / downside_std) if downside_std > 0 else np.nan


def calmar_ratio(equity: pd.Series, years: float) -> float:
    """Calmar ratio = CAGR / |Max Drawdown|."""
    if years <= 0 or equity.empty:
        return np.nan
    cagr = float(equity.iloc[-1] / equity.iloc[0]) ** (1 / years) - 1
    mdd  = abs(max_drawdown(equity))
    return float(cagr / mdd) if mdd > 0 else np.nan


# ── Monte Carlo Simulation (GBM) ──────────────────────────────────────

def monte_carlo_gbm(
    initial: float,
    annual_return: float,
    annual_vol: float,
    n_years: int = 20,
    n_sims: int = 1000,
    seed: int = 42,
) -> np.ndarray:
    """
    Simulate portfolio growth via Geometric Brownian Motion.

    Returns array of shape (n_sims, n_steps) where n_steps = n_years * 252.
    Each row is one simulated path, starting at `initial`.

    The drift uses Ito's correction:  μ_adj = μ - 0.5σ²
    """
    rng = np.random.default_rng(seed)
    dt = 1 / 252
    n_steps = int(n_years * 252)

    # Daily drift and volatility
    drift = (annual_return - 0.5 * annual_vol ** 2) * dt
    diffusion = annual_vol * np.sqrt(dt)

    # Simulate returns: shape (n_sims, n_steps)
    shocks = rng.standard_normal((n_sims, n_steps))
    log_returns = drift + diffusion * shocks

    # Cumulative product → portfolio value paths
    paths = initial * np.exp(np.cumsum(log_returns, axis=1))
    return paths


def mc_percentiles(paths: np.ndarray) -> dict[str, np.ndarray]:
    """Return percentile bands from Monte Carlo paths."""
    return {
        "p5":  np.percentile(paths, 5,  axis=0),
        "p25": np.percentile(paths, 25, axis=0),
        "p50": np.percentile(paths, 50, axis=0),
        "p75": np.percentile(paths, 75, axis=0),
        "p95": np.percentile(paths, 95, axis=0),
    }
