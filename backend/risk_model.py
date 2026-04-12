"""
ML-based Risk Profiler — predicts investor risk profile (1-5) from demographics.

Uses Gradient Boosting trained on synthetic data derived from standard financial
planning heuristics (age, income, dependents, horizon, loss tolerance, etc.).

Model is persisted to models/risk_model.pkl on first run and loaded from disk
on every subsequent startup — no retraining needed.
"""

from __future__ import annotations

import pickle
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import LabelEncoder
import streamlit as st

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
_MODEL_DIR = _ROOT / "models"
_MODEL_PATH = _MODEL_DIR / "risk_model.pkl"

# ── Fund definitions ──────────────────────────────────────────────────
FUND_PROFILES = {
    "F1": {
        "name": "Fund 1 — Core Income",
        "risk_range": (1, 2),
        "description": "Conservative: 70% bonds, 20% intl equity, 10% gold",
        "expected_return": "4-6% annualised",
        "allocations": {
            "LQD": 0.50, "IEF": 0.20, "GLD": 0.10, "VEA": 0.10,
            "MSFT": 0.02, "APH": 0.02, "GWW": 0.02, "PH": 0.02, "BSX": 0.02,
        },
    },
    "F2": {
        "name": "Fund 2 — Pro Core",
        "risk_range": (2, 3),
        "description": "Moderate: 40% bonds, 24% equity, 12% intl, 8% gold",
        "expected_return": "8-12% annualised",
        "allocations": {
            "LQD": 0.30, "IEF": 0.10, "GLD": 0.08, "VEA": 0.12, "SPY": 0.12,
            "MSFT": 0.03, "APH": 0.03, "GWW": 0.03, "PH": 0.03, "BSX": 0.03,
            "ETN": 0.03, "EME": 0.025, "PWR": 0.025, "FAST": 0.025, "BWXT": 0.025,
        },
    },
    "F3": {
        "name": "Fund 3 — Pro Growth 17",
        "risk_range": (3, 4),
        "description": "Growth: 20% bonds, 40% tech/industrials, 12% intl, 11% gold",
        "expected_return": "12-17% annualised",
        "allocations": {
            "LQD": 0.098, "IEF": 0.098, "SPY": 0.060, "VEA": 0.120, "GLD": 0.112,
            "NVDA": 0.025, "AVGO": 0.025, "MSFT": 0.025, "KLAC": 0.025,
            "CDNS": 0.025, "ETN": 0.025, "PH": 0.025, "HEI": 0.025,
            "EME": 0.025, "PWR": 0.025, "FAST": 0.025, "BWXT": 0.025,
        },
    },
    "F5": {
        "name": "Fund 5 — Bridge Growth 26",
        "risk_range": (4, 4),
        "description": "Aggressive growth: tech-heavy with EM and gold hedge",
        "expected_return": "17-26% annualised",
        "allocations": {
            "NVDA": 0.10, "AVGO": 0.07, "KLAC": 0.06, "CDNS": 0.05,
            "MSFT": 0.05, "ETN": 0.05, "EME": 0.04, "PWR": 0.04,
            "VEA": 0.08, "GLD": 0.04, "VWO": 0.02,
        },
    },
    "F4": {
        "name": "Fund 4 — Redeem Surge 31",
        "risk_range": (5, 5),
        "description": "Maximum growth: 48% semiconductor, 32% EM, 15% gold",
        "expected_return": "25-35%+ annualised",
        "allocations": {
            "NVDA": 0.24, "AVGO": 0.12, "KLAC": 0.06, "CDNS": 0.06,
            "VWO": 0.32, "GLD": 0.15,
        },
    },
}

RISK_LABELS = {
    1: "Very Conservative",
    2: "Conservative",
    3: "Moderate",
    4: "Aggressive",
    5: "Very Aggressive",
}


# ── synthetic data generation ─────────────────────────────────────────
def _generate_training_data(n: int = 3000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    age = rng.integers(22, 76, n)
    annual_income = np.clip(rng.lognormal(10.9, 0.6, n), 20_000, 800_000).astype(int)
    dependents = rng.choice([0, 1, 2, 3, 4, 5], n, p=[0.25, 0.22, 0.25, 0.15, 0.08, 0.05])
    marital = rng.choice(
        ["single", "married", "divorced", "widowed"], n,
        p=[0.30, 0.45, 0.15, 0.10],
    )
    horizon = np.clip(rng.normal(15, 7, n), 1, 35).astype(int)
    loss_tolerance = np.clip(rng.normal(20, 10, n), 2, 55).astype(int)
    experience = rng.choice([0, 1, 2, 3], n, p=[0.20, 0.30, 0.30, 0.20])
    employment = rng.choice(
        ["stable", "variable", "retired"], n,
        p=[0.55, 0.25, 0.20],
    )

    retired_mask = age >= 62
    employment[retired_mask] = rng.choice(
        ["retired", "stable"], retired_mask.sum(), p=[0.7, 0.3]
    )
    horizon[retired_mask] = np.clip(horizon[retired_mask], 1, 15)

    income_pct = (annual_income - annual_income.min()) / (annual_income.max() - annual_income.min() + 1)

    base = np.full(n, 3.0)
    base += (loss_tolerance - 20) * 0.055
    base += (horizon - 12) * 0.045
    base -= (age - 40) * 0.025
    base += experience * 0.35
    base -= dependents * 0.18
    base += (income_pct - 0.5) * 0.8

    marital_adj = np.where(marital == "single", 0.15, 0.0)
    marital_adj += np.where(marital == "widowed", -0.2, 0.0)
    base += marital_adj

    emp_adj = np.where(employment == "retired", -0.3, 0.0)
    emp_adj += np.where(employment == "variable", -0.1, 0.0)
    base += emp_adj

    noise = rng.normal(0, 0.35, n)
    risk_profile = np.clip(np.round(base + noise), 1, 5).astype(int)

    return pd.DataFrame({
        "age": age,
        "annual_income": annual_income,
        "dependents": dependents,
        "marital_status": marital,
        "investment_horizon": horizon,
        "loss_tolerance_pct": loss_tolerance,
        "investment_experience": experience,
        "employment_type": employment,
        "risk_profile": risk_profile,
    })


# ── feature engineering ───────────────────────────────────────────────
_le_marital = LabelEncoder()
_le_employment = LabelEncoder()
_le_marital.fit(["single", "married", "divorced", "widowed"])
_le_employment.fit(["stable", "variable", "retired"])


def _prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["marital_enc"] = _le_marital.transform(out["marital_status"])
    out["employment_enc"] = _le_employment.transform(out["employment_type"])
    feature_cols = [
        "age", "annual_income", "dependents", "marital_enc",
        "investment_horizon", "loss_tolerance_pct",
        "investment_experience", "employment_enc",
    ]
    return out[feature_cols]


# ── model persistence ─────────────────────────────────────────────────
def _train_and_save() -> tuple:
    """Train model, save to disk, return (model, accuracy, importances)."""
    logger.info("Training risk model from scratch...")
    data = _generate_training_data(n=3000)
    X = _prepare_features(data)
    y = data["risk_profile"]

    model = GradientBoostingClassifier(
        n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42,
    )
    scores = cross_val_score(model, X, y, cv=5, scoring="accuracy")
    model.fit(X, y)

    importances = pd.Series(
        model.feature_importances_, index=X.columns,
    ).sort_values(ascending=False)

    accuracy = float(scores.mean())

    # Persist to disk
    _MODEL_DIR.mkdir(parents=True, exist_ok=True)
    with open(_MODEL_PATH, "wb") as f:
        pickle.dump({"model": model, "accuracy": accuracy, "importances": importances}, f)
    logger.info("Risk model saved to %s", _MODEL_PATH)

    return model, accuracy, importances


def _load_from_disk() -> tuple | None:
    """Load model from pkl if it exists. Returns None if not found."""
    if not _MODEL_PATH.exists():
        return None
    try:
        with open(_MODEL_PATH, "rb") as f:
            obj = pickle.load(f)
        return obj["model"], obj["accuracy"], obj["importances"]
    except Exception as e:
        logger.warning("Failed to load risk model from disk: %s — will retrain.", e)
        return None


# ── public API ────────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Loading risk model...")
def get_trained_model():
    """
    Load model from disk if available, otherwise train once and save.
    Returns (model, cv_accuracy, feature_importances).
    """
    cached = _load_from_disk()
    if cached is not None:
        return cached
    return _train_and_save()


def predict_risk(
    age: int,
    annual_income: int,
    dependents: int,
    marital_status: str,
    investment_horizon: int,
    loss_tolerance_pct: int,
    investment_experience: int,
    employment_type: str,
) -> dict:
    model, _, _ = get_trained_model()

    row = pd.DataFrame([{
        "age": age,
        "annual_income": annual_income,
        "dependents": dependents,
        "marital_status": marital_status,
        "investment_horizon": investment_horizon,
        "loss_tolerance_pct": loss_tolerance_pct,
        "investment_experience": investment_experience,
        "employment_type": employment_type,
    }])
    X = _prepare_features(row)

    pred = int(model.predict(X)[0])
    proba = model.predict_proba(X)[0]
    classes = model.classes_

    return {
        "risk_profile": pred,
        "label": RISK_LABELS[pred],
        "confidence": float(proba.max()),
        "probabilities": {int(c): float(p) for c, p in zip(classes, proba)},
    }


def recommend_funds(risk_profile: int) -> list[dict]:
    suitable = []
    for fid, info in FUND_PROFILES.items():
        lo, hi = info["risk_range"]
        if lo <= risk_profile <= hi:
            suitable.append({"id": fid, **info})
    if not suitable:
        closest = min(
            FUND_PROFILES.items(),
            key=lambda kv: min(abs(kv[1]["risk_range"][0] - risk_profile),
                               abs(kv[1]["risk_range"][1] - risk_profile)),
        )
        suitable.append({"id": closest[0], **closest[1]})
    return suitable
