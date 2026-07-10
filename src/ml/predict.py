"""Prediction service: wraps price and wait models for one flight query."""

from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = PROJECT_ROOT / "models"
CAL_DIR = PROJECT_ROOT / "data" / "collected" / "calendars"

_price_model = joblib.load(MODEL_DIR / "price_regressor.joblib")
_wait_model = joblib.load(MODEL_DIR / "wait_classifier.joblib")
_meta = joblib.load(MODEL_DIR / "metadata.joblib")
_route_avg = joblib.load(MODEL_DIR / "route_averages.joblib")


def _load_recent_history(route: str, departure_date: str) -> pd.DataFrame:
    """Load recent calendar observations for one route + departure date."""
    files = sorted(CAL_DIR.glob("calendars_*.parquet"))[-30:]
    if not files:
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    df["route"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
    df["departure_date"] = pd.to_datetime(df["departure_date"])

    mask = (df["route"] == route) & (
        df["departure_date"] == pd.to_datetime(departure_date)
    )
    return df[mask].sort_values("collected_at")


def predict_flight(route: str, departure_date: str) -> dict:
    """Predict price and book/wait recommendation for a flight."""
    hist = _load_recent_history(route, departure_date)
    dep = pd.to_datetime(departure_date)
    today = pd.Timestamp(datetime.now(timezone.utc).date())
    days_ahead = max((dep - today).days, 0)

    if len(hist) > 0:
        recent = hist["price"].tail(7)
        price_lag1 = recent.iloc[-1]
        price_lag7 = recent.iloc[0]
        price_roll3 = recent.tail(3).mean()
    else:
        fallback = _route_avg.get(route, 400.0)
        price_lag1 = price_lag7 = price_roll3 = fallback

    last_price = price_lag1

    row = pd.DataFrame([{
        "days_ahead": days_ahead,
        "dep_dow": dep.dayofweek,
        "dep_month": dep.month,
        "col_dow": today.dayofweek,
        "route": route,
        "price_lag1": price_lag1,
        "price_lag7": price_lag7,
        "price_roll3": price_roll3,
    }])
    row["route"] = row["route"].astype("category")

    est_price = float(_price_model.predict(row[_meta["price_features"]])[0])

    clf_row = row.copy()
    clf_row["price"] = last_price
    clf_row["trend"] = price_lag1 - price_lag7
    proba = float(
        _wait_model.predict_proba(clf_row[_meta["clf_features"]])[0, 1]
    )

    action = (
        "Price predicted to drop"
        if proba >= _meta["wait_threshold"]
        else "Price predicted to increase"
    )
    dist = abs(proba - _meta["wait_threshold"])
    confidence = "High" if dist > 0.25 else "Medium" if dist > 0.10 else "Low"

    return {
        "route": route,
        "departure_date": departure_date,
        "days_ahead": days_ahead,
        "estimated_price": round(est_price, 2),
        "last_observed_price": round(last_price, 2),
        "action": action,
        "confidence": confidence,
        "wait_probability": round(proba, 4),
        "wait_threshold": _meta["wait_threshold"],
        "confidence_distance": round(dist, 4),
        "has_history": len(hist) > 0,
    }
