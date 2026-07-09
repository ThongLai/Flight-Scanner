"""Load and combine daily collected data into single datasets."""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COLLECTED = PROJECT_ROOT / "data" / "collected"


def load_all_calendars() -> pd.DataFrame:
    """Load and combine every daily calendar Parquet file."""
    files = sorted((COLLECTED / "calendars").glob("*.parquet"))
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

    df["collected_at"] = pd.to_datetime(df["collected_at"])
    df["departure_date"] = pd.to_datetime(df["departure_date"])
    df["collected_date"] = df["collected_at"].dt.normalize()
    df["days_ahead"] = (df["departure_date"] - df["collected_date"]).dt.days

    return df[df["days_ahead"] >= 0]


def load_all_itineraries(columns: list[str] | None = None) -> pd.DataFrame:
    """Load and combine every daily itinerary Parquet file."""
    files = sorted((COLLECTED / "itineraries").glob("*.parquet"))
    frames = [pd.read_parquet(f, columns=columns) for f in files]
    return pd.concat(frames, ignore_index=True)
