"""Feature engineering for collected calendar data."""

import pandas as pd


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add temporal and route features for modelling."""
    df = df.copy()
    df["days_ahead"] = df["days_ahead"].clip(0, 365)
    df["dep_dow"] = df["departure_date"].dt.dayofweek
    df["dep_month"] = df["departure_date"].dt.month
    df["col_dow"] = df["collected_date"].dt.dayofweek
    df["route"] = df["origin"].astype(str) + "-" + df["destination"].astype(str)
    return df


def add_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add previous-price features per route + departure_date series."""
    df = df.sort_values(["route", "departure_date", "collected_date"]).copy()
    grp = df.groupby(["route", "departure_date"], observed=True)["price"]

    df["price_lag1"] = grp.shift(1)
    df["price_lag7"] = grp.shift(7)
    df["price_roll3"] = (
        grp.shift(1)
        .groupby([df["route"], df["departure_date"]])
        .rolling(3, min_periods=1).mean()
        .reset_index(level=[0, 1], drop=True)
    )
    df["price_change"] = df["price"] - df["price_lag1"]

    route_mean = df.groupby("route", observed=True)["price"].transform("mean")
    for col in ["price_lag1", "price_lag7", "price_roll3"]:
        df[col] = df[col].fillna(route_mean)
    df["price_change"] = df["price_change"].fillna(0)
    return df
