"""NSGA-II multi-objective flight optimisation with live fallback."""

import pandas as pd
from pymoo.util.nds.non_dominated_sorting import NonDominatedSorting

from data_collection.flight_scraper import FlightScraper
from ml.data_loader import load_all_itineraries

_scraper = None


def _get_scraper() -> FlightScraper:
    global _scraper
    if _scraper is None:
        _scraper = FlightScraper()
    return _scraper


def _skyscanner_link(origin: str, destination: str, search_date: str) -> str:
    """Build a one-way Skyscanner search link (YYMMDD date format)."""
    d = search_date.replace("-", "")[2:]   # 2026-09-04 -> 260904
    return (
        f"https://www.skyscanner.net/transport/flights/"
        f"{origin.lower()}/{destination.lower()}/{d}/"
        f"?adultsv2=1&cabinclass=economy"
    )


def _from_collected(origin, destination, search_date) -> pd.DataFrame:
    """Try to load options from collected itinerary data."""
    cols = ["collected_at", "search_date", "origin_code", "dest_code",
            "price_raw", "duration_minutes", "stop_count",
            "carrier_names", "segment_route", "is_direct"]
    df = load_all_itineraries(columns=cols)
    df["search_date"] = df["search_date"].astype(str).str[:10]

    mask = ((df["origin_code"] == origin)
            & (df["dest_code"] == destination)
            & (df["search_date"] == search_date))
    df = df[mask].copy()
    if df.empty:
        return df

    latest = df["collected_at"].max()
    return df[df["collected_at"] == latest].reset_index(drop=True)


def _from_live(origin, destination, search_date) -> pd.DataFrame:
    """Fetch options live from the API for any date."""
    scraper = _get_scraper()
    response = scraper.search_one_way(origin, destination, search_date)
    records = scraper.parse_itineraries(response)
    return pd.DataFrame(records)


def find_pareto_flights(origin, destination, search_date, top_n=5):
    """Return Pareto-optimal flights for any date.

    Uses collected data if available, otherwise fetches live.
    """
    df = _from_collected(origin, destination, search_date)
    source = "collected"

    if df.empty:
        df = _from_live(origin, destination, search_date)
        source = "live"

    if df.empty:
        return df

    # Filter outliers for sensible economy options
    df = df[
        (df["price_raw"] <= df["price_raw"].quantile(0.95))
        & (df["duration_minutes"] <= df["duration_minutes"].quantile(0.95))
        & (df["stop_count"] <= 2)
    ].reset_index(drop=True)

    if df.empty:
        return df

    objectives = df[
        ["price_raw", "duration_minutes", "stop_count"]
    ].values.astype(float)

    fronts = NonDominatedSorting().do(objectives)
    pareto = df.iloc[fronts[0]].sort_values("price_raw").head(top_n).copy()

    pareto["duration_hrs"] = (pareto["duration_minutes"] / 60).round(1)
    result = pareto[["price_raw", "duration_hrs", "stop_count",
                     "carrier_names", "segment_route"]]
    result.attrs["source"] = source
    return result
