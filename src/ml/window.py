"""Window search: find cheapest departure dates in a date range."""

from datetime import date

import pandas as pd

from data_collection.flight_scraper import FlightScraper
from ml.predict import predict_flight

_scraper = FlightScraper()


def find_best_dates(
    origin: str,
    destination: str,
    start: str,
    end: str,
    top_n: int = 5,
) -> pd.DataFrame:
    """Return cheapest departure dates in a range with book/wait advice."""
    today = date.today().strftime("%Y-%m-%d")

    # One call returns the full forward calendar
    response = _scraper.price_calendar(origin, destination, today)
    records = _scraper.parse_price_calendar(response, origin, destination)
    df = pd.DataFrame(records)

    if df.empty:
        return df

    # Filter to the requested window and rank by price
    df["departure_date"] = pd.to_datetime(df["departure_date"])
    mask = df["departure_date"].between(
        pd.to_datetime(start), pd.to_datetime(end)
    )
    df = df[mask].sort_values("price").head(top_n).copy()

    # Add model verdict per date
    route = f"{origin}-{destination}"
    verdicts = [
        predict_flight(route, d.strftime("%Y-%m-%d"))
        for d in df["departure_date"]
    ]
    df["action"] = [v["action"] for v in verdicts]
    df["confidence"] = [v["confidence"] for v in verdicts]

    return df[["departure_date", "price", "action", "confidence"]]
