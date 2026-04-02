import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from flight_scraper import FlightScraper
from routes import TARGET_ROUTES, COLLECTION_CONFIG

logger = logging.getLogger(__name__)

# DATA_DIR = Path("data/collected")
# CALENDAR_DIR = DATA_DIR / "calendars"
# ITINERARY_DIR = DATA_DIR / "itineraries"

# At the top of daily_collector.py, replace the DATA_DIR lines with:
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "collected"
CALENDAR_DIR = DATA_DIR / "calendars"
ITINERARY_DIR = DATA_DIR / "itineraries"


class DailyCollector:
    """Collects flight prices for all target routes."""

    def __init__(self, scraper: Optional[FlightScraper] = None):
        self.scraper = scraper or FlightScraper()
        CALENDAR_DIR.mkdir(parents=True, exist_ok=True)
        ITINERARY_DIR.mkdir(parents=True, exist_ok=True)

    def collect_route_calendar(self, origin: str, destination: str) -> pd.DataFrame:
        """Collect price calendar for a single route."""
        tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
        response = self.scraper.price_calendar(
            origin, destination, tomorrow,
            currency=COLLECTION_CONFIG["currency"],
        )
        records = self.scraper.parse_price_calendar(response, origin, destination)
        return pd.DataFrame(records)

    def collect_route_itineraries(
        self, origin: str, destination: str, depart_date: str
    ) -> pd.DataFrame:
        """Collect full itineraries for a single route and date."""
        response = self.scraper.search_one_way(
            origin, destination, depart_date,
            currency=COLLECTION_CONFIG["currency"],
            max_polls=COLLECTION_CONFIG["max_polls"],
            poll_interval=COLLECTION_CONFIG["poll_interval"],
        )
        records = self.scraper.parse_itineraries(response)
        df = pd.DataFrame(records)
        if not df.empty:
            df["collected_at"] = datetime.utcnow().isoformat()
            df["search_date"] = depart_date
        return df

    def _save_parquet(self, df: pd.DataFrame, directory: Path, prefix: str):
        """Append dataframe to daily Parquet file."""
        if df.empty:
            return

        today = date.today().strftime("%Y-%m-%d")
        path = directory / f"{prefix}_{today}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)

        df.to_parquet(path, index=False)
        logger.info(f"Saved {len(df)} rows to {path}")

    def run(self):
        """Execute daily collection for all routes."""
        start = datetime.utcnow()
        logger.info(f"Daily collection started at {start.isoformat()}")

        calendar_frames = []
        itinerary_frames = []

        # Target date for detailed search: 8 weeks from now
        search_date = (date.today() + timedelta(weeks=8)).strftime("%Y-%m-%d")

        for route in TARGET_ROUTES:
            origin = route["origin"]
            dest = route["destination"]
            route_label = f"{origin}-{dest}"

            # Price calendar
            try:
                cal_df = self.collect_route_calendar(origin, dest)
                calendar_frames.append(cal_df)
                logger.info(f"Calendar {route_label}: {len(cal_df)} days")
            except Exception as e:
                logger.error(f"Calendar {route_label} failed: {e}")

            # Detailed itineraries
            try:
                itin_df = self.collect_route_itineraries(origin, dest, search_date)
                itinerary_frames.append(itin_df)
                logger.info(f"Itineraries {route_label}: {len(itin_df)} results")
            except Exception as e:
                logger.error(f"Itineraries {route_label} failed: {e}")

        # Save collected data
        if calendar_frames:
            all_cal = pd.concat(calendar_frames, ignore_index=True)
            self._save_parquet(all_cal, CALENDAR_DIR, "calendars")

        if itinerary_frames:
            all_itin = pd.concat(itinerary_frames, ignore_index=True)
            self._save_parquet(all_itin, ITINERARY_DIR, "itineraries")

        elapsed = (datetime.utcnow() - start).total_seconds()
        quota = self.scraper.get_quota_status()
        logger.info(
            f"Collection complete in {elapsed:.0f}s. "
            f"API remaining: {quota['api_remaining']}, "
            f"daily used: {quota['daily_used']}"
        )
