"""Daily flight price collection pipeline."""

import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd

from flight_scraper import FlightScraper
from routes import TARGET_ROUTES, COLLECTION_CONFIG

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "collected"
CALENDAR_DIR = DATA_DIR / "calendars"
ITINERARY_DIR = DATA_DIR / "itineraries"

# SEARCH_OFFSETS_WEEKS = [2, 4, 8, 12]
SEARCH_OFFSETS_WEEKS = [3, 8]
DELAY_BETWEEN_SEARCHES = 10
DELAY_BETWEEN_ROUTES = 60
RETRY_DELAY = 60
MAX_RETRIES = 5


ITINERARY_KEEP_COLS = [
    "collected_at",
    "search_date",
    "itinerary_id",
    "price_raw",
    "score",
    "origin_code",
    "origin_city",
    "dest_code",
    "dest_city",
    "departure",
    "arrival",
    "duration_minutes",
    "time_delta_days",
    "stop_count",
    "is_direct",
    "is_self_transfer",
    "num_segments",
    "carrier_names",
    "carrier_codes",
    "segment_route",
    "operation_type",
    "tags",
]


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
            keep = [c for c in ITINERARY_KEEP_COLS if c in df.columns]
            df = df[keep]
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

    def run(self, force: bool = False):
        today = date.today()
        cal_path = CALENDAR_DIR / f"calendars_{today.strftime('%Y-%m-%d')}.parquet"
        if cal_path.exists() and not force:
            logger.info(f"Already collected for {today}, skipping")
            return
        if cal_path.exists() and force:
            logger.warning(
                "Force re-run: previous data exists. "
                "Multiple runs per day may trigger API throttling."
            )


        start = datetime.utcnow()
        logger.info(f"Daily collection started at {start.isoformat()}")

        calendar_frames = []
        itinerary_frames = []
        empty_count = 0
        total_count = 0

        search_dates = [
            (today + timedelta(weeks=w)).strftime("%Y-%m-%d")
            for w in SEARCH_OFFSETS_WEEKS
        ]
        logger.info(f"Search dates: {search_dates}")

        for i, route in enumerate(TARGET_ROUTES):
            origin = route["origin"]
            dest = route["destination"]
            route_label = f"{origin}-{dest}"

            if i > 0:
                logger.info(
                    f"Waiting {DELAY_BETWEEN_ROUTES}s before next route..."
                )
                time.sleep(DELAY_BETWEEN_ROUTES)

            # Price calendar
            try:
                cal_df = self.collect_route_calendar(origin, dest)
                calendar_frames.append(cal_df)
                logger.info(f"Calendar {route_label}: {len(cal_df)} days")
            except Exception as e:
                logger.error(f"Calendar {route_label} failed: {e}")

            time.sleep(DELAY_BETWEEN_SEARCHES)

            # Itineraries for each search date
            for j, sd in enumerate(search_dates):
                total_count += 1
                itin_df = pd.DataFrame()

                for attempt in range(1 + MAX_RETRIES):
                    try:
                        itin_df = self.collect_route_itineraries(
                            origin, dest, sd
                        )
                    except Exception as e:
                        logger.error(
                            f"Itineraries {route_label} ({sd}) failed: {e}"
                        )

                    if not itin_df.empty:
                        break

                    if attempt < MAX_RETRIES:
                        logger.warning(
                            f"Itineraries {route_label} ({sd}): 0 results. "
                            f"Retrying in {RETRY_DELAY}s..."
                        )
                        time.sleep(RETRY_DELAY)

                if itin_df.empty:
                    empty_count += 1
                    logger.warning(
                        f"Itineraries {route_label} ({sd}): "
                        f"0 results after {1 + MAX_RETRIES} attempts"
                    )
                else:
                    itinerary_frames.append(itin_df)
                    logger.info(
                        f"Itineraries {route_label} ({sd}): "
                        f"{len(itin_df)} results"
                    )

                if j < len(search_dates) - 1:
                    time.sleep(DELAY_BETWEEN_SEARCHES)


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
            f"Itinerary searches: {total_count - empty_count}/{total_count} "
            f"returned results. "
            f"API remaining: {quota['api_remaining']}, "
            f"daily used: {quota['daily_used']}"
        )
