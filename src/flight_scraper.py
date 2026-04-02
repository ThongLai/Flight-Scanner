import os
import time
import logging
from datetime import date
from typing import Optional

import requests

logger = logging.getLogger(__name__)


ITINERARY_FIELDS = {
    "price": {
        "itinerary_id": ("id",),
        "price_raw": ("price", "raw"),
        "price_formatted": ("price", "formatted"),
        "score": ("score",),
    },
    "fare_policy": {
        "is_change_allowed": ("farePolicy", "isChangeAllowed"),
        "is_cancel_allowed": ("farePolicy", "isCancellationAllowed"),
        "is_self_transfer": ("isSelfTransfer",),
        "tags": ("tags",),
    },
    "leg": {
        "origin_code": ("origin", "displayCode"),
        "origin_city": ("origin", "city"),
        "dest_code": ("destination", "displayCode"),
        "dest_city": ("destination", "city"),
        "departure": ("departure",),
        "arrival": ("arrival",),
        "duration_minutes": ("durationInMinutes",),
        "time_delta_days": ("timeDeltaInDays",),
        "stop_count": ("stopCount",),
    },
    "segment": {
        "origin": ("origin", "displayCode"),
        "dest": ("destination", "displayCode"),
        "flight_number": ("flightNumber",),
        "duration": ("durationInMinutes",),
        "departure": ("departure",),
        "arrival": ("arrival",),
        "marketing_carrier": ("marketingCarrier", "name"),
        "marketing_code": ("marketingCarrier", "alternateId"),
        "operating_carrier": ("operatingCarrier", "name"),
        "operating_code": ("operatingCarrier", "alternateId"),
    },
}

MAX_SEGMENTS = 4



class FlightScraper:
    """Client for the Flights Scraper Sky API."""

    def __init__(self, api_key: Optional[str] = None, host: Optional[str] = None):
        self.api_key = api_key or os.getenv("RAPIDAPI_KEY")
        if not self.api_key:
            raise ValueError("RAPIDAPI_KEY not found in environment or arguments")

        self.host = host or os.getenv("RAPIDAPI_HOST", "flights-sky.p.rapidapi.com")
        self.base_url = f"https://{self.host}"
        self.headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": self.host,
        }

        self.requests_remaining = None
        self._last_request_time = 0.0
        self._min_interval = 0.35
        self._daily_count = 0
        self._daily_limit = 500
        self._count_date = None

    def _check_budget(self):
        from datetime import date
        today = date.today()
        if today != self._count_date:
            self._daily_count = 0
            self._count_date = today
        if self._daily_count >= self._daily_limit:
            raise RuntimeError(
                f"Daily budget exceeded ({self._daily_limit})"
            )

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)

    def _get(self, endpoint: str, params: dict, max_retries: int = 3) -> dict:
        """Make a GET request with rate limiting and exponential backoff."""
        import random

        self._check_budget()
        self._throttle()

        retryable_codes = {429, 500, 502, 503, 504}

        for attempt in range(max_retries):
            try:
                resp = requests.get(
                    f"{self.base_url}/{endpoint}",
                    headers=self.headers,
                    params=params,
                    timeout=30,
                )
                self._last_request_time = time.time()
                self._daily_count += 1

                self.requests_remaining = resp.headers.get(
                    "X-RateLimit-Requests-Remaining"
                )

                if resp.status_code in retryable_codes:
                    base_wait = 2 ** attempt
                    jitter = random.uniform(0, base_wait * 0.5)
                    wait = base_wait + jitter
                    logger.warning(
                        f"{endpoint} -> {resp.status_code}. "
                        f"Retry {attempt + 1}/{max_retries} in {wait:.1f}s"
                    )
                    time.sleep(wait)
                    continue

                logger.info(
                    f"{endpoint} -> {resp.status_code} "
                    f"(remaining: {self.requests_remaining}, "
                    f"daily: {self._daily_count}/{self._daily_limit})"
                )

                resp.raise_for_status()
                return resp.json()

            except requests.exceptions.Timeout:
                wait = 2 ** attempt + random.uniform(0, 1)
                logger.warning(
                    f"{endpoint} -> Timeout. "
                    f"Retry {attempt + 1}/{max_retries} in {wait:.1f}s"
                )
                time.sleep(wait)

            except requests.exceptions.ConnectionError:
                wait = 2 ** attempt + random.uniform(0, 1)
                logger.warning(
                    f"{endpoint} -> Connection error. "
                    f"Retry {attempt + 1}/{max_retries} in {wait:.1f}s"
                )
                time.sleep(wait)

        raise RuntimeError(
            f"Failed {endpoint} after {max_retries} retries"
        )




    # ----- Search Endpoints -----
    def search_one_way(
        self,
        origin: str,
        destination: str,
        depart_date: str,
        currency: str = "GBP",
        max_polls: int = 3,
        poll_interval: float = 2.0,
    ) -> dict:
        """Search one-way flights with polling via search-incomplete."""
        params = {
            "fromEntityId": origin,
            "toEntityId": destination,
            "departDate": depart_date,
            "currency": currency,
        }

        data = self._get("flights/search-one-way", params)

        polls = 0
        while (
            data.get("data", {}).get("context", {}).get("status") == "incomplete"
            and polls < max_polls
        ):
            session_id = data["data"]["context"]["sessionId"]
            time.sleep(poll_interval)
            data = self._get(
                "flights/search-incomplete",
                {"sessionId": session_id},
            )
            polls += 1

        return data

    def search_return(
        self,
        origin: str,
        destination: str,
        depart_date: str,
        return_date: str,
        currency: str = "GBP",
        max_polls: int = 3,
        poll_interval: float = 2.0,
    ) -> dict:
        """Search return flights with polling via search-incomplete."""
        params = {
            "fromEntityId": origin,
            "toEntityId": destination,
            "departDate": depart_date,
            "returnDate": return_date,
            "currency": currency,
        }

        data = self._get("flights/search-roundtrip", params)

        polls = 0
        while (
            data.get("data", {}).get("context", {}).get("status") == "incomplete"
            and polls < max_polls
        ):
            session_id = data["data"]["context"]["sessionId"]
            time.sleep(poll_interval)
            data = self._get(
                "flights/search-incomplete",
                {"sessionId": session_id},
            )
            polls += 1

        return data




    # ----- Price Calendar (High Value for Collection) -----
    def price_calendar(
        self,
        origin: str,
        destination: str,
        depart_date: str,
        currency: str = "GBP",
    ) -> dict:
        """Get price calendar for a route starting from depart_date."""
        return self._get("flights/price-calendar", {
            "fromEntityId": origin,
            "toEntityId": destination,
            "departDate": depart_date,
            "currency": currency,
        })

    def price_calendar_return(
        self,
        origin: str,
        destination: str,
        depart_date: str,
        return_date: str,
        currency: str = "GBP",
    ) -> dict:
        """Get return price calendar for a route."""
        return self._get("flights/price-calendar-return", {
            "fromEntityId": origin,
            "toEntityId": destination,
            "departDate": depart_date,
            "returnDate": return_date,
            "currency": currency,
        })
        
    def parse_price_calendar(
        self,
        response: dict,
        origin: str = "",
        destination: str = "",
        currency: str = "GBP",
    ) -> list[dict]:
        """Extract daily prices from price calendar response."""
        from datetime import datetime

        flights = response.get("data", {}).get("flights", {})
        days = flights.get("days", [])
        collected_at = datetime.utcnow().isoformat()

        return [
            {
                "collected_at": collected_at,
                "origin": origin,
                "destination": destination,
                "currency": currency,
                "departure_date": day.get("day"),
                "price": day.get("price"),
                "group": day.get("group"),
            }
            for day in days
            if day.get("price") is not None
        ]




    # ----- Cheapest Flights -----
    def cheapest_one_way(
        self,
        origin: str,
        destination: str,
        currency: str = "GBP",
    ) -> dict:
        """Get cheapest one-way fares."""
        return self._get("flights/cheapest-one-way", {
            "fromEntityId": origin,
            "toEntityId": destination,
            "currency": currency,
        })




    # ----- Reference Data -----
    def get_airports(self) -> dict:
        """Get all airports with entity IDs."""
        return self._get("flights/airports", {})

    def auto_complete(self, query: str) -> dict:
        """Search for airports/cities by name."""
        return self._get("flights/auto-complete", {"query": query})




    # ----- Parser -----
    def _extract(self, data: dict, path: tuple):
        for key in path:
            if not isinstance(data, dict):
                return None
            data = data.get(key)
        return data

    def parse_itineraries(self, response: dict) -> list[dict]:
        """Extract flat itinerary records from API response."""
        itineraries = response.get("data", {}).get("itineraries", [])
        fields = ITINERARY_FIELDS
        records = []

        for it in itineraries:
            for leg in it.get("legs", []):
                marketing = leg.get("carriers", {}).get("marketing", [])
                segments = leg.get("segments", [])

                record = {}

                for col, path in fields["price"].items():
                    record[col] = self._extract(it, path)

                for col, path in fields["fare_policy"].items():
                    val = self._extract(it, path)
                    if isinstance(val, list):
                        val = ", ".join(val)
                    record[col] = val

                for col, path in fields["leg"].items():
                    record[col] = self._extract(leg, path)

                record["is_direct"] = record.get("stop_count", 0) == 0
                record["num_segments"] = len(segments)
                record["carrier_names"] = ", ".join(
                    c.get("name", "") for c in marketing
                )
                record["carrier_codes"] = ", ".join(
                    c.get("alternateId", "") for c in marketing
                )
                record["operation_type"] = leg.get("carriers", {}).get(
                    "operationType", ""
                )

                if segments:
                    record["segment_route"] = " -> ".join(
                        [segments[0]["origin"]["displayCode"]]
                        + [s["destination"]["displayCode"] for s in segments]
                    )
                else:
                    record["segment_route"] = ""

                for i, seg in enumerate(segments[:MAX_SEGMENTS]):
                    for col, path in fields["segment"].items():
                        record[f"seg{i}_{col}"] = self._extract(seg, path)
                    record[f"seg{i}_flight"] = (
                        self._extract(seg, ("marketingCarrier", "alternateId")) or ""
                    ) + (seg.get("flightNumber") or "")

                records.append(record)

        return records

    def get_quota_status(self) -> dict:
        return {
            "api_remaining": self.requests_remaining,
            "daily_used": self._daily_count,
            "daily_limit": self._daily_limit,
        }
