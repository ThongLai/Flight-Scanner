import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CollectionReport:
    """Tracks collection results for alerting."""

    calendar_expected: int = 6
    itinerary_expected: int = 12
    calendar_success: int = 0
    itinerary_success: int = 0
    itinerary_empty: int = 0
    retries_used: int = 0
    api_calls: int = 0
    errors: list = field(default_factory=list)

    def log_calendar_success(self, route: str, rows: int):
        self.calendar_success += 1

    def log_itinerary_success(self, route: str, date: str, rows: int):
        self.itinerary_success += 1

    def log_itinerary_empty(self, route: str, date: str):
        self.itinerary_empty += 1
        self.errors.append(f"Empty: {route} ({date})")

    def log_retry(self):
        self.retries_used += 1

    def log_error(self, msg: str):
        self.errors.append(msg)

    @property
    def is_healthy(self) -> bool:
        """Collection is healthy if most searches succeeded."""
        cal_ok = self.calendar_success >= self.calendar_expected
        itin_ok = self.itinerary_success >= (self.itinerary_expected * 0.75)
        return cal_ok and itin_ok

    def summary(self) -> str:
        lines = [
            "=== Collection Report ===",
            f"Calendars:    {self.calendar_success}/{self.calendar_expected}",
            f"Itineraries:  {self.itinerary_success}/{self.itinerary_expected}",
            f"Empty results:{self.itinerary_empty}",
            f"Retries used: {self.retries_used}",
            f"Status:       {'HEALTHY' if self.is_healthy else 'DEGRADED'}",
        ]
        if self.errors:
            lines.append(f"Issues:")
            for e in self.errors:
                lines.append(f"  - {e}")
        return "\n".join(lines)

    def emit_github_annotations(self):
        """Emit GitHub Actions annotations for visibility."""
        if self.is_healthy:
            print(
                f"::notice::Collection healthy: "
                f"{self.calendar_success}/{self.calendar_expected} calendars, "
                f"{self.itinerary_success}/{self.itinerary_expected} itineraries"
            )
        else:
            print(
                f"::warning::Collection degraded: "
                f"{self.calendar_success}/{self.calendar_expected} calendars, "
                f"{self.itinerary_success}/{self.itinerary_expected} itineraries"
            )
        for e in self.errors:
            print(f"::warning::{e}")

    def check_and_alert(self):
        """Log summary and emit GitHub annotations."""
        logger.info(self.summary())
        self.emit_github_annotations()

        if not self.is_healthy:
            logger.warning("Collection health check FAILED")
            return False
        return True
