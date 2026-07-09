"""Shared configuration and paths for the Flight Scanner project."""

from pathlib import Path

# Project paths (single source of truth)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
COLLECTED_DIR = DATA_DIR / "collected"
CALENDAR_DIR = COLLECTED_DIR / "calendars"
ITINERARY_DIR = COLLECTED_DIR / "itineraries"
BTS_DIR = DATA_DIR / "bts"
MODEL_DIR = PROJECT_ROOT / "models"

# Model settings
WAIT_THRESHOLD = 0.45
CURRENCY = "GBP"

# The 12 active routes (codes only)
ROUTE_CODES = [
    "LHR-SGN", "LHR-HAN", "LHR-BKK",
    "MAN-SGN", "MAN-HAN", "MAN-BKK",
    "SGN-LHR", "HAN-LHR", "BKK-LHR",
    "SGN-MAN", "HAN-MAN", "BKK-MAN",
]
