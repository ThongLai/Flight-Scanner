import os

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.environ["DATABASE_URL"]


def _conn():
    """Open a new database connection."""
    return psycopg2.connect(DATABASE_URL)


def init_db() -> None:
    """Create the tracks table if it does not exist."""
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                origin TEXT NOT NULL,
                destination TEXT NOT NULL,
                departure_date DATE NOT NULL,
                baseline_price REAL NOT NULL,
                last_notified_price REAL,
                active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT now(),
                UNIQUE (user_id, origin, destination, departure_date)
            )
        """)


def add_track(user_id: int, origin: str, dest: str,
              date: str, baseline: float) -> None:
    """Add or reactivate a track for a user's flight."""
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            INSERT INTO tracks
                (user_id, origin, destination, departure_date, baseline_price)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (user_id, origin, destination, departure_date)
            DO UPDATE SET baseline_price = EXCLUDED.baseline_price,
                          active = TRUE,
                          last_notified_price = NULL
        """, (user_id, origin, dest, date, baseline))


def get_user_tracks(user_id: int) -> list[dict]:
    """Return all active tracks for one user."""
    with _conn() as c, \
         c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM tracks WHERE user_id=%s AND active=TRUE "
            "ORDER BY departure_date",
            (user_id,))
        return [dict(r) for r in cur.fetchall()]


def get_active_tracks() -> list[dict]:
    """Return every active track across all users (for the checker)."""
    with _conn() as c, \
         c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM tracks WHERE active=TRUE")
        return [dict(r) for r in cur.fetchall()]


def deactivate_track(user_id: int, origin: str,
                     dest: str, date: str) -> None:
    """Deactivate a specific track."""
    with _conn() as c, c.cursor() as cur:
        cur.execute("""
            UPDATE tracks SET active=FALSE
            WHERE user_id=%s AND origin=%s AND destination=%s
                  AND departure_date=%s
        """, (user_id, origin, dest, date))


def update_notified_price(track_id: int, price: float) -> None:
    """Record the price at which a user was last alerted."""
    with _conn() as c, c.cursor() as cur:
        cur.execute(
            "UPDATE tracks SET last_notified_price=%s WHERE id=%s",
            (price, track_id))
