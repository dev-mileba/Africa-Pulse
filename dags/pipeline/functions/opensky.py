"""OpenSky flights ingestion: daily arrivals and departures per airport.

Each (airport, direction, UTC day) is one request, well inside the API's
2-day window limit. Responses are turned straight into a DataFrame and
written to CSV. Re-running the same window overwrites that day's CSV, so
records are never multiplied.
"""

import logging
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from token_manager import TokenManager

log = logging.getLogger(__name__)

FLIGHTS_URL = "https://opensky-network.org/api/flights"
DIRECTIONS = ("arrival", "departure")

MAX_ATTEMPTS = 5
# A longer Retry-After than this means the daily credits are gone.
# Stop and report it instead of sleeping for hours.
MAX_WAIT_SECONDS = 300
PAUSE_BETWEEN_CALLS = 1.0

# A flight appearing twice (overlapping windows, re-runs) is the same
# observation when all of these match.
DEDUP_KEY = ["queried_airport", "direction", "icao24", "first_seen", "last_seen"]


class RateLimitExhausted(Exception):
    """Daily API credits are used up; retrying now would not help."""


def _get(tokens, url, params):
    """GET with backoff. Returns a 200 or 404 response, raises otherwise."""
    refreshed = False
    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = 2**attempt
        try:
            r = requests.get(url, params=params, headers=tokens.headers(), timeout=30)
        except requests.RequestException as e:
            log.warning("request error (%s), attempt %d/%d", e, attempt, MAX_ATTEMPTS)
            time.sleep(wait)
            continue

        if r.status_code in (200, 404):
            return r
        if r.status_code == 401 and not refreshed:
            log.warning("401 from OpenSky, refreshing token once")
            tokens.token = None  # forces TokenManager to fetch a new one
            refreshed = True
            continue
        if r.status_code == 429:
            try:
                wait = int(r.headers["X-Rate-Limit-Retry-After-Seconds"])
            except (KeyError, ValueError):
                pass
            if wait > MAX_WAIT_SECONDS:
                raise RateLimitExhausted(f"429, retry after {wait}s (credits used up)")
            log.warning("429 rate limited, waiting %ds", wait)
        elif r.status_code >= 500:
            log.warning("HTTP %d, retrying in %ds", r.status_code, wait)
        else:
            r.raise_for_status()
        time.sleep(wait)
    raise RuntimeError(f"gave up after {MAX_ATTEMPTS} attempts: {url} {params}")


def _day_window(day):
    begin = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return int(begin.timestamp()), int((begin + timedelta(days=1)).timestamp())


def _fetch_window(tokens, city, direction, day, fetched_at, batch_id):
    """Fetch one window. Returns (rows, number of rejected records)."""
    airport = city["airport_icao"]
    begin, end = _day_window(day)
    params = {"airport": airport, "begin": begin, "end": end}
    r = _get(tokens, f"{FLIGHTS_URL}/{direction}", params)
    # 404 means "no flights in this window", not a failure
    flights = r.json() if r.status_code == 200 else []

    rows = []
    rejected = 0
    for f in flights:
        icao24, first, last = f.get("icao24"), f.get("firstSeen"), f.get("lastSeen")
        if not icao24 or first is None or last is None:
            rejected += 1
            continue
        departure = f.get("estDepartureAirport")
        arrival = f.get("estArrivalAirport")
        rows.append(
            {
                "city": city["city"],
                "country": city["country"],
                "queried_airport": airport,
                "direction": direction,
                "icao24": icao24,
                "callsign": (f.get("callsign") or "").strip() or None,
                "first_seen": pd.to_datetime(first, unit="s", utc=True),
                "last_seen": pd.to_datetime(last, unit="s", utc=True),
                "est_departure_airport": departure,
                "est_arrival_airport": arrival,
                # the airport at the other end of the flight
                "other_airport": departure if direction == "arrival" else arrival,
                "fetched_at": fetched_at,
                "batch_id": batch_id,
            }
        )
    return rows, rejected


def ingest_flights(cities, start_day, end_day):
    """Fetch arrivals and departures for every city and UTC day in the range.

    Returns a deduplicated DataFrame. Only days up to yesterday (UTC) are
    requested, because OpenSky publishes flight data with a delay.
    """
    batch_id = str(uuid.uuid4())
    last_available = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end_day > last_available:
        log.info(
            "capping end_day %s to %s (data lags by a day)", end_day, last_available
        )
        end_day = last_available

    tokens = TokenManager()
    rows = []
    fetched = failed = rejected = 0
    day = start_day
    try:
        while day <= end_day:
            for city in cities:
                for direction in DIRECTIONS:
                    airport = city["airport_icao"]
                    try:
                        window_rows, window_rejected = _fetch_window(
                            tokens,
                            city,
                            direction,
                            day,
                            datetime.now(timezone.utc).isoformat(),
                            batch_id,
                        )
                        rows.extend(window_rows)
                        rejected += window_rejected
                        log.info(
                            "%s %s %s: %d flights",
                            airport,
                            direction,
                            day,
                            len(window_rows),
                        )
                        fetched += 1
                    except RateLimitExhausted:
                        raise
                    except Exception:
                        # One bad window must not stop the rest
                        log.exception("failed %s %s %s", airport, direction, day)
                        failed += 1
                    time.sleep(PAUSE_BETWEEN_CALLS)
            day += timedelta(days=1)
    except RateLimitExhausted as e:
        log.error("stopping early: %s. Results below are partial.", e)

    if rejected:
        log.warning(
            "%d flight records missing icao24/firstSeen/lastSeen were skipped", rejected
        )
    log.info("batch %s: fetched=%d failed=%d", batch_id, fetched, failed)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.drop_duplicates(DEDUP_KEY).sort_values(
        ["city", "first_seen"], ignore_index=True
    )


def export_movements_csv(movements, path=f"data/opensky_mobility_{date.today()}.csv"):
    """Write the movements DataFrame to CSV."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    movements.to_csv(path, index=False)
    log.info("wrote %d movements to %s", len(movements), path)


def summarize(movements):
    """Per city and direction: movement count and how often the other airport is unknown."""
    if movements.empty:
        return "no movements"
    return movements.groupby(["city", "direction"]).agg(
        movements=("icao24", "size"),
        other_airport_missing_pct=(
            "other_airport",
            lambda s: round(s.isna().mean() * 100, 1),
        ),
    )
