import hashlib
import json
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
RAW_DIR = Path("data/raw/opensky")
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


def _already_fetched(airport, direction, day):
    folder = RAW_DIR / airport / direction
    return folder.exists() and any(folder.glob(f"{day}_*.json"))


def _fetch_window(tokens, city, direction, day, batch_id):
    """Fetch one window and save the raw response. Returns the HTTP status."""
    airport = city["airport_icao"]
    begin, end = _day_window(day)
    params = {"airport": airport, "begin": begin, "end": end}
    url = f"{FLIGHTS_URL}/{direction}"

    fetched_at = datetime.now(timezone.utc)
    r = _get(tokens, url, params)
    body = r.content
    # 404 means "no flights in this window", not a failure
    payload = r.json() if r.status_code == 200 else []

    record = {
        "meta": {
            "source": "opensky",
            "url": url,
            "params": params,
            "city": city["city"],
            "country": city["country"],
            "airport": airport,
            "direction": direction,
            "day": day.isoformat(),
            "http_status": r.status_code,
            "payload_sha256": hashlib.sha256(body).hexdigest(),
            "batch_id": batch_id,
            "fetched_at": fetched_at.isoformat(),
        },
        "payload": payload,
    }
    folder = RAW_DIR / airport / direction
    folder.mkdir(parents=True, exist_ok=True)
    stamp = fetched_at.strftime("%Y%m%dT%H%M%SZ")
    (folder / f"{day}_{stamp}.json").write_text(json.dumps(record))
    return r.status_code, len(payload)


def ingest_flights(cities, start_day, end_day, force=False):
    """Fetch arrivals and departures for every city and UTC day in the range.

    Only days up to yesterday (UTC) are requested: OpenSky publishes flight
    data with a delay. Days that already have raw evidence are skipped
    unless force=True, which also saves API credits.
    """
    batch_id = str(uuid.uuid4())
    last_available = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end_day > last_available:
        log.info(
            "capping end_day %s to %s (data lags by a day)", end_day, last_available
        )
        end_day = last_available

    tokens = TokenManager()
    fetched = skipped = failed = 0
    day = start_day
    try:
        while day <= end_day:
            for city in cities:
                for direction in DIRECTIONS:
                    airport = city["airport_icao"]
                    if not force and _already_fetched(airport, direction, day):
                        skipped += 1
                        continue
                    try:
                        status, n = _fetch_window(
                            tokens, city, direction, day, batch_id
                        )
                        log.info(
                            "%s %s %s: HTTP %d, %d flights",
                            airport,
                            direction,
                            day,
                            status,
                            n,
                        )
                        fetched += 1
                    except RateLimitExhausted:
                        raise
                    except Exception:
                        # One bad window must not stop the rest, and nothing
                        # is written for it, so history stays untouched.
                        log.exception("failed %s %s %s", airport, direction, day)
                        failed += 1
                    time.sleep(PAUSE_BETWEEN_CALLS)
            day += timedelta(days=1)
    except RateLimitExhausted as e:
        log.error("stopping early: %s. Re-run later; finished days are skipped.", e)

    log.info(
        "batch %s: fetched=%d skipped=%d failed=%d", batch_id, fetched, skipped, failed
    )
    return build_movements()


def build_movements():
    """Build the deduplicated movements table from all saved raw responses."""
    rows = []
    rejected = 0
    for path in sorted(RAW_DIR.glob("*/*/*.json")):
        record = json.loads(path.read_text())
        meta = record["meta"]
        for f in record["payload"]:
            icao24, first, last = f.get("icao24"), f.get("firstSeen"), f.get("lastSeen")
            if not icao24 or first is None or last is None:
                rejected += 1
                continue
            departure = f.get("estDepartureAirport")
            arrival = f.get("estArrivalAirport")
            rows.append(
                {
                    "city": meta["city"],
                    "country": meta["country"],
                    "queried_airport": meta["airport"],
                    "direction": meta["direction"],
                    "icao24": icao24,
                    "callsign": (f.get("callsign") or "").strip() or None,
                    "first_seen": pd.to_datetime(first, unit="s", utc=True),
                    "last_seen": pd.to_datetime(last, unit="s", utc=True),
                    "est_departure_airport": departure,
                    "est_arrival_airport": arrival,
                    # the airport at the other end of the flight
                    "other_airport": (
                        departure if meta["direction"] == "arrival" else arrival
                    ),
                    "fetched_at": meta["fetched_at"],
                    "batch_id": meta["batch_id"],
                    "raw_file": path.name,
                }
            )
    if rejected:
        log.warning(
            "%d flight records missing icao24/firstSeen/lastSeen were skipped", rejected
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # Keep the most recently fetched copy of each observation
    return (
        df.sort_values("fetched_at")
        .drop_duplicates(DEDUP_KEY, keep="last")
        .sort_values(["city", "first_seen"], ignore_index=True)
    )


def export_movements_csv(path=f"data/opensky_mobility_{date.today()}.csv"):
    """Write all raw responses to one deduplicated CSV. Safe to re-run: it is
    rebuilt from data/raw each time, so it never accumulates duplicates."""
    movements = build_movements()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    movements.to_csv(path, index=False)
    log.info("wrote %d movements to %s", len(movements), path)
    return movements


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
