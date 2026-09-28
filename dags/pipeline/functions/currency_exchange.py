"""Daily USD-based exchange rates from ExchangeRate-API (history endpoint).

One request per UTC day. Rates for the requested currencies are turned
straight into a DataFrame (one row per day and currency) and written to CSV.
The API key is sent in a header, so it never appears in URLs or logs.
"""

import logging
import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# This file lives at dags/pipeline/functions/, so the root is 3 levels up.
load_dotenv(Path(__file__).resolve().parents[3] / ".env")
EXCHANGE_RATE_API_KEY = os.environ["EXCHANGE_RATE_API_KEY"]

log = logging.getLogger(__name__)

BASE_URL = "https://v6.exchangerate-api.com/v6/history"
BASE_CURRENCY = "USD"

MAX_ATTEMPTS = 5
PAUSE_BETWEEN_CALLS = 0.5
# A day-over-day move bigger than this is flagged as suspicious (kept, not dropped)
LARGE_MOVE_PCT = 5.0

# Retrying these cannot help: stop the run and tell the user
STOP_ERRORS = {
    "quota-reached",
    "plan-upgrade-required",
    "invalid-key",
    "inactive-account",
}


class AccessProblem(Exception):
    """The API refused us for a reason a retry will not fix (quota, plan, key)."""


def _fetch_day(day):
    """Fetch one day's rates. Returns the parsed response body."""
    url = f"{BASE_URL}/{BASE_CURRENCY}/{day.year}/{day.month}/{day.day}"
    headers = {"Authorization": f"Bearer {EXCHANGE_RATE_API_KEY}"}

    for attempt in range(1, MAX_ATTEMPTS + 1):
        wait = 2**attempt
        try:
            r = requests.get(url, headers=headers, timeout=30)
        except requests.RequestException as e:
            # log the type only: exception text can contain the request URL
            log.warning(
                "request error (%s), attempt %d/%d",
                type(e).__name__,
                attempt,
                MAX_ATTEMPTS,
            )
            time.sleep(wait)
            continue

        try:
            body = r.json()
        except ValueError:
            body = None
        error = body.get("error-type") if isinstance(body, dict) else None
        if error in STOP_ERRORS:
            raise AccessProblem(error)
        if r.status_code == 429 or r.status_code >= 500:
            log.warning("HTTP %d for %s, retrying in %ds", r.status_code, day, wait)
            time.sleep(wait)
            continue
        break
    else:
        raise RuntimeError(f"gave up after {MAX_ATTEMPTS} attempts for {day}")

    if (
        r.status_code != 200
        or not isinstance(body, dict)
        or body.get("result") != "success"
    ):
        raise RuntimeError(
            f"no usable rates for {day}: HTTP {r.status_code}, error={error}"
        )
    # Guard against an unexpected response for a different date
    if (body.get("year"), body.get("month"), body.get("day")) != (
        day.year,
        day.month,
        day.day,
    ):
        raise RuntimeError(f"response date does not match requested {day}")
    return body


def _add_quality_flags(df):
    """Every row gets a quality_flag. Flagged rows are kept so problems stay visible."""
    df = df.sort_values(["quote_currency", "rate_date"], ignore_index=True)
    df["day_change_pct"] = df.groupby("quote_currency")["rate"].pct_change() * 100
    df["quality_flag"] = "ok"
    df.loc[df["day_change_pct"].abs() > LARGE_MOVE_PCT, "quality_flag"] = "large_move"
    df.loc[df["rate"] <= 0, "quality_flag"] = "non_positive"
    df.loc[df["rate"].isna(), "quality_flag"] = "missing"
    return df


def ingest_fx(start_day, end_day, currencies):
    """Fetch USD rates for every day in the range (inclusive).

    Returns a DataFrame with one row per day and currency. Only days up to
    yesterday are requested. A day that fails is logged and left out; it is
    not filled with made-up values.
    """
    batch_id = str(uuid.uuid4())
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    if end_day > yesterday:
        log.info("capping end_day %s to %s", end_day, yesterday)
        end_day = yesterday

    rows = []
    fetched = failed = 0
    day = start_day
    try:
        while day <= end_day:
            try:
                body = _fetch_day(day)
                fetched_at = datetime.now(timezone.utc).isoformat()
                rates = body.get("conversion_rates", {})
                for currency in currencies:
                    rows.append(
                        {
                            "rate_date": day,
                            "base_currency": BASE_CURRENCY,
                            "quote_currency": currency,
                            "rate": pd.to_numeric(rates.get(currency), errors="coerce"),
                            "fetched_at": fetched_at,
                            "batch_id": batch_id,
                        }
                    )
                log.info("%s: %d currencies", day, len(rates))
                fetched += 1
            except AccessProblem:
                raise
            except Exception:
                # One bad day must not stop the rest
                log.exception("failed %s", day)
                failed += 1
            time.sleep(PAUSE_BETWEEN_CALLS)
            day += timedelta(days=1)
    except AccessProblem as e:
        log.error(
            "stopping early: API refused the request (%s). Check plan/quota/key. "
            "Results below are partial.",
            e,
        )

    log.info("batch %s: fetched=%d failed=%d", batch_id, fetched, failed)

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(["rate_date", "base_currency", "quote_currency"])
    return _add_quality_flags(df)


def export_fx_csv(fx, path=f"data/fx_rates_{date.today()}.csv"):
    """Write the rates DataFrame to CSV."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fx.to_csv(path, index=False)
    log.info("wrote %d rows to %s", len(fx), path)


def summarize_fx(fx):
    """Per currency: days covered, date range, rate range, and flagged rows."""
    if fx.empty:
        return "no rates"
    return fx.groupby("quote_currency").agg(
        days=("rate_date", "nunique"),
        first=("rate_date", "min"),
        last=("rate_date", "max"),
        min_rate=("rate", "min"),
        max_rate=("rate", "max"),
        flagged=("quality_flag", lambda s: int((s != "ok").sum())),
    )
