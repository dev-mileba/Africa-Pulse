import os
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

load_dotenv()

TABLES = {"fx_rates", "weather", "mobility"}

CREATE_FX_RATES = """
    CREATE TABLE IF NOT EXISTS fx_rates (
    rate_date       date           NOT NULL,
    base_currency   text           NOT NULL,   -- always USD for now
    quote_currency  text           NOT NULL,   -- ZAR | KES
    rate            numeric(20,8),             -- NULL if the currency was missing in the response
    fetched_at      timestamptz,
    batch_id        uuid,
    day_change_pct  double precision,          -- NULL on each currency's first day
    quality_flag    text,                      -- ok | large_move | non_positive | missing
    PRIMARY KEY (base_currency, quote_currency, rate_date)
);
"""

CREATE_WEATHER = """
    CREATE TABLE IF NOT EXISTS weather (
    city             text              NOT NULL,
    country          text              NOT NULL,
    date             timestamptz       NOT NULL,   -- the hour, UTC
    temperature_2m   double precision,             -- °C
    precipitation    double precision,             -- mm
    carbon_dioxide   double precision,             -- ppm
    dust             double precision,             -- µg/m³
    carbon_monoxide  double precision,             -- µg/m³
    updated_at       timestamptz       NOT NULL DEFAULT now(),
    PRIMARY KEY (city, date)
);
"""

CREATE_MOBILITY = """
    CREATE TABLE IF NOT EXISTS mobility (
    city                   text         NOT NULL,
    country                text         NOT NULL,
    queried_airport        text         NOT NULL,
    direction              text         NOT NULL,   -- arrival | departure
    icao24                 text         NOT NULL,
    callsign               text,
    first_seen             timestamptz  NOT NULL,
    last_seen              timestamptz  NOT NULL,
    est_departure_airport  text,
    est_arrival_airport    text,
    other_airport          text,
    fetched_at             timestamptz,
    batch_id               uuid,
    raw_file               text,                    -- only in the older CSV
    PRIMARY KEY (queried_airport, direction, icao24, first_seen, last_seen)
);
"""


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_FX_RATES)
        cur.execute(CREATE_WEATHER)
        cur.execute(CREATE_MOBILITY)
    conn.commit()


def load(conn, table_name, dataframe):
    """Bulk-insert a DataFrame into an existing table."""
    if table_name not in TABLES:
        raise ValueError(f"Unsupported table name: {table_name}")
    if dataframe.empty:
        print(f"{table_name}: nothing to load (empty dataframe)")
        return

    register_adapter(np.int64, lambda val: AsIs(val))
    register_adapter(np.float64, lambda val: AsIs(val))

    rows = [tuple(x) for x in dataframe.to_numpy()]
    cols = ",".join(dataframe.columns)

    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table_name} ({cols}) VALUES %s", rows)
    conn.commit()
    print(f"{table_name}: loaded {len(rows)} rows")


def main():
    # TODO: pick the latest CSV instead of a hardcoded date
    # TODO: add ON CONFLICT so a second run doesn't fail on the primary key
    root = Path(__file__).resolve().parents[3]  # project root
    fx_rates = pd.read_csv(root / "data" / "fx_rates_2026-09-20.csv")
    mobility = pd.read_csv(root / "data" / "opensky_mobility_2026-09-20.csv")
    weather = pd.read_csv(root / "data" / "weather_2026-09-20.csv")

    conn = connect()
    try:
        create_tables(conn)
        load(conn, "fx_rates", fx_rates)
        load(conn, "weather", weather)
        load(conn, "mobility", mobility)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
