import os

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import AsIs, register_adapter
from psycopg2.extras import execute_values

from pipeline.paths import FX_RATE_SOURCE, OPENSKY_MOBILITY_SOURCE, WEATHER_SOURCE

load_dotenv()

# Natural key (must match each table's PRIMARY KEY above) and what a
# duplicate key should do: "update" refreshes the row with the new values,
# "nothing" leaves the existing row alone.
#   - fx_rates: rates can be corrected after the fact -> update
#   - weather:  recent hours are forecasts that get revised -> update
#   - mobility: a finished flight rarely changes -> leave the first copy
TABLES = {
    "fx_rates": {
        "key": ["base_currency", "quote_currency", "rate_date"],
        "on_conflict": "update",
    },
    "weather": {
        "key": ["city", "date"],
        "on_conflict": "update",
    },
    "mobility": {
        "key": ["queried_airport", "direction", "icao24", "first_seen", "last_seen"],
        "on_conflict": "nothing",
    },
}

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
    """Connect using POSTGRES_* from .env. Shared by every module in this
    package (load_db, warehouse, silver, ...), so this is the one place to
    look if a script unexpectedly touched the real database: it always
    prints exactly which host/db it's about to open, host first, host/db
    only (never the password), specifically so that never has to be
    guessed or discovered after the fact.
    """
    host, dbname = os.getenv("POSTGRES_HOST"), os.getenv("POSTGRES_DB")
    print(f"[load_db.connect] connecting to postgres://{host}/{dbname}")
    return psycopg2.connect(
        host=host,
        port=os.getenv("POSTGRES_PORT"),
        dbname=dbname,
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
    """Upsert a DataFrame into an existing table on its natural key.

    Safe to re-run: a row that already exists is either refreshed or left
    alone (see TABLES[table_name]["on_conflict"]), never duplicated or
    rejected with a primary-key error.
    """
    if table_name not in TABLES:
        raise ValueError(f"Unsupported table name: {table_name}")
    if dataframe.empty:
        print(f"{table_name}: nothing to load (empty dataframe)")
        return

    register_adapter(np.int64, lambda val: AsIs(val))
    register_adapter(np.float64, lambda val: AsIs(val))

    spec = TABLES[table_name]
    key_cols = spec["key"]
    update_cols = [c for c in dataframe.columns if c not in key_cols]

    if spec["on_conflict"] == "update" and update_cols:
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
        conflict_clause = f"ON CONFLICT ({', '.join(key_cols)}) DO UPDATE SET {set_clause}"
    else:
        conflict_clause = f"ON CONFLICT ({', '.join(key_cols)}) DO NOTHING"

    rows = [tuple(x) for x in dataframe.to_numpy()]
    cols = ",".join(dataframe.columns)
    sql = f"INSERT INTO {table_name} ({cols}) VALUES %s {conflict_clause}"

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
        affected = cur.rowcount
    conn.commit()
    print(f"{table_name}: {len(rows)} rows in file, {affected} inserted or updated")


def main(db_conn=None):
    """Opens its own connection via connect() (which prints the target
    host/db) unless one is passed in -- pass an explicit db_conn when
    testing against something other than the real database.

    Parameter is named db_conn, not conn: Airflow reserves "conn" as a
    context key (the Connections accessor for "{{ conn.my_id.host }}"
    templating). A PythonOperator with no op_kwargs passes its context
    through for any parameter name that matches a reserved key, so a
    parameter literally named conn silently received Airflow's connections
    accessor instead of the intended default of None here, skipping the
    real connect() call entirely.
    """
    # These are the same fixed-name files the extract/export functions write
    # to (see pipeline/paths.py), so this always reads the latest run's output.
    fx_rates = pd.read_csv(FX_RATE_SOURCE)
    mobility = pd.read_csv(OPENSKY_MOBILITY_SOURCE)
    weather = pd.read_csv(WEATHER_SOURCE)

    owns_conn = db_conn is None
    if owns_conn:
        db_conn = connect()
    conn = db_conn
    try:
        create_tables(conn)
        load(conn, "fx_rates", fx_rates)
        load(conn, "weather", weather)
        load(conn, "mobility", mobility)
    finally:
        if owns_conn:
            conn.close()


if __name__ == "__main__":
    main()
