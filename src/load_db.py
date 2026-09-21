import pandas as pd
from datetime import date
import psycopg2
import numpy as np
from psycopg2.extras import execute_values
from psycopg2.extensions import register_adapter, AsIs

conn = psycopg2.connect(
    host="localhost",
    port="5432",
    dbname="db_in_psg",
    user="postgres",
    password="postgres",
)

# TODO: Change the date to a dynamic date
fx_rates = pd.read_csv(f"../data/fx_rates_2026-09-20.csv")
mobility = pd.read_csv(f"../data/opensky_mobility_2026-09-20.csv")
weather = pd.read_csv(f"../data/weather_2026-09-20.csv")

cur = conn.cursor()

cur.execute("""
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
    """)

cur.execute("""
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
""")

cur.execute("""
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
""")

# register_adapter(np.int64, lambda val: AsIs(val))
# register_adapter(np.float64, lambda val: AsIs(val))
#
# rows = [tuple(x) for x in fx_rates.to_numpy()]
# cols = ",".join(fx_rates.columns)
#
# # Add the values
# execute_values(cur, f"INSERT INTO fx_rates ({cols}) VALUES %s", rows)
#
# conn.commit()


def load(table_name, dataframe):
    if table_name not in {"fx_rates", "weather", "mobility"}:
        raise ValueError(f"Unsupported table name: {table_name}")

    register_adapter(np.int64, lambda val: AsIs(val))
    register_adapter(np.float64, lambda val: AsIs(val))

    rows = [tuple(x) for x in dataframe.to_numpy()]
    cols = ",".join(dataframe.columns)

    # Add the values
    execute_values(cur, f"INSERT INTO {table_name} ({cols}) VALUES %s", rows)

    conn.commit()


load("fx_rates", fx_rates)
load("weather", weather)
load("mobility", mobility)
