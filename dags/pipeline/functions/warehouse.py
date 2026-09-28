"""Build the bronze/silver/gold layers inside db_in_ch, on top of the
`public` schema that load_db.py writes to (Postgres, hosted on ClickHouse
Cloud's Managed Postgres service -- see connect() in load_db.py).

This module currently implements:
  - bronze: a structural copy of public.{fx_rates,weather,mobility}, kept in
    sync with the same upsert rules load_db.py already uses (fx_rates and
    weather are corrected in place; mobility is append-only).
  - silver/gold: schemas only, created empty. Populating them (conformed
    dim_city/dim_date, cleaned+validated facts, and the marts) is a
    follow-up step -- in particular the City Intelligence composite score
    needs weighting/normalization decisions that shouldn't be invented here.

Run standalone:  python -m pipeline.functions.warehouse
"""

import psycopg2

from pipeline.functions.load_db import TABLES, connect

CREATE_BRONZE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS bronze"
CREATE_SILVER_SCHEMA = "CREATE SCHEMA IF NOT EXISTS silver"
CREATE_GOLD_SCHEMA = "CREATE SCHEMA IF NOT EXISTS gold"

# LIKE ... INCLUDING ALL copies the source table's primary key too, so the
# ON CONFLICT clauses below have something to key on.
CREATE_BRONZE_TABLE = "CREATE TABLE IF NOT EXISTS bronze.{table} (LIKE public.{table} INCLUDING ALL)"

# Same natural key + on_conflict policy as load_db.py's own upsert into
# public, so bronze always mirrors what public currently holds.
COPY_TO_BRONZE = {
    "fx_rates": """
        INSERT INTO bronze.fx_rates
        SELECT * FROM public.fx_rates
        ON CONFLICT (base_currency, quote_currency, rate_date) DO UPDATE SET
            rate = EXCLUDED.rate,
            fetched_at = EXCLUDED.fetched_at,
            batch_id = EXCLUDED.batch_id,
            day_change_pct = EXCLUDED.day_change_pct,
            quality_flag = EXCLUDED.quality_flag
    """,
    "weather": """
        INSERT INTO bronze.weather
        SELECT * FROM public.weather
        ON CONFLICT (city, date) DO UPDATE SET
            country = EXCLUDED.country,
            temperature_2m = EXCLUDED.temperature_2m,
            precipitation = EXCLUDED.precipitation,
            carbon_dioxide = EXCLUDED.carbon_dioxide,
            dust = EXCLUDED.dust,
            carbon_monoxide = EXCLUDED.carbon_monoxide,
            updated_at = EXCLUDED.updated_at
    """,
    "mobility": """
        INSERT INTO bronze.mobility
        SELECT * FROM public.mobility
        ON CONFLICT (queried_airport, direction, icao24, first_seen, last_seen) DO NOTHING
    """,
}


def build_bronze(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_BRONZE_SCHEMA)
        for table in TABLES:  # fx_rates, weather, mobility
            cur.execute(CREATE_BRONZE_TABLE.format(table=table))
        for table, sql in COPY_TO_BRONZE.items():
            cur.execute(sql)
            print(f"bronze.{table}: {cur.rowcount} rows inserted or updated")
    conn.commit()


def create_empty_schemas(conn: psycopg2.extensions.connection) -> None:
    """Scaffolding only -- see the module docstring for what's still missing."""
    with conn.cursor() as cur:
        cur.execute(CREATE_SILVER_SCHEMA)
        cur.execute(CREATE_GOLD_SCHEMA)
    conn.commit()


def main(conn=None):
    """Opens its own connection via load_db.connect() (which prints the
    target host/db) unless one is passed in -- pass an explicit conn when
    testing against something other than the real database.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = connect()
    try:
        build_bronze(conn)
        create_empty_schemas(conn)
    finally:
        if owns_conn:
            conn.close()


if __name__ == "__main__":
    main()
