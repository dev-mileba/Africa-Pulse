"""Build the silver layer inside db_in_ch: a conformed city dimension plus
cleaned, validated versions of bronze.{weather,fx_rates,mobility}.

Every table is fully rebuilt each run (TRUNCATE + re-derive from bronze),
so silver always reflects bronze's current state and re-running never
duplicates rows. One consequence: silver.rejected_records shows what is
CURRENTLY wrong in bronze, not a historical log of every past rejection --
that is a deliberate trade-off for simplicity at the current data volume,
not an oversight. A row that fails a hard rule (unknown city/currency/
airport, an impossible value, a malformed key) is quarantined in
rejected_records with its original values. A row that is merely suspicious
(a missing value, a large FX move, an unresolved flight leg) is kept in the
silver table with a quality_flag, never silently dropped.

Run standalone:  python -m pipeline.functions.silver
"""

import psycopg2

from pipeline.functions.load_db import connect

# (city_id, city_name, country_code, latitude, longitude, airport_icao, currency)
# Reference data, not derived from any source -- matches the CITIES list in
# the DAG and in the ingest functions' op_kwargs.
CITIES = [
    (1, "Johannesburg", "ZA", -26.2041, 28.0473, "FAOR", "ZAR"),
    (2, "Cape Town", "ZA", -33.9249, 18.4241, "FACT", "ZAR"),
    (3, "Nairobi", "KE", -1.2833, 36.8167, "HKJK", "KES"),
]

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS silver"

CREATE_DIM_CITY = """
    CREATE TABLE IF NOT EXISTS silver.dim_city (
        city_id       smallint PRIMARY KEY,
        city_name     text NOT NULL UNIQUE,
        country_code  text NOT NULL,
        latitude      double precision NOT NULL,
        longitude     double precision NOT NULL,
        airport_icao  text NOT NULL UNIQUE,
        currency      text NOT NULL
    )
"""

CREATE_REJECTED_RECORDS = """
    CREATE TABLE IF NOT EXISTS silver.rejected_records (
        source       text NOT NULL,
        rule         text NOT NULL,
        natural_key  text NOT NULL,
        record       jsonb NOT NULL,
        rejected_at  timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (source, rule, natural_key)
    )
"""

CREATE_WEATHER_HOURLY = """
    CREATE TABLE IF NOT EXISTS silver.weather_hourly (
        city_id            smallint NOT NULL REFERENCES silver.dim_city (city_id),
        observed_at        timestamptz NOT NULL,
        temperature_c      double precision,
        precipitation_mm   double precision,
        co2_ppm            double precision,
        dust_ugm3          double precision,
        co_ugm3            double precision,
        quality_flag       text NOT NULL,
        source_updated_at  timestamptz,
        PRIMARY KEY (city_id, observed_at)
    )
"""

CREATE_FX_RATES_DAILY = """
    CREATE TABLE IF NOT EXISTS silver.fx_rates_daily (
        rate_date          date NOT NULL,
        base_currency      text NOT NULL,
        quote_currency     text NOT NULL,
        rate               numeric(20, 8),
        day_change_pct     double precision,
        quality_flag       text NOT NULL,
        source_fetched_at  timestamptz,
        PRIMARY KEY (base_currency, quote_currency, rate_date)
    )
"""

CREATE_FLIGHT_MOVEMENTS = """
    CREATE TABLE IF NOT EXISTS silver.flight_movements (
        city_id              smallint NOT NULL REFERENCES silver.dim_city (city_id),
        airport_icao         text NOT NULL,
        direction            text NOT NULL,
        icao24               text NOT NULL,
        callsign             text,
        first_seen           timestamptz NOT NULL,
        last_seen            timestamptz NOT NULL,
        event_time           timestamptz NOT NULL,
        event_date           date NOT NULL,
        origin_airport       text,
        destination_airport  text,
        other_airport        text,
        quality_flag         text NOT NULL,
        source_fetched_at    timestamptz,
        source_batch_id      uuid,
        PRIMARY KEY (airport_icao, direction, icao24, first_seen, last_seen)
    )
"""

# Rebuild each fact table from bronze in one shot: rows that pass every rule
# go into the silver table, rows that fail one go into rejected_records with
# their original values. reject_rule is NULL exactly when a row passes.
REBUILD_WEATHER = """
    WITH checked AS (
        SELECT
            d.city_id,
            b.city AS city_name,
            b.date AS observed_at,
            b.temperature_2m AS temperature_c,
            b.precipitation AS precipitation_mm,
            b.carbon_dioxide AS co2_ppm,
            b.dust AS dust_ugm3,
            b.carbon_monoxide AS co_ugm3,
            b.updated_at AS source_updated_at,
            CASE
                WHEN d.city_id IS NULL THEN 'unknown_city'
                WHEN b.temperature_2m < -90 OR b.temperature_2m > 60 THEN 'temperature_out_of_range'
                WHEN b.precipitation < 0 THEN 'negative_precipitation'
                WHEN b.dust < 0 THEN 'negative_dust'
                WHEN b.carbon_monoxide < 0 THEN 'negative_co'
                WHEN b.carbon_dioxide <= 0 THEN 'non_positive_co2'
            END AS reject_rule,
            CASE
                WHEN num_nulls(b.temperature_2m, b.precipitation, b.carbon_dioxide, b.dust, b.carbon_monoxide) > 0
                THEN 'missing_values'
                ELSE 'ok'
            END AS quality_flag
        FROM bronze.weather b
        LEFT JOIN silver.dim_city d ON d.city_name = b.city
    ),
    good AS (
        INSERT INTO silver.weather_hourly
            (city_id, observed_at, temperature_c, precipitation_mm, co2_ppm, dust_ugm3, co_ugm3,
             quality_flag, source_updated_at)
        SELECT city_id, observed_at, temperature_c, precipitation_mm, co2_ppm, dust_ugm3, co_ugm3,
               quality_flag, source_updated_at
        FROM checked
        WHERE reject_rule IS NULL
    )
    INSERT INTO silver.rejected_records (source, rule, natural_key, record)
    SELECT 'weather', reject_rule, concat_ws('|', city_name, observed_at), to_jsonb(c) - 'reject_rule'
    FROM checked c
    WHERE reject_rule IS NOT NULL
"""

REBUILD_FX_RATES = """
    WITH with_change AS (
        SELECT
            rate_date, base_currency, quote_currency, rate, fetched_at,
            lag(rate) OVER (PARTITION BY base_currency, quote_currency ORDER BY rate_date) AS prev_rate
        FROM bronze.fx_rates
    ),
    checked AS (
        SELECT
            rate_date, base_currency, quote_currency, rate,
            fetched_at AS source_fetched_at,
            CASE
                WHEN prev_rate IS NULL OR prev_rate = 0 THEN NULL
                ELSE ((rate - prev_rate) / prev_rate * 100)::double precision
            END AS day_change_pct,
            CASE
                WHEN quote_currency NOT IN (SELECT currency FROM silver.dim_city) THEN 'unknown_currency'
                WHEN rate <= 0 THEN 'non_positive_rate'
            END AS reject_rule
        FROM with_change
    ),
    flagged AS (
        SELECT *,
            CASE
                WHEN rate IS NULL THEN 'missing_rate'
                WHEN abs(day_change_pct) > 5 THEN 'large_move'
                ELSE 'ok'
            END AS quality_flag
        FROM checked
    ),
    good AS (
        INSERT INTO silver.fx_rates_daily
            (rate_date, base_currency, quote_currency, rate, day_change_pct, quality_flag, source_fetched_at)
        SELECT rate_date, base_currency, quote_currency, rate, day_change_pct, quality_flag, source_fetched_at
        FROM flagged
        WHERE reject_rule IS NULL
    )
    INSERT INTO silver.rejected_records (source, rule, natural_key, record)
    SELECT 'fx_rates', reject_rule, concat_ws('|', base_currency, quote_currency, rate_date),
           to_jsonb(f) - 'reject_rule'
    FROM flagged f
    WHERE reject_rule IS NOT NULL
"""

REBUILD_FLIGHT_MOVEMENTS = """
    WITH checked AS (
        SELECT
            d.city_id,
            b.queried_airport AS airport_icao,
            b.direction,
            b.icao24,
            NULLIF(BTRIM(b.callsign), '') AS callsign,
            b.first_seen,
            b.last_seen,
            CASE WHEN b.direction = 'arrival' THEN b.last_seen ELSE b.first_seen END AS event_time,
            (CASE WHEN b.direction = 'arrival' THEN b.last_seen ELSE b.first_seen END
                AT TIME ZONE 'UTC')::date AS event_date,
            b.est_departure_airport AS origin_airport,
            b.est_arrival_airport AS destination_airport,
            b.other_airport,
            b.fetched_at AS source_fetched_at,
            b.batch_id AS source_batch_id,
            CASE
                WHEN d.city_id IS NULL THEN 'unknown_airport'
                WHEN b.icao24 !~ '^[0-9a-f]{6}$' THEN 'invalid_icao24'
                WHEN b.last_seen < b.first_seen THEN 'last_seen_before_first_seen'
            END AS reject_rule,
            CASE
                WHEN b.est_departure_airport = b.est_arrival_airport THEN 'same_origin_destination'
                WHEN b.other_airport IS NULL THEN 'missing_other_airport'
                ELSE 'ok'
            END AS quality_flag
        FROM bronze.mobility b
        LEFT JOIN silver.dim_city d ON d.airport_icao = b.queried_airport
    ),
    good AS (
        INSERT INTO silver.flight_movements
            (city_id, airport_icao, direction, icao24, callsign, first_seen, last_seen, event_time,
             event_date, origin_airport, destination_airport, other_airport, quality_flag,
             source_fetched_at, source_batch_id)
        SELECT city_id, airport_icao, direction, icao24, callsign, first_seen, last_seen, event_time,
               event_date, origin_airport, destination_airport, other_airport, quality_flag,
               source_fetched_at, source_batch_id
        FROM checked
        WHERE reject_rule IS NULL
    )
    INSERT INTO silver.rejected_records (source, rule, natural_key, record)
    SELECT 'flights', reject_rule, concat_ws('|', airport_icao, direction, icao24, first_seen),
           to_jsonb(c) - 'reject_rule'
    FROM checked c
    WHERE reject_rule IS NOT NULL
"""


def build_dim_city(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA)
        cur.execute(CREATE_DIM_CITY)
        cur.execute("TRUNCATE silver.dim_city")
        cur.executemany(
            "INSERT INTO silver.dim_city VALUES (%s, %s, %s, %s, %s, %s, %s)", CITIES
        )
    conn.commit()
    print(f"silver.dim_city: {len(CITIES)} cities")


def _rebuild(conn, table, create_sql, rebuild_sql, source):
    """TRUNCATE a silver table and its rejected_records for `source`, then
    re-derive both from bronze in one transaction."""
    with conn.cursor() as cur:
        cur.execute(CREATE_REJECTED_RECORDS)
        cur.execute(create_sql)
        cur.execute(f"TRUNCATE silver.{table}")
        cur.execute("DELETE FROM silver.rejected_records WHERE source = %s", (source,))
        cur.execute(rebuild_sql)
        cur.execute(f"SELECT count(*) FROM silver.{table}")
        kept = cur.fetchone()[0]
        cur.execute(
            "SELECT count(*) FROM silver.rejected_records WHERE source = %s", (source,)
        )
        rejected = cur.fetchone()[0]
    conn.commit()
    print(f"silver.{table}: {kept} rows, {rejected} rejected ({source})")


def build_weather(conn):
    _rebuild(conn, "weather_hourly", CREATE_WEATHER_HOURLY, REBUILD_WEATHER, "weather")


def build_fx_rates(conn):
    _rebuild(conn, "fx_rates_daily", CREATE_FX_RATES_DAILY, REBUILD_FX_RATES, "fx_rates")


def build_flight_movements(conn):
    _rebuild(
        conn, "flight_movements", CREATE_FLIGHT_MOVEMENTS, REBUILD_FLIGHT_MOVEMENTS, "flights"
    )


def main(db_conn=None):
    """Build all of silver. Opens its own connection via load_db.connect()
    (which prints the target host/db) unless one is passed in -- pass an
    explicit db_conn when testing against something other than the real
    database, e.g. main(db_conn=my_scratch_conn), so a test can never
    silently fall through to the real one.

    Parameter is named db_conn, not conn: Airflow reserves "conn" as a
    context key (the Connections accessor), and a PythonOperator with no
    op_kwargs would silently pass that accessor in for a parameter named
    conn instead of the intended default of None.
    """
    owns_conn = db_conn is None
    if owns_conn:
        db_conn = connect()
    conn = db_conn
    try:
        build_dim_city(conn)  # must run first: the other tables join against it
        build_weather(conn)
        build_fx_rates(conn)
        build_flight_movements(conn)
    finally:
        if owns_conn:
            conn.close()


if __name__ == "__main__":
    main()
