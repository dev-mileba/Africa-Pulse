"""Build the gold layer inside db_in_ch: conformed dimensions, one fact
table per domain, the three single-domain marts, and the City Intelligence
composite score.

Everything except City Intelligence is mechanical -- same grain as silver,
just organized and aggregated for consumers.

City Intelligence design (see mart_city_intelligence below for the exact
formula): higher = more livable (comfortable environment + strong
connectivity + a stable currency), each domain weighted equally at 1/3 and
automatically rescaled when a domain has no data for a city/day, every raw
measure min-max normalized across this city set before combining -- so the
score is a relative comparison within these cities, not an absolute
standard. Trace a score back to its inputs by joining
mart_city_intelligence to mart_climate_environment / mart_mobility /
mart_economic on (city_id, date_key).

Run standalone:  python -m pipeline.functions.gold
"""

import psycopg2

from pipeline.functions.load_db import connect

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS gold"

CREATE_DIM_DATE = """
    CREATE TABLE IF NOT EXISTS gold.dim_date (
        date_key      date PRIMARY KEY,
        year          smallint NOT NULL,
        month         smallint NOT NULL,
        day           smallint NOT NULL,
        day_of_week   smallint NOT NULL,   -- 0 = Sunday
        week_of_year  smallint NOT NULL,
        month_name    text NOT NULL,
        is_weekend    boolean NOT NULL
    )
"""

# Covers the pipeline's operating window with headroom; cheap to extend later.
FILL_DIM_DATE = """
    INSERT INTO gold.dim_date
    SELECT
        d::date,
        extract(year FROM d)::smallint,
        extract(month FROM d)::smallint,
        extract(day FROM d)::smallint,
        extract(dow FROM d)::smallint,
        extract(week FROM d)::smallint,
        trim(to_char(d, 'Month')),
        extract(dow FROM d) IN (0, 6)
    FROM generate_series('2026-01-01'::date, '2027-12-31'::date, interval '1 day') d
    ON CONFLICT (date_key) DO NOTHING
"""

CREATE_DIM_CITY_VIEW = "CREATE OR REPLACE VIEW gold.dim_city AS SELECT * FROM silver.dim_city"

CREATE_FACT_WEATHER = """
    CREATE TABLE IF NOT EXISTS gold.fact_weather_hourly (
        city_id            smallint NOT NULL REFERENCES silver.dim_city (city_id),
        date_key           date NOT NULL REFERENCES gold.dim_date (date_key),
        observed_at        timestamptz NOT NULL,
        temperature_c      double precision,
        precipitation_mm   double precision,
        co2_ppm            double precision,
        dust_ugm3          double precision,
        co_ugm3            double precision,
        quality_flag       text NOT NULL,
        PRIMARY KEY (city_id, observed_at)
    )
"""

REBUILD_FACT_WEATHER = """
    INSERT INTO gold.fact_weather_hourly
    SELECT
        city_id,
        (observed_at AT TIME ZONE 'UTC')::date,
        observed_at, temperature_c, precipitation_mm, co2_ppm, dust_ugm3, co_ugm3, quality_flag
    FROM silver.weather_hourly
"""

CREATE_FACT_FX = """
    CREATE TABLE IF NOT EXISTS gold.fact_fx_rate_daily (
        date_key         date NOT NULL REFERENCES gold.dim_date (date_key),
        base_currency    text NOT NULL,
        quote_currency   text NOT NULL,
        rate             numeric(20, 8),
        day_change_pct   double precision,
        quality_flag     text NOT NULL,
        PRIMARY KEY (base_currency, quote_currency, date_key)
    )
"""

REBUILD_FACT_FX = """
    INSERT INTO gold.fact_fx_rate_daily
    SELECT rate_date, base_currency, quote_currency, rate, day_change_pct, quality_flag
    FROM silver.fx_rates_daily
"""

CREATE_FACT_FLIGHTS = """
    CREATE TABLE IF NOT EXISTS gold.fact_flight_movement (
        city_id        smallint NOT NULL REFERENCES silver.dim_city (city_id),
        date_key       date NOT NULL REFERENCES gold.dim_date (date_key),
        airport_icao   text NOT NULL,
        direction      text NOT NULL,
        icao24         text NOT NULL,
        first_seen     timestamptz NOT NULL,
        last_seen      timestamptz NOT NULL,
        event_time     timestamptz NOT NULL,
        other_airport  text,
        quality_flag   text NOT NULL,
        PRIMARY KEY (airport_icao, direction, icao24, first_seen, last_seen)
    )
"""

REBUILD_FACT_FLIGHTS = """
    INSERT INTO gold.fact_flight_movement
    SELECT
        city_id, event_date, airport_icao, direction, icao24,
        first_seen, last_seen, event_time, other_airport, quality_flag
    FROM silver.flight_movements
"""

# Grain: city x day. §9 minimum: temperature, precipitation, air-quality,
# comparable time trends. hours_missing counts weather_hourly's own
# missing_values flag, so consumers can see per-day data completeness
# instead of a silently averaged-over gap.
CREATE_MART_CLIMATE = """
    CREATE TABLE IF NOT EXISTS gold.mart_climate_environment (
        city_id                  smallint NOT NULL REFERENCES silver.dim_city (city_id),
        date_key                 date NOT NULL REFERENCES gold.dim_date (date_key),
        avg_temperature_c        double precision,
        min_temperature_c        double precision,
        max_temperature_c        double precision,
        total_precipitation_mm   double precision,
        avg_co2_ppm              double precision,
        avg_dust_ugm3            double precision,
        avg_co_ugm3              double precision,
        hours_observed           integer NOT NULL,
        hours_missing            integer NOT NULL,
        PRIMARY KEY (city_id, date_key)
    )
"""

REBUILD_MART_CLIMATE = """
    INSERT INTO gold.mart_climate_environment
    SELECT
        city_id, date_key,
        avg(temperature_c), min(temperature_c), max(temperature_c),
        sum(precipitation_mm), avg(co2_ppm), avg(dust_ugm3), avg(co_ugm3),
        count(*), count(*) FILTER (WHERE quality_flag = 'missing_values')
    FROM gold.fact_weather_hourly
    GROUP BY city_id, date_key
"""

# Grain: city x day. §9 minimum: daily demand/volume, temporal patterns.
# other_airport_missing_pct carries forward the route-data limitation
# (undercounted/uneven OpenSky coverage) rather than hiding it.
CREATE_MART_MOBILITY = """
    CREATE TABLE IF NOT EXISTS gold.mart_mobility (
        city_id                     smallint NOT NULL REFERENCES silver.dim_city (city_id),
        date_key                    date NOT NULL REFERENCES gold.dim_date (date_key),
        arrivals                    integer NOT NULL,
        departures                  integer NOT NULL,
        total_movements             integer NOT NULL,
        other_airport_missing_pct   double precision,
        PRIMARY KEY (city_id, date_key)
    )
"""

REBUILD_MART_MOBILITY = """
    INSERT INTO gold.mart_mobility
    SELECT
        city_id, date_key,
        count(*) FILTER (WHERE direction = 'arrival'),
        count(*) FILTER (WHERE direction = 'departure'),
        count(*),
        round(100.0 * count(*) FILTER (WHERE other_airport IS NULL) / count(*), 1)
    FROM gold.fact_flight_movement
    GROUP BY city_id, date_key
"""

# Grain: quote_currency x day, NOT city x day: ZAR covers two cities
# (Johannesburg, Cape Town) with one shared rate, and duplicating that rate
# per city would misrepresent it as two independent measurements. Consumers
# join to a city through dim_city.currency when they want a city view.
# base_currency is always USD (see fact_fx_rate_daily) so it is dropped here.
CREATE_MART_ECONOMIC = """
    CREATE TABLE IF NOT EXISTS gold.mart_economic (
        date_key         date NOT NULL REFERENCES gold.dim_date (date_key),
        quote_currency   text NOT NULL,
        rate             numeric(20, 8),
        day_change_pct   double precision,
        quality_flag     text NOT NULL,
        PRIMARY KEY (quote_currency, date_key)
    )
"""

REBUILD_MART_ECONOMIC = """
    INSERT INTO gold.mart_economic
    SELECT date_key, quote_currency, rate, day_change_pct, quality_flag
    FROM gold.fact_fx_rate_daily
"""

# Grain: city x day. The composite City Intelligence score. Every score
# column is 0-100, higher = better. environment/mobility/economic_score are
# each an average of their own min-max-normalized raw measures (direction
# noted below); city_intelligence_score is those three averaged, with a
# domain dropped and the rest rescaled when it has no data for that
# city/day (mathematically identical to "1/3 each, redistributed" since
# the weights are equal) -- n_components and components_used make that
# explicit rather than silent. Temperature is deliberately not scored: it
# has no single "better" direction (comfort is U-shaped) and expressing
# that needs an external reference point, which pure relative normalization
# doesn't have -- a stated limitation, not an oversight. When every usable
# value for a sub-measure is identical (no variance to normalize against),
# it scores a neutral 0.5/50.0 rather than being dropped: nobody is better
# or worse, which is different from the value being genuinely missing.
CREATE_MART_CITY_INTELLIGENCE = """
    CREATE TABLE IF NOT EXISTS gold.mart_city_intelligence (
        city_id                  smallint NOT NULL REFERENCES silver.dim_city (city_id),
        date_key                 date NOT NULL REFERENCES gold.dim_date (date_key),
        environment_score        double precision,  -- avg of: lower CO2/dust/CO/rain = better
        mobility_score           double precision,  -- higher total_movements = better
        economic_score           double precision,  -- lower abs(day_change_pct) = better
        city_intelligence_score  double precision,  -- average of the components present
        n_components             smallint NOT NULL,
        components_used          text NOT NULL,
        PRIMARY KEY (city_id, date_key)
    )
"""

REBUILD_MART_CITY_INTELLIGENCE = """
    WITH env_bounds AS (
        SELECT
            min(avg_co2_ppm) AS co2_min, max(avg_co2_ppm) AS co2_max,
            min(avg_dust_ugm3) AS dust_min, max(avg_dust_ugm3) AS dust_max,
            min(avg_co_ugm3) AS co_min, max(avg_co_ugm3) AS co_max,
            min(total_precipitation_mm) AS precip_min, max(total_precipitation_mm) AS precip_max
        FROM gold.mart_climate_environment
    ),
    -- Each sub-measure: NULL if the raw value itself is missing (stays
    -- excluded from the environment average below); 0.5 if every city has
    -- the same value (no variance to normalize against -- nobody is better
    -- or worse, so neutral rather than dropped); otherwise min-max normalized.
    env_norm AS (
        SELECT
            m.city_id, m.date_key,
            CASE WHEN m.avg_co2_ppm IS NULL THEN NULL
                 WHEN b.co2_max = b.co2_min THEN 0.5
                 ELSE 1 - (m.avg_co2_ppm - b.co2_min) / (b.co2_max - b.co2_min) END AS co2_n,
            CASE WHEN m.avg_dust_ugm3 IS NULL THEN NULL
                 WHEN b.dust_max = b.dust_min THEN 0.5
                 ELSE 1 - (m.avg_dust_ugm3 - b.dust_min) / (b.dust_max - b.dust_min) END AS dust_n,
            CASE WHEN m.avg_co_ugm3 IS NULL THEN NULL
                 WHEN b.co_max = b.co_min THEN 0.5
                 ELSE 1 - (m.avg_co_ugm3 - b.co_min) / (b.co_max - b.co_min) END AS co_n,
            CASE WHEN m.total_precipitation_mm IS NULL THEN NULL
                 WHEN b.precip_max = b.precip_min THEN 0.5
                 ELSE 1 - (m.total_precipitation_mm - b.precip_min) / (b.precip_max - b.precip_min) END AS precip_n
        FROM gold.mart_climate_environment m CROSS JOIN env_bounds b
    ),
    environment AS (
        SELECT
            city_id, date_key,
            100.0 * (COALESCE(co2_n, 0) + COALESCE(dust_n, 0) + COALESCE(co_n, 0) + COALESCE(precip_n, 0))
                / NULLIF(
                    (CASE WHEN co2_n IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN dust_n IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN co_n IS NOT NULL THEN 1 ELSE 0 END
                    + CASE WHEN precip_n IS NOT NULL THEN 1 ELSE 0 END), 0
                  ) AS environment_score
        FROM env_norm
    ),
    mob_bounds AS (
        SELECT min(total_movements) AS mv_min, max(total_movements) AS mv_max FROM gold.mart_mobility
    ),
    mobility AS (
        SELECT
            m.city_id, m.date_key,
            -- explicit ::double precision cast: total_movements/mv_min/mv_max
            -- are integers, and Postgres truncates integer/integer division
            CASE WHEN b.mv_max = b.mv_min THEN 50.0
                 ELSE 100.0 * (m.total_movements - b.mv_min)::double precision / (b.mv_max - b.mv_min)
            END AS mobility_score
        FROM gold.mart_mobility m CROSS JOIN mob_bounds b
    ),
    econ_raw AS (
        SELECT c.city_id, e.date_key, abs(e.day_change_pct) AS volatility
        FROM gold.mart_economic e
        JOIN gold.dim_city c ON c.currency = e.quote_currency
        WHERE e.day_change_pct IS NOT NULL
    ),
    econ_bounds AS (
        SELECT min(volatility) AS vol_min, max(volatility) AS vol_max FROM econ_raw
    ),
    economic AS (
        SELECT
            r.city_id, r.date_key,
            CASE WHEN b.vol_max = b.vol_min THEN 50.0
                 ELSE 100.0 * (1 - (r.volatility - b.vol_min) / (b.vol_max - b.vol_min))
            END AS economic_score
        FROM econ_raw r CROSS JOIN econ_bounds b
    ),
    all_keys AS (
        SELECT city_id, date_key FROM environment
        UNION
        SELECT city_id, date_key FROM mobility
        UNION
        SELECT city_id, date_key FROM economic
    ),
    combined AS (
        SELECT
            k.city_id, k.date_key,
            env.environment_score, mob.mobility_score, eco.economic_score,
            (CASE WHEN env.environment_score IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN mob.mobility_score IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN eco.economic_score IS NOT NULL THEN 1 ELSE 0 END) AS n_components
        FROM all_keys k
        LEFT JOIN environment env ON env.city_id = k.city_id AND env.date_key = k.date_key
        LEFT JOIN mobility mob ON mob.city_id = k.city_id AND mob.date_key = k.date_key
        LEFT JOIN economic eco ON eco.city_id = k.city_id AND eco.date_key = k.date_key
    )
    INSERT INTO gold.mart_city_intelligence
    SELECT
        city_id, date_key, environment_score, mobility_score, economic_score,
        CASE
            WHEN n_components = 0 THEN NULL
            ELSE (COALESCE(environment_score, 0) + COALESCE(mobility_score, 0) + COALESCE(economic_score, 0))
                 / n_components
        END,
        n_components,
        concat_ws(
            ',',
            CASE WHEN environment_score IS NOT NULL THEN 'environment' END,
            CASE WHEN mobility_score IS NOT NULL THEN 'mobility' END,
            CASE WHEN economic_score IS NOT NULL THEN 'economic' END
        )
    FROM combined
"""

# (create table, rebuild, target table to TRUNCATE) -- run in this order:
# dims first, then facts (which the marts are built from), then marts.
_FACTS = [
    ("fact_weather_hourly", CREATE_FACT_WEATHER, REBUILD_FACT_WEATHER),
    ("fact_fx_rate_daily", CREATE_FACT_FX, REBUILD_FACT_FX),
    ("fact_flight_movement", CREATE_FACT_FLIGHTS, REBUILD_FACT_FLIGHTS),
]
# city_intelligence must rebuild last: it reads the other three marts.
_MARTS = [
    ("mart_climate_environment", CREATE_MART_CLIMATE, REBUILD_MART_CLIMATE),
    ("mart_mobility", CREATE_MART_MOBILITY, REBUILD_MART_MOBILITY),
    ("mart_economic", CREATE_MART_ECONOMIC, REBUILD_MART_ECONOMIC),
    ("mart_city_intelligence", CREATE_MART_CITY_INTELLIGENCE, REBUILD_MART_CITY_INTELLIGENCE),
]


def build_dimensions(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_SCHEMA)
        cur.execute(CREATE_DIM_DATE)
        cur.execute(FILL_DIM_DATE)
        cur.execute(CREATE_DIM_CITY_VIEW)
    conn.commit()
    print("gold.dim_date, gold.dim_city ready")


def _rebuild(conn, table, create_sql, rebuild_sql):
    with conn.cursor() as cur:
        cur.execute(create_sql)
        cur.execute(f"TRUNCATE gold.{table}")
        cur.execute(rebuild_sql)
        cur.execute(f"SELECT count(*) FROM gold.{table}")
        n = cur.fetchone()[0]
    conn.commit()
    print(f"gold.{table}: {n} rows")


def build_facts(conn):
    for table, create_sql, rebuild_sql in _FACTS:
        _rebuild(conn, table, create_sql, rebuild_sql)


def build_marts(conn):
    for table, create_sql, rebuild_sql in _MARTS:
        _rebuild(conn, table, create_sql, rebuild_sql)


def main(conn=None):
    """Opens its own connection via load_db.connect() (which prints the
    target host/db) unless one is passed in -- pass an explicit conn when
    testing against something other than the real database.
    """
    owns_conn = conn is None
    if owns_conn:
        conn = connect()
    try:
        build_dimensions(conn)  # facts reference dim_date, so this runs first
        build_facts(conn)  # marts are built from the facts, so this runs next
        build_marts(conn)
    finally:
        if owns_conn:
            conn.close()


if __name__ == "__main__":
    main()
