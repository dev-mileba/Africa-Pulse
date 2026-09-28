from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))

SOURCE_DIR = DATA_DIR / "source"
STAGING_DIR = DATA_DIR / "staging"
WAREHOUSE_DIR = DATA_DIR / "warehouse"

FX_RATE_SOURCE = SOURCE_DIR / "fx_rates.csv"
WEATHER_SOURCE = SOURCE_DIR / "weather.csv"
OPENSKY_MOBILITY_SOURCE = SOURCE_DIR / "opensky_mobility.csv"

FX_RATE_STAGING = STAGING_DIR / "fx_rates.csv"
WEATHER_STAGING = STAGING_DIR / "weather.csv"
OPENSKY_MOBILITY_STAGING = STAGING_DIR / "opensky_mobility.csv"

def ensure_dirs() -> None:
    for d in (SOURCE_DIR, STAGING_DIR, WAREHOUSE_DIR):
        d.mkdir(parents=True, exist_ok=True)