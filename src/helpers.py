from datetime import date

import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
from pathlib import Path

Path("data").mkdir(exist_ok=True)

WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AIR_QUALITY_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
OPEN_SKY_URL = f"https://opensky-network.org/api/flights/"

WEATHER_VARIABLES = ["temperature_2m", "precipitation"]
AIR_QUALITY_VARIABLES = ["carbon_dioxide", "dust", "carbon_monoxide"]


def _client(cache_name):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(cache_name, expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def _hourly_frame(response, variables):
    hourly = response.Hourly()
    df = pd.DataFrame(
        {
            "date": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left",
            )
        }
    )
    # The order of variables in the request must match the order read here
    for i, name in enumerate(variables):
        df[name] = hourly.Variables(i).ValuesAsNumpy()
    return df


def _fetch_hourly(client, url, cities, variables, start_date, end_date):
    params = {
        "latitude": [c["lat"] for c in cities],
        "longitude": [c["lon"] for c in cities],
        "hourly": variables,
        "timezone": "UTC",
        "start_date": start_date,
        "end_date": end_date,
    }
    responses = client.weather_api(url, params=params)

    # Responses come back in the same order as the requested locations
    frames = []
    for city, response in zip(cities, responses, strict=True):
        df = _hourly_frame(response, variables)
        df["city"] = city["city"]
        df["country"] = city["country"]
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def get_air_quality_data(cities, start_date, end_date):
    client = _client(".cache_air_quality")
    return _fetch_hourly(
        client, AIR_QUALITY_URL, cities, AIR_QUALITY_VARIABLES, start_date, end_date
    )


def get_weather_data(cities, start_date, end_date):
    client = _client(".cache_weather_data")
    weather = _fetch_hourly(
        client, WEATHER_URL, cities, WEATHER_VARIABLES, start_date, end_date
    )
    air_quality = get_air_quality_data(cities, start_date, end_date)

    # Join on city + timestamp (not row position). An outer join keeps gaps
    # visible instead of silently dropping rows that only one source has.
    hourly_dataframe = weather.merge(
        air_quality, on=["city", "country", "date"], how="outer"
    ).sort_values(["city", "date"], ignore_index=True)

    hourly_dataframe.to_csv(f"data/weather_{date.today()}.csv", index=False)

    print(hourly_dataframe)
    return hourly_dataframe
