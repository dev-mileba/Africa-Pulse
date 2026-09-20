from datetime import date

import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
from pathlib import Path

Path("data").mkdir(exist_ok=True)


def get_air_quality_data(longitude, latitude):
    cache_session = requests_cache.CachedSession(
        ".cache_air_quality", expire_after=3600
    )
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    air_url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    air_params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["carbon_dioxide", "dust", "carbon_monoxide"],
        "timezone": "auto",
        "start_date": "2026-09-13",
        "end_date": "2026-09-20",
    }
    air_responses = openmeteo.weather_api(air_url, params=air_params)
    air_response = air_responses[0]

    air_hourly = air_response.Hourly()
    air_hourly_carbon_dioxide = air_hourly.Variables(0).ValuesAsNumpy()
    air_hourly_dust = air_hourly.Variables(1).ValuesAsNumpy()
    air_hourly_carbon_monoxide = air_hourly.Variables(2).ValuesAsNumpy()

    return (air_hourly_carbon_dioxide, air_hourly_dust, air_hourly_carbon_monoxide)


def get_weather_data(longitude, latitude):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(
        ".cache_weather_data", expire_after=3600
    )
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "precipitation"],
        "timezone": "auto",
        "start_date": "2026-09-13",
        "end_date": "2026-09-20",
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # print(f"Coordinates: {response.Latitude()}°N {response.Longitude()}°E")
    timezone = response.Timezone()  # b'Africa/Lagos'
    timezone_str = timezone.decode("utf-8")  # 'Africa/Lagos'
    city = timezone_str.split("/")[-1]  # 'Lagos'
    city = city.replace("_", " ")  # handles names like 'Addis_Ababa' -> 'Addis Ababa'
    # print(city)

    hourly = response.Hourly()

    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ).tz_convert(response.Timezone().decode())
    }

    hourly_carbon_dioxide, hourly_dust, hourly_carbon_monoxide = get_air_quality_data(
        longitude, latitude
    )

    # print(len(hourly_temperature_2m))
    # print(len(hourly_carbon_dioxide))

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["city"] = city
    hourly_data["carbon_dioxide"] = hourly_carbon_dioxide
    hourly_data["carbon_monoxide"] = hourly_carbon_monoxide
    hourly_data["dust"] = hourly_dust
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe.to_csv(f"data/weather_{date.today()}.csv", mode="w")

    print(hourly_dataframe)
