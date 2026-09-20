import logging
from datetime import date, timedelta

from helpers import get_weather_data
from opensky import export_movements_csv, ingest_flights, summarize

CITIES = [
    {
        "city": "Johannesburg",
        "country": "ZA",
        "lat": -26.2041,
        "lon": 28.0473,
        "airport_icao": "FAOR",
    },
    {
        "city": "Cape Town",
        "country": "ZA",
        "lat": -33.9249,
        "lon": 18.4241,
        "airport_icao": "FACT",
    },
    {
        "city": "Nairobi",
        "country": "KE",
        "lat": -1.2833,
        "lon": 36.8167,
        "airport_icao": "HKJK",
    },
]


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=6)
    get_weather_data(CITIES, start.isoformat(), end.isoformat())

    # OpenSky flight data lags by a day, so the last full day is yesterday
    end = date.today() - timedelta(days=1)
    start = end - timedelta(days=6)
    ingest_flights(CITIES, start, end)
    movements = export_movements_csv()
    print(summarize(movements))


if __name__ == "__main__":
    main()
