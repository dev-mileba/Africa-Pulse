import logging
from datetime import date, timedelta

from currency_exchange import export_fx_csv, ingest_fx, summarize_fx
from helpers import get_weather_data
from opensky import export_movements_csv, ingest_flights, summarize

CITIES = [
    {
        "city": "Johannesburg",
        "country": "ZA",
        "lat": -26.2041,
        "lon": 28.0473,
        "airport_icao": "FAOR",
        "currency": "ZAR",
    },
    {
        "city": "Cape Town",
        "country": "ZA",
        "lat": -33.9249,
        "lon": 18.4241,
        "airport_icao": "FACT",
        "currency": "ZAR",
    },
    {
        "city": "Nairobi",
        "country": "KE",
        "lat": -1.2833,
        "lon": 36.8167,
        "airport_icao": "HKJK",
        "currency": "KES",
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
    # end = date.today() - timedelta(days=1)
    # start = end - timedelta(days=6)
    movements = ingest_flights(CITIES, start, end)
    export_movements_csv(movements)
    print(summarize(movements))

    # Daily USD->local currency rates over the same window
    currencies = sorted({c["currency"] for c in CITIES})
    fx = ingest_fx(start, end, currencies)
    export_fx_csv(fx)
    print(summarize_fx(fx))


if __name__ == "__main__":
    main()
