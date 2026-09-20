from datetime import date, timedelta

from helpers import get_weather_data

CITIES = [
    {"city": "Lagos", "country": "NG", "lat": 6.4541, "lon": 3.3947},
    {"city": "Abuja", "country": "NG", "lat": 9.0579, "lon": 7.4951},
    {"city": "Nairobi", "country": "KE", "lat": -1.2833, "lon": 36.8167},
]


def main():
    end = date.today()
    start = end - timedelta(days=7)
    get_weather_data(CITIES, start.isoformat(), end.isoformat())


if __name__ == "__main__":
    main()
