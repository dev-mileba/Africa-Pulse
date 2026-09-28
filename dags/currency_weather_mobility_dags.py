from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from pipeline.functions.currency_exchange import ingest_and_export_fx
from pipeline.functions.helpers import ingest_and_export_weather
from pipeline.functions.opensky import ingest_and_export_flights

default_args = {
    "owner": "mileba",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": None,
}


def _run_and_push_watermark(func, **kwargs):
    """Call func (one of the ingest_and_export_* callables, each returning
    {"rows": int, "watermark_date": "YYYY-MM-DD" | None}), then push the
    watermark to its own XCom key so downstream tasks or the next run can
    read it directly instead of unpacking the whole return_value payload.

    The watermark is the latest date actually written this run, which can
    trail the requested end_day if a source capped the range or partially
    failed -- see the docstrings on the ingest_and_export_* functions.
    """
    result = func(**kwargs)
    get_current_context()["ti"].xcom_push(
        key="watermark_date", value=result.get("watermark_date")
    )
    return result


with DAG(
    dag_id="currency_weather_mobility_dag",
    description="Ingest currency exchange rates, weather data, and OpenSky flight movements for specified cities.",
    schedule="0 0 * * *",  # Daily at midnight
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["currency", "weather", "mobility", "etl"],
) as dag:

    # Each task fetches from its source and writes straight to CSV in one
    # step (ingest_and_export_fx / ingest_and_export_flights /
    # ingest_and_export_weather), instead of returning a DataFrame for a
    # separate task to pick up: Airflow's default XCom backend serializes to
    # JSON and cannot hold a pandas DataFrame, so a two-task ingest-then-export
    # split would fail as soon as it ran. Each callable instead returns a
    # small {"rows": ..., "watermark_date": ...} summary, and
    # _run_and_push_watermark also pushes watermark_date under its own XCom
    # key. The three sources are independent, so the tasks run in parallel;
    # loading the CSVs into Postgres (pipeline.functions.load_db) is a
    # separate, not-yet-wired-in step.

    ingest_currency_exchange = PythonOperator(
        task_id="ingest_currency_exchange",
        python_callable=partial(_run_and_push_watermark, ingest_and_export_fx),
        op_kwargs={
            "start_day": "{{ ds }}",
            "end_day": "{{ ds }}",
            "currencies": ["ZAR", "KES"],  # Add more currencies as needed
        },
    )

    ingest_weather_data = PythonOperator(
        task_id="ingest_weather_data",
        python_callable=partial(_run_and_push_watermark, ingest_and_export_weather),
        op_kwargs={
            "cities": [
                {"city": "Johannesburg", "country": "ZA", "lat": -26.2041, "lon": 28.0473},
                {"city": "Cape Town", "country": "ZA", "lat": -33.9249, "lon": 18.4241},
                {"city": "Nairobi", "country": "KE", "lat": -1.2833, "lon": 36.8167},
            ],
            "start_date": "{{ ds }}",
            "end_date": "{{ ds }}",
        },
    )

    ingest_flight_movements = PythonOperator(
        task_id="ingest_flight_movements",
        python_callable=partial(_run_and_push_watermark, ingest_and_export_flights),
        op_kwargs={
            "cities": [
                {"city": "Johannesburg", "country": "ZA", "airport_icao": "FAOR"},
                {"city": "Cape Town", "country": "ZA", "airport_icao": "FACT"},
                {"city": "Nairobi", "country": "KE", "airport_icao": "HKJK"},
            ],
            "start_day": "{{ ds }}",
            "end_day": "{{ ds }}",
        },
    )
