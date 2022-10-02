import json
import logging
import datetime as dt
import pandas as pd
from airflow.decorators import dag, task
from etl import extract, transform, load


@dag(start_date=dt.datetime(2022, 10, 1), schedule_interval="@daily", catchup=False)
def flights_pipeline() -> None:
    """
    Complete ETL Airflow Pipeline (DAG).
    """
    @task
    def extract_flights_task(date: dt.date) -> list[json]:
        logging.info(f"Extracting all Schiphol flights on {date.strftime('%d-%m-&Y')}...")
        return extract.extract_flights(date)

    @task
    def extract_capacity_task() -> dict[dict[str:float]]:
        logging.info("Extracting flight capacity...")
        return extract.extract_capacity()

    @task
    def transform_task(flight_data: list[json], capacity_data: dict[dict[str:float]]) -> pd.DataFrame:
        logging.info("Transforming extracted data to dataframe...")
        return transform.transform(flight_data, capacity_data)

    @task
    def load_task(df: pd.DataFrame, q_dt: dt.datetime) -> None:
        logging.info("Uploading dataframe to AWS S3 bucket...")
        return load.load(df, q_dt)

    query_datetime = dt.datetime.now() - dt.timedelta(days=1)
    load_task(
        df=transform_task(
            flight_data=extract_flights_task(query_datetime.date()),
            capacity_data=extract_capacity_task()),
        date=query_datetime
    )


dag = flights_pipeline()
