import datetime as dt
from etl import extract, transform, load


# Run pipeline without AirFlow.
if __name__ == '__main__':
    query_datetime = dt.datetime.now() - dt.timedelta(days=1)
    load.load(
        df=transform.transform(
            flight_data=extract.extract_flights(query_datetime.date()),
            capacity_data=extract.extract_capacity(),
        ),
        q_dt=query_datetime
    )
