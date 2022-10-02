import datetime as dt
import pandas as pd
import requests
import pathlib
import re
import json
import time


def extract_flights(date: dt.date) -> list[json] | None:
    """
    Retrieves all flights for a specific date {date} using the Schiphol API.
    """
    secrets_path = pathlib.Path.cwd() / ".secret" / "api_creds.json"
    with open(secrets_path, 'r') as f:
        api_creds = json.load(f)

    all_flights = []
    url = 'https://api.schiphol.nl/public-flights/flights'
    page_params = {'scheduleDate': date.strftime("%Y-%m-%d")}
    headers = {
        'accept': 'application/json',
        'resourceversion': 'v4',
        'app_id': api_creds['app_id'],
        'app_key': api_creds['app_key'],
    }

    try:
        response = requests.request('GET', url, headers=headers)
    except requests.exceptions.ConnectionError as error:
        print(f'Error when establishing connection: {error}\n')
        return None

    if response.status_code == 200:
        n_pages = re.findall(r'page=(\d+)>; rel=\"last\"', response.headers['Link'])[0]
        for page in range(int(n_pages) + 1):
            page_params['page'] = page
            time.sleep(0.5)
            try:
                response = requests.get(url=url, headers=headers, params=page_params)
            except requests.exceptions.ConnectionError as error:
                print(f'Error at page {page}: {error}\n')
                return None

            if response.status_code == 200:
                flights = response.json()
                for flight in flights['flights']:

                    # Exclude any duplicates (multiple flight operators).
                    if flight["mainFlight"] == flight["flightName"]:
                        all_flights.append(flight)

        return all_flights
    else:
        print(f'HTTP {response.status_code}: {response.text}')
        return None


def extract_capacity() -> dict[dict[str:float]]:
    """
    Retrieves a dictionary of dictionaries for each .csv file using pandas method chaining.
    Also basic transformations. 
    """
    csv_path = pathlib.Path.cwd() / "csv"

    airline_capacity = (
        pd.read_csv(
            csv_path / 'airline_capacity.csv',
            dtype={
                'prefix': str,
                'mainsub': str,
                'airline_cap': float,
            })
        .assign(airline=lambda row: row.apply(
            lambda x: x.prefix + x.mainsub, axis=1))
        .set_index('airline')
        .filter(['airline', 'airline_cap'])
        .to_dict()
    )

    aircraft_capacity = (
        pd.read_csv(
            csv_path / 'aircraft_capacity.csv',
            dtype={
                'mainsub': str,
                'airline_cap': float,
            })
        .set_index('mainsub')
        .to_dict()
    )

    passenger_load_factor = (
        pd.read_csv(
            csv_path / 'passenger_load_factor.csv',
            dtype={
                'code': str,
                'airline': str,
                'plf': float,
            })
        .set_index('code')
        .filter(['code', 'plf'])
        .to_dict()
    )

    return airline_capacity | aircraft_capacity | passenger_load_factor
