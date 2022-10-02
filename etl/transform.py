import json
import pandas as pd


def transform(flight_data: list[json], capacity_data: dict[dict[str:float]]) -> pd.DataFrame:
    """
    Transforms the two data sources into one useful .csv file.
    """
    def get_airline(row: pd.Series) -> str:
        """
        Tries to find the prefix IATA code (2 characters) if unavailable.
        """
        n_chars = sum(letter.isalpha() for letter in row["mainFlight"])
        if row["prefixIATA"] is not None:
            return row["prefixIATA"]
        elif n_chars == 2:
            return "".join([c for c in row["mainFlight"] if c.isalpha()])
        else:
            return str(row["mainFlight"][:2])

    def calc_capacity(row: pd.Series) -> float:
        """
        Calculates the aircraft capacity based on the aircraft type and operating airline (if available)
        """
        def combine_iata(str_l: list[str | None]) -> str:
            """
            Converts list of strings to string.
            """
            return "".join(filter(None, str_l))

        airline, iata_main, iata_sub = row["airline"], row["iataMain"], row["iataSub"]
        if combine_iata([airline, iata_main, iata_sub]) in capacity_data["airline_cap"]:
            return capacity_data["airline_cap"][combine_iata([airline, iata_main, iata_sub])]
        elif combine_iata([iata_main, iata_sub]) in capacity_data["aircraft_cap"]:
            return capacity_data["aircraft_cap"][combine_iata([iata_main, iata_sub])]
        else:
            return capacity_data["aircraft_cap"]["AVG"]

    def calc_passenger_load_factor(row: pd.Series) -> float:
        """
        Calculates the passenger load factor (plf) based on service type and airline.
        """
        if row["serviceType"] == "J":
            airline = row["airline"]
            if airline in capacity_data["plf"]:
                return capacity_data["plf"][airline] / 100
            else:
                return capacity_data["plf"]["AVG"] / 100
        elif row["serviceType"] == "C":  # Assumption that charter flights are always fully booked.
            return 1.0
        else:
            return 0.0

    df = (
        pd
        .json_normalize(flight_data)
        .rename(columns={
            "aircraftType.iataMain": "iataMain",
            "aircraftType.iataSub": "iataSub",
            "route.destinations": "destination"})
        .filter([
            "mainFlight", "prefixIATA", "flightNumber", "iataMain", "iataSub", "serviceType", "flightDirection",
            "destination", "terminal", "pier", "gate", "scheduleDateTime", "estimatedLandingTime", "actualLandingTime",
            "lastUpdatedAt"])
        .assign(
            destination=lambda x: x.destination.apply(lambda y: y[0]),  # Solely select first destination.
            airline=lambda x: x.apply(lambda r: get_airline(r), axis=1),
            capacity=lambda x: x.apply(lambda r: calc_capacity(r), axis=1),
            plf=lambda x: x.apply(lambda r: calc_passenger_load_factor(r), axis=1),
            estimatedPassengers=lambda x: x.capacity * x.plf,
        )
    )
    return df
