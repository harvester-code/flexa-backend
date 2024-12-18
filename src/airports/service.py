from json import loads
from typing import Annotated

import pandas as pd
from fastapi import Depends
from sqlalchemy import Engine

from src.airports.queries import SELECT_AIRPORT_ARRIVAL, SELECT_AIRPORT_DEPARTURE
from src.database import get_snowflake_session

SessionDep = Annotated[Engine, Depends(get_snowflake_session)]


class AirportService:
    @staticmethod
    def fetch_general_declarations(date, airport, flight_io, session: SessionDep):
        with session.connect() as connection:
            if flight_io == "arrival":
                df = pd.read_sql(
                    SELECT_AIRPORT_ARRIVAL.format(airport=airport, date=date),
                    connection,
                )

            if flight_io == "departure":
                df = pd.read_sql(
                    SELECT_AIRPORT_DEPARTURE.format(airport=airport, date=date),
                    connection,
                )

        result = loads(df.to_json(orient="records"))
        return result
