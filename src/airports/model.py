from datetime import date

from sqlmodel import Field, SQLModel


class GeneralDeclarationDeparture(SQLModel, table=True):
    __tablename__ = "FLIGHTS_EXTENDED"

    FLIGHT_ID: str = Field(primary_key=True)
    FLIGHT_NUMBER: str = Field()
    FLIGHT_DATE_UTC: date = Field()
    FLEET_AIRCRAFT_ID: str = Field()
    OPERATING_CARRIER_IATA: str = Field()
    OPERATING_CARRIER_ID: str = Field()
    OPERATING_CARRIER_NAME: str = Field()
    TAIL_NUMBER: str = Field()
    ACTUAL_TAXI_OUT_TIME: str = Field()
    ACTUAL_TAXI_IN_TIME: str = Field()
    BAGGAGE_CLAIM: str = Field()
    ACTUAL_FLIGHT_DURATION: str = Field()
    ACTUAL_BLOCK_TIME: str = Field()
    AIRCRAFT_TYPE: str = Field()
    AIRCRAFT_TYPE_SERIES: str = Field()
    AIRCRAFT_CODE_IATA: str = Field()
    AIRCRAFT_CODE_ICAO: str = Field()
    AIRCRAFT_SERIAL_NUMBER: str = Field()
    TOTAL_SEAT_COUNT: str = Field()
    IS_CANCELLED: str = Field()
    IS_DIVERTED: str = Field()

    DEPARTURE_TERMINAL: str = Field()
    DEPARTURE_GATE: str = Field()
    DEPARTURE_AIRPORT_ID: str = Field()
    DEPARTURE_AIRPORT_IATA: str = Field()
    SCHEDULED_GATE_DEPARTURE_LOCAL: str = Field()
    ACTUAL_GATE_DEPARTURE_LOCAL: str = Field()
    ACTUAL_RUNWAY_DEPARTURE_LOCAL: str = Field()
    GATE_DEPARTURE_DELAY: str = Field()
