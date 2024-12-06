from datetime import date, datetime
from typing import Optional

from sqlmodel import Column, Field, SQLModel


class GeneralDeclaration(SQLModel, table=True):
    __tablename__ = "SIM_GD_TABLE"

    flight_id: Optional[int] = Field(None, primary_key=True)
    origin_airport: Optional[str] = Field(None)
    dep_arr_airport: Optional[str] = Field(None, sa_column=Column("dep/arr_airport"))
    flight_distance_km: Optional[float] = Field(None)
    total_seat_count: Optional[float] = Field(None)
    flight_io: Optional[str] = Field(None)
    movement: Optional[int] = Field(None)
    baggage_claim: Optional[str] = Field(None)
    scheduled_gate_local: Optional[datetime] = Field(None)
    actual_gate_local: Optional[datetime] = Field(None)
    actual_runway_local: Optional[datetime] = Field(None)
    operating_carrier_id: Optional[str] = Field(None)
    operating_carrier_name: Optional[str] = Field(None)
    operating_carrier_iata: Optional[str] = Field(None)
    flight_number: Optional[str] = Field(None)
    tail_number: Optional[str] = Field(None)
    aircraft_serial_number: Optional[str] = Field(None)
    terminal: Optional[str] = Field(None)
    gate: Optional[str] = Field(None)
    actual_taxi_time: Optional[float] = Field(None)
    actual_block_time: Optional[float] = Field(None)
    gate_delay: Optional[float] = Field(None)
    actual_flight_duration: Optional[float] = Field(None)
    standing_time: Optional[float] = Field(None)
    ground_time: Optional[float] = Field(None)
    is_turnaround: Optional[float] = Field(None)
    aircraft_market_sector: Optional[str] = Field(None)
    aircraft_class: Optional[str] = Field(None)
    aircraft_market_group: Optional[str] = Field(None)
    aircraft_family: Optional[str] = Field(None)
    aircraft_type: Optional[str] = Field(None)
    aircraft_type_series: Optional[str] = Field(None)
    aircraft_code_iata: Optional[str] = Field(None)
    aircraft_code_icao: Optional[str] = Field(None)
    primary_usage: Optional[str] = Field(None)
    aircraft_start_of_life_date: Optional[date] = Field(None)
    aircraft_age_months: Optional[float] = Field(None)
    operating_maximum_takeoff_weight_lb: Optional[float] = Field(None)
    maximum_landing_weight_lb: Optional[float] = Field(None)
    maximum_payload_lb: Optional[float] = Field(None)
    country_code: Optional[str] = Field(None)
    international_domestic: Optional[str] = Field(
        None, sa_column=Column("International/Domestic")
    )
