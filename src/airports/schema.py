from datetime import date, datetime
from typing import Any, Dict, List

from pydantic import BaseModel


class GeneralDeclarationBase(BaseModel):
    actual_block_time: int | None = None
    actual_flight_duration: int | None = None
    actual_taxi_in_time: int | None = None
    actual_taxi_out_time: int | None = None
    aircraft_code_iata: str | None = None
    aircraft_code_icao: str | None = None
    aircraft_serial_number: str | None = None
    aircraft_type: str | None = None
    aircraft_type_series: str | None = None
    baggage_claim: str | None = None
    country_code: str | None = None
    fleet_aircraft_id: int | None = None
    flight_date_utc: date | None = None
    flight_id: int | None = None
    flight_number: str | None = None
    flight_type: str | None = None
    is_cancelled: bool | None = None
    is_diverted: bool | None = None
    operating_carrier_iata: str | None = None
    operating_carrier_id: str | None = None
    operating_carrier_name: str | None = None
    region_name: str | None = None
    tail_number: str | None = None
    total_seat_count: int | None = None


# NOTE: 컬럼 이름을 통일할 수 없을까?
class GeneralDeclarationArrival(GeneralDeclarationBase):
    actual_gate_arrival_local: datetime | None = None
    actual_runway_arrival_local: datetime | None = None
    arrival_airport_iata: str | None = None
    arrival_airport_id: str | None = None
    arrival_gate: str | None = None
    arrival_terminal: str | None = None
    gate_arrival_delay: int | None = None
    scheduled_gate_arrival_local: datetime | None = None


# NOTE: 컬럼 이름을 통일할 수 없을까?
class GeneralDeclarationDeparture(GeneralDeclarationBase):
    actual_gate_departure_local: datetime | None = None
    actual_runway_departure_local: datetime | None = None
    departure_airport_iata: str | None = None
    departure_airport_id: str | None = None
    departure_gate: str | None = None
    departure_terminal: str | None = None
    gate_departure_delay: int | None = None
    scheduled_gate_departure_local: datetime | None = None


# ==================================
class Condition(BaseModel):
    criteria: str
    operator: str
    value: List[str]


class destribution_conditions(BaseModel):
    index: int
    conditions: List[Condition]
    mean: int
    standard_deviation: int


class PriorityMatricx(BaseModel):
    condition: List[Condition]
    matricx: Dict[str, Dict[str, float]]


class processes(BaseModel):
    name: str
    nodes: List[str]
    source: str | None
    destination: str | None
    default_matricx: Dict[str, Dict[str, float]] | None
    priority_matricx: list[PriorityMatricx] | None


# ==================================
class ShowupBody(BaseModel):
    # FIXME: data -> 실제 데이터 스키마로 변경필요
    data: List[Any]
    destribution_conditions: List[destribution_conditions]


class ChoiceMatricxBody(BaseModel):
    # FIXME: data -> 실제 데이터 스키마로 변경필요
    data: List[Any]
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, processes]
