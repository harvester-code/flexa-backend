from typing import Any, Dict, List

from pydantic import BaseModel


class Condition(BaseModel):
    criteria: str
    operator: str
    value: List[str]


class destribution_conditions(BaseModel):
    index: int
    conditions: List[Condition]
    mean: int
    standard_deviation: int


class PriorityMatrix(BaseModel):
    condition: List[Condition]
    matrix: Dict[str, Dict[str, float]]


class Processes(BaseModel):
    name: str
    nodes: List[str]
    source: str | None
    destination: str | None
    default_matrix: Dict[str, Dict[str, float]] | None
    priority_matrix: list[PriorityMatrix] | None


class Node(BaseModel):
    id: int
    name: str
    facility_count: int
    max_queue_length: int
    facility_schedules: List[List[int]]


class Component(BaseModel):
    name: str
    nodes: List[Node]


class SimulationBody(BaseModel):
    data: List[Any]
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]
    components: List[Component]


class FlightScheduleBody(BaseModel):
    user_id: str | None
    airport: str
    date: str
    first_load: bool
    condition: List[Condition] | None


class PassengerScheduleBody(BaseModel):
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]


class FacilityConnBody(BaseModel):
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]


class RunSimulationBody(BaseModel):
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]
    components: List[Component]


class SimulationScenarioBody(BaseModel):
    simulation_name: str
    terminal: str
    editor: str
    note: str | None


class ScenarioMetadataBody(BaseModel):
    simulation_id: str
    # 여기서 dict는 jsonb 형태로 supabase에 저장될 예정
    overview: dict | None
    history: dict | None
    flight_sch: dict | None
    passenger_sch: dict | None
    passenger_attr: dict | None
    facility_conn: dict | None
    facility_info: dict | None


class ScenarioUpdateBody(BaseModel):
    id: str
    simulation_name: str | None
    note: str | None
