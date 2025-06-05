from typing import Any, Dict, List

from pydantic import BaseModel


class Condition(BaseModel):
    criteria: str
    operator: str
    value: List[str] | str


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
    facility_schedules: List[List[float]]


class Component(BaseModel):
    name: str
    nodes: List[Node]


class SimulationBody(BaseModel):
    data: List[Any]
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]
    components: List[Component]


class FlightScheduleBody(BaseModel):
    airport: str
    date: str
    condition: List[Condition] | None


class PassengerScheduleBody(BaseModel):
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]


class FacilityConnBody(BaseModel):
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]


class RunSimulationBody(BaseModel):
    scenario_id: str
    flight_schedule: FlightScheduleBody
    destribution_conditions: List[destribution_conditions]
    processes: Dict[str, Processes]
    components: List[Component]


class SimulationScenarioBody(BaseModel):
    name: str
    editor: str
    terminal: str
    airport: str | None
    memo: str | None


class ScenarioMetadataBody(BaseModel):
    # 여기서 dict는 jsonb 형태로 supabase에 저장될 예정
    overview: dict | None
    history: List[dict] | None
    flight_schedule: dict | None
    passenger_schedule: dict | None
    processing_procedures: dict | None
    facility_connection: dict | None
    facility_information: dict | None


class ScenarioUpdateBody(BaseModel):
    name: str | None
    memo: str | None


class TotalChartDict(BaseModel):
    process: str
    node: str


class SimulationTotalChartBody(BaseModel):
    total: List[TotalChartDict]


class ScenarioDeactivateBody(BaseModel):
    scenario_ids: List[str]


class DuplicateScenarioBody(BaseModel):
    editor: str


class SetOpeningHoursBody(BaseModel):
    time_unit: int
    facility_schedules: List[List[float]]
