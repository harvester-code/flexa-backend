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
