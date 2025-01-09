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
    processes: Dict[str, processes]
    components: List[Component]
