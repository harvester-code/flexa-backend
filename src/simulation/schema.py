from typing import Any, Dict, List

from pydantic import BaseModel


class Condition(BaseModel):
    criteria: str
    operator: str
    value: List[str]


class Filter(BaseModel):
    index: int
    conditions: List[Condition]
    mean: int
    standard_deviation: int


class MatrixFilter(BaseModel):
    index: int
    # 여기에 원래는 edited_df가 들어와야한다.
    conditions: List[str]
    matricx: Dict[str, Dict[str, float]]


class Facility(BaseModel):
    name: str
    nodes: List[str]
    filters: List[MatrixFilter]
    source: str | None
    destination: str | None


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
    filters: List[Filter]
    facility_detail: Dict[str, Facility]
    components: List[Component]
