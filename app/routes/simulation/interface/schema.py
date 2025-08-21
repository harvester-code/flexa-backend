from typing import Any, Dict, List

from pydantic import BaseModel


class Condition(BaseModel):
    criteria: str
    operator: str
    value: List[str] | str


class FilterCondition(BaseModel):
    criteria: str  # types, terminal, airline
    value: List[str]


class destribution_conditions(BaseModel):
    index: int
    name: str | None = None  # 그룹 이름 (optional)
    conditions: List[Condition]
    mean: int
    standard_deviation: int



class FlightScheduleBody(BaseModel):
    airport: str
    date: str
    condition: List[FilterCondition] | None


class PassengerScheduleBody(BaseModel):
    destribution_conditions: List[destribution_conditions]


class SimulationScenarioBody(BaseModel):
    name: str
    editor: str
    terminal: str
    airport: str | None
    memo: str | None


class ScenarioUpdateBody(BaseModel):
    name: str | None
    terminal: str | None
    airport: str | None
    memo: str | None


class ScenarioDeactivateBody(BaseModel):
    scenario_ids: List[str]
