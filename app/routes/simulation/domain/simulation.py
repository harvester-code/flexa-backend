from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class ScenarioInformation:
    id: Optional[int]
    user_id: str
    editor: str
    name: str
    terminal: str
    airport: str | None
    memo: str | None
    target_flight_schedule_date: str | None
    created_at: datetime
    updated_at: datetime
    scenario_id: str


@dataclass
class ScenarioMetadata:
    scenario_id: str
    # 여기서 dict는 jsonb 형태로 supabase에 저장될 예정
    overview: dict | None
    history: List[dict] | None
    flight_schedule: dict | None
    passenger_schedule: dict | None
    processing_procedures: dict | None
    facility_connection: dict | None
    facility_information: dict | None
