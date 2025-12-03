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
