from typing import Any, Dict, List

from pydantic import BaseModel


class FilterCondition(BaseModel):
    field: str  # types, terminal, airline
    values: List[str]


class FlightScheduleBody(BaseModel):
    airport: str
    date: str
    type: str  # "departure" or "arrival"
    conditions: List[FilterCondition] | None


class PassengerScheduleBody(BaseModel):
    """
    승객 스케줄 생성 요청 스키마 - pax_simple.json 구조 기반 + 동적 설정

    🚨 settings에 포함되어야 하는 필수 필드들:
    - load_factor: float (필수) - 탑승률 (예: 0.85)
    - min_arrival_minutes: int (필수) - 최소 도착 시간 (예: 15)
    - target_date: str (필수) - 대상 날짜 YYYY-MM-DD (예: "2025-08-05")
    - departure_airport: str (필수) - 출발공항 IATA 코드 (예: "ICN")

    ⚠️ 모든 필드는 필수입니다. 기본값 제공하지 않습니다.
    """

    settings: Dict[
        str, Any
    ]  # 동적 설정 (load_factor, target_date, departure_airport 등)
    pax_demographics: Dict[str, Any]  # nationality, profile 등 인구통계 설정
    pax_arrival_patterns: Dict[str, Any]  # rules, default 도착 패턴


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


class RunSimulationBody(BaseModel):
    """
    시뮬레이션 실행 요청 스키마 - Lambda SQS 메시지 전송용

    사용자 친화적 입력 구조 (일관성 있는 직접 구조):
    {
        "process_flow": [
            {
                "step": 0,
                "name": "visa_check",
                "travel_time_minutes": 5,
                "zones": {...}
            },
            {
                "step": 1,
                "name": "checkin",
                "travel_time_minutes": 10,
                "zones": {...}
            }
        ]
    }

    Lambda 전송시 자동으로 scenario_id와 함께 포장됨:
    {"scenario_id": "UUID", "process_flow": [...]}
    """

    process_flow: List[Dict[str, Any]]  # 공항 프로세스 단계별 설정
