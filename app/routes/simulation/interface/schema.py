from typing import Any, Dict, List, Optional

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
    승객 스케줄 생성 요청 스키마 - show_up_pax3.json 구조 기반 + 동적 설정

    🚨 settings에 포함되어야 하는 필수 필드들:
    - min_arrival_minutes: int (필수) - 최소 도착 시간 (예: 15)
    - date: str (필수) - 대상 날짜 YYYY-MM-DD (예: "2025-09-03")
    - airport: str (필수) - 출발공항 IATA 코드 (예: "ICN")

    🚨 pax_generation에 포함되는 필드들:
    - rules: 조건별 탑승률 설정 (항공편 레벨)
    - default: 기본 탑승률

    🚨 pax_demographics에 포함되는 필드들:
    - nationality: 국적 분포 설정 (승객 레벨) - 프론트엔드에서 정수로 전송, 백엔드에서 100으로 나눠서 확률로 변환
    - profile: 승객 프로필 분포 설정 (승객 레벨) - 프론트엔드에서 정수로 전송, 백엔드에서 100으로 나눠서 확률로 변환

    🚨 pax_arrival_patterns에 포함되는 필드들:
    - rules: 조건별 도착 시간 패턴 설정 (승객 레벨)
    - default: 기본 도착 시간 패턴

    ⚠️ 모든 필드는 필수입니다. 기본값 제공하지 않습니다.
    """

    settings: Dict[
        str, Any
    ]  # 동적 설정 (date, airport, min_arrival_minutes)
    pax_generation: Dict[str, Any]  # 탑승률 설정 (항공편 레벨)
    pax_demographics: Dict[str, Any]  # nationality, profile 등 인구통계 설정 (승객 레벨)
    pax_arrival_patterns: Dict[str, Any]  # rules, default 도착 패턴 (승객 레벨)


class SimulationScenarioBody(BaseModel):
    name: str
    editor: str | None = None
    terminal: str | None = None
    airport: str | None = None
    memo: str | None = None


class ScenarioUpdateBody(BaseModel):
    name: str | None
    terminal: str | None
    airport: str | None
    memo: str | None


class ScenarioDeactivateBody(BaseModel):
    scenario_ids: List[str]


class ScenarioCopyRequest(BaseModel):
    """시나리오 복사 요청 스키마"""
    name: Optional[str] = None  # 새 시나리오 이름 (선택사항)


class ScenarioCopyResponse(BaseModel):
    """시나리오 복사 응답 스키마"""
    scenario_id: str  # 새로 생성된 시나리오 ID
    name: str
    terminal: str | None
    airport: str | None
    memo: str | None
    message: str = "Scenario copied successfully"


class RunSimulationBody(BaseModel):
    """
    시뮬레이션 실행 요청 스키마 - Lambda SQS 메시지 전송용

    사용자 친화적 입력 구조 (일관성 있는 직접 구조):
    {
        "setting": {
            "airport": "ICN",
            "date": "2025-08-05"
        },
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

    Lambda 전송시 PATH의 scenario_id와 setting이 함께 전송됨:
    {"scenario_id": "UUID", "setting": {...}, "process_flow": [...]}
    """

    setting: Dict[str, Any]  # 시뮬레이션 기본 설정 (airport, date)
    process_flow: List[Dict[str, Any]]  # 공항 프로세스 단계별 설정


class FlightFiltersResponse(BaseModel):
    """
    항공편 필터링 메타데이터 응답 스키마

    Departure/Arrival 모드별 필터 옵션을 제공합니다:
    - departure: ICN에서 출발하는 편들의 필터 (어느 터미널에서 출발? 어디로 가는가?)
    - arrival: ICN에 도착하는 편들의 필터 (어느 터미널에 도착? 어디서 출발?)
    """

    # Request context (for data identification and tracking)
    airport: str  # Airport IATA code (e.g., "ICN")
    date: str  # Target date (YYYY-MM-DD)
    scenario_id: str  # Scenario identifier for tracking and data management

    # Flight data summary
    total_flights: int
    
    # Filter options (먼저 나와야 함)
    filters: Dict[str, Any]  # {"departure": {...}, "arrival": {...}}
    
    # Airlines mapping (나중에 나와야 함)
    airlines: Dict[str, str]  # Airlines mapping: {"KE": "Korean Air", "7C": "Jeju Air"}
