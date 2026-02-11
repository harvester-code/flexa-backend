"""
Home API 응답 스키마 정의

FastAPI 자동 문서화 및 응답 검증을 위한 Pydantic 모델입니다.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ============================================================
# 공용 모델
# ============================================================

class TimeHMS(BaseModel):
    """시:분:초 형식의 시간 데이터"""
    hour: int = 0
    minute: int = 0
    second: int = 0


# ============================================================
# Static Data 응답 (/homes/{scenario_id}/static)
# ============================================================

class HomeStaticResponse(BaseModel):
    """정적 분석 데이터 (KPI와 무관한 시각화 데이터)"""
    flow_chart: Optional[Dict[str, Any]] = None
    histogram: Optional[Dict[str, Any]] = None
    sankey_diagram: Optional[Dict[str, Any]] = None


# ============================================================
# Metrics Data 응답 (/homes/{scenario_id}/metrics)
# ============================================================

class ProcessWaitingTime(BaseModel):
    """프로세스별 대기시간"""
    total: TimeHMS = Field(default_factory=TimeHMS)
    open_wait: TimeHMS = Field(default_factory=TimeHMS)
    queue_wait: TimeHMS = Field(default_factory=TimeHMS)


class PaxExperience(BaseModel):
    """승객 경험 데이터"""
    waiting_time: Dict[str, ProcessWaitingTime] = Field(default_factory=dict)
    queue_length: Dict[str, int] = Field(default_factory=dict)


class TimeMetrics(BaseModel):
    """시간 메트릭 (대기시간 요약)"""
    open_wait: TimeHMS = Field(default_factory=TimeHMS)
    queue_wait: TimeHMS = Field(default_factory=TimeHMS)
    total_wait: TimeHMS = Field(default_factory=TimeHMS)
    process_time: TimeHMS = Field(default_factory=TimeHMS)


class DwellTimes(BaseModel):
    """체류시간"""
    commercial_dwell_time: TimeHMS = Field(default_factory=TimeHMS)
    airport_dwell_time: TimeHMS = Field(default_factory=TimeHMS)


class FacilityMetric(BaseModel):
    """시설 메트릭 (효율성)"""
    process: str
    operating_rate: float = 0.0
    utilization_rate: float = 0.0
    total_rate: float = 0.0
    zones: Optional[Dict[str, Any]] = None


class PassengerSummary(BaseModel):
    """승객 요약"""
    total: int = 0
    completed: int = 0
    missed: int = 0


class GdpData(BaseModel):
    """GDP 데이터"""
    formatted: str
    year: int


class AirportContext(BaseModel):
    """공항 경제 컨텍스트"""
    country_name: str
    gdp_ppp: Optional[GdpData] = None
    gdp: Optional[GdpData] = None


class EconomicImpact(BaseModel):
    """경제적 영향"""
    total_wait_value: float = 0.0
    process_time_value: float = 0.0
    commercial_dwell_value: float = 0.0
    airport_context: Optional[AirportContext] = None


class HomeSummaryData(BaseModel):
    """메트릭 요약 데이터"""
    pax_experience: Optional[PaxExperience] = None
    timeMetrics: Optional[TimeMetrics] = None
    dwellTimes: Optional[DwellTimes] = None
    facility_metrics: Optional[List[FacilityMetric]] = None
    passenger_summary: Optional[PassengerSummary] = None
    economic_impact: Optional[EconomicImpact] = None


# ============================================================
# Facility Details 응답
# ============================================================

class FacilityDetailComponent(BaseModel):
    """시설 세부 컴포넌트"""
    title: str = ""
    throughput: int = 0
    queuePax: int = 0
    waitTime: Optional[TimeHMS] = None
    facility_effi: float = 0.0
    workforce_effi: float = 0.0
    opened: Optional[List[int]] = None


class FacilityDetailOverview(BaseModel):
    """시설 세부 카테고리 개요"""
    throughput: int = 0
    queuePax: int = 0
    waitTime: Optional[TimeHMS] = None
    facility_effi: float = 0.0
    workforce_effi: float = 0.0
    opened: Optional[List[int]] = None


class FacilityDetailCategory(BaseModel):
    """시설 세부 카테고리"""
    category: str
    overview: FacilityDetailOverview = Field(default_factory=FacilityDetailOverview)
    components: List[FacilityDetailComponent] = Field(default_factory=list)


class HomeMetricsResponse(BaseModel):
    """KPI 메트릭 데이터 응답"""
    summary: HomeSummaryData = Field(default_factory=HomeSummaryData)
    facility_details: List[FacilityDetailCategory] = Field(default_factory=list)
