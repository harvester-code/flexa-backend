from __future__ import annotations

from typing import List

from pydantic import BaseModel, ConfigDict, Field


class FacilityChartQuery(BaseModel):
    step: str = Field(..., description="분석할 프로세스 단계 이름")
    facility_id: str = Field(..., alias="facilityId", description="분석 대상 시설 ID")
    interval_minutes: int = Field(
        30,
        alias="intervalMinutes",
        ge=5,
        le=180,
        description="시간 간격(분)",
    )

    model_config = ConfigDict(populate_by_name=True)


class FacilitiesResponse(BaseModel):
    facilities: List[str]
    step: str
