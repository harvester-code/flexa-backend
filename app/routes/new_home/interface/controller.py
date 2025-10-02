from __future__ import annotations

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Query, status

from app.libs.containers import Container
from app.libs.response import SuccessResponse
from app.routes.new_home.application.service import NewHomeService
from packages.supabase.dependencies import verify_token

new_home_router = APIRouter(
    prefix="/new-homes",
    dependencies=[Depends(verify_token)],
)


@new_home_router.get(
    "/{scenario_id}/facility-charts",
    status_code=200,
    summary="시설별 수요/처리 분석 데이터 전체 조회",
    description="시나리오의 모든 단계와 시설에 대한 시간대별 수요, 처리, 처리능력 데이터를 반환합니다.",
)
@inject
async def get_facility_charts(
    scenario_id: str,
    interval_minutes: int = Query(30, ge=5, le=180),
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    chart_data = await service.get_all_facility_charts(
        scenario_id=scenario_id,
        interval_minutes=interval_minutes,
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=chart_data)
