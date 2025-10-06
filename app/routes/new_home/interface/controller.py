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
    interval_minutes: int = Query(60, ge=5, le=180),
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    chart_data = await service.get_all_facility_charts(
        scenario_id=scenario_id,
        interval_minutes=interval_minutes,
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=chart_data)


@new_home_router.get(
    "/{scenario_id}/passenger-summary",
    status_code=200,
    summary="승객 분포 요약 데이터",
    description="항공사, 도시, 국가 단위로 승객 수 상위 순위를 제공하는 요약 데이터를 반환합니다.",
)
@inject
async def get_passenger_summary(
    scenario_id: str,
    top_n: int = Query(10, ge=3, le=50),
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    summary = await service.get_passenger_summary(
        scenario_id=scenario_id,
        top_n=top_n,
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=summary)


@new_home_router.get(
    "/{scenario_id}/flight-summary",
    status_code=200,
    summary="항공편 요약 데이터",
    description="시간대, 항공사, 기종별 항공편 및 승객 분포 데이터를 제공합니다.",
)
@inject
async def get_flight_summary(
    scenario_id: str,
    top_n: int = Query(10, ge=3, le=50),
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    summary = await service.get_flight_summary(
        scenario_id=scenario_id,
        top_n=top_n,
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=summary)
