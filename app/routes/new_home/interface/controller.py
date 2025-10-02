from __future__ import annotations

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from app.libs.response import SuccessResponse
from app.routes.new_home.application.service import NewHomeService
from app.routes.new_home.interface.schema import FacilityChartQuery
from packages.supabase.dependencies import verify_token

new_home_router = APIRouter(
    prefix="/new-homes",
    dependencies=[Depends(verify_token)],
)


@new_home_router.get(
    "/{scenario_id}/facilities",
    status_code=200,
    summary="프로세스 단계별 시설 목록 조회",
    description="시나리오의 process_flow 설정을 기반으로 단계별 시설 ID 목록을 반환합니다.",
)
@inject
async def list_facilities(
    scenario_id: str,
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    facilities_by_step = await service.list_available_facilities(scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data={"steps": facilities_by_step})


@new_home_router.get(
    "/{scenario_id}/facility-chart",
    status_code=200,
    summary="시설별 수요/처리 분석 데이터",
    description="특정 단계의 시설에 대해 시간대별 수요, 처리, 처리능력 데이터를 반환합니다.",
)
@inject
async def get_facility_chart(
    scenario_id: str,
    query: FacilityChartQuery = Depends(FacilityChartQuery),
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    chart_data = await service.get_facility_chart(
        scenario_id=scenario_id,
        step_name=query.step,
        facility_id=query.facility_id,
        interval_minutes=query.interval_minutes,
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=chart_data)
