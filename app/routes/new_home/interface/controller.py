from __future__ import annotations

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from app.libs.response import SuccessResponse
from app.routes.new_home.application.service import NewHomeService
from packages.supabase.dependencies import verify_token

new_home_router = APIRouter(
    prefix="/new-homes",
    dependencies=[Depends(verify_token)],
)


@new_home_router.get(
    "/{scenario_id}/dashboard",
    status_code=200,
    summary="신규 홈 대시보드 통합 데이터",
    description="시설, 승객, 항공편 요약 정보를 한 번의 요청으로 반환합니다.",
)
@inject
async def get_dashboard_summary(
    scenario_id: str,
    service: NewHomeService = Depends(Provide[Container.new_home_service]),
):
    data = await service.get_dashboard_summary(scenario_id=scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)
