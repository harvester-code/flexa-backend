from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from packages.supabase.dependencies import verify_token
from app.routes.home.application.service import HomeService
from app.libs.response import SuccessResponse

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""

home_router = APIRouter(
    prefix="/homes",
    dependencies=[Depends(verify_token)],
)


@home_router.get(
    "/common-data/{scenario_id}",
    status_code=200,
    summary="홈 공통 데이터 조회",
    description="시나리오의 홈 화면에 표시할 공통 데이터를 조회합니다. 알림 이슈, 플로우 차트, 히스토그램, 상키 다이어그램 등 KPI 계산과 무관한 기본적인 시각화 데이터를 제공합니다.",
)
@inject
async def fetch_common_home_data(
    scenario_id: str,
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    result = await home_service.fetch_common_home_data(scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/kpi-data/{scenario_id}",
    status_code=200,
    summary="홈 KPI 데이터 조회",
    description="시나리오의 홈 화면에 표시할 KPI 관련 데이터를 조회합니다. 시나리오 요약 정보와 시설별 상세 성능 지표를 포함하며, 다양한 통계 계산 방식과 백분위수 기반 분석을 지원합니다.",
)
@inject
async def fetch_kpi_home_data(
    scenario_id: str,
    calculate_type: str,
    home_service: HomeService = Depends(Provide[Container.home_service]),
    percentile: int | None = None,
):
    result = await home_service.fetch_kpi_home_data(
        scenario_id, calculate_type, percentile
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/aemos-template/{scenario_id}",
    status_code=200,
    summary="aemos-template를 응답하는 엔드포인트",
)
@inject
async def fetch_aemos_template(
    scenario_id: str,
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    result = await home_service.fetch_aemos_template(scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)
