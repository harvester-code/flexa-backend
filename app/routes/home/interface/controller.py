from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from app.libs.dependencies import verify_token
from app.routes.home.application.service import HomeService
from packages.response import SuccessResponse

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
    summary="KPI와 무관한 공통 홈 데이터 (alert_issues, flow_chart, histogram, sankey_diagram)",
)
@inject
def fetch_common_home_data(
    scenario_id: str,
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    result = home_service.fetch_common_home_data(scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/kpi-data/{scenario_id}",
    status_code=200,
    summary="KPI 의존적 홈 데이터 (summary, facility_details)",
)
@inject
def fetch_kpi_home_data(
    scenario_id: str,
    calculate_type: str,
    home_service: HomeService = Depends(Provide[Container.home_service]),
    percentile: int | None = None,
):
    result = home_service.fetch_kpi_home_data(scenario_id, calculate_type, percentile)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/aemos_template/{scenario_id}",
    status_code=200,
    summary="aemos_template를 응답하는 엔드포인트",
)
@inject
def fetch_aemos_template(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    scenario_id: str | None = None,
):
    result = home_service.fetch_aemos_template(scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)
