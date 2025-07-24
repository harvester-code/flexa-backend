from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from app.libs.exceptions import BadRequestException
from app.routes.facility.application.service import FacilityService
from packages.response import SuccessResponse

facility_router = APIRouter(prefix="/facilities")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


@facility_router.get(
    "/processes/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="시나리오별 프로세스 목록 조회",
    description="특정 시나리오에서 사용 가능한 모든 프로세스(체크인, 보안검색, 출입국심사 등)의 목록을 조회합니다. 각 프로세스의 기본 정보와 설정 가능한 옵션들을 포함하여 반환합니다.",
)
@inject
async def fetch_process_list(
    scenario_id: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    data = await facility_service.fetch_process_list(scenario_id=scenario_id)

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@facility_router.get(
    "/kpi-summaries/tables/kpi/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="KPI 테이블 데이터 조회",
    description="시나리오의 특정 프로세스에 대한 KPI 테이블 데이터를 조회합니다. 평균, 최대, 최소, 중앙값, 상위/하위 5개 등 다양한 통계 계산 방식(max, min, median, mean, top5, bottom5)을 지원하며, 백분위수 기반 분석도 가능합니다.",
)
@inject
async def fetch_kpi(
    scenario_id: str,
    process: str,
    calculate_type: str = "mean",
    percentile: int | None = None,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):

    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    data = await facility_service.generate_kpi(
        process=process,
        scenario_id=scenario_id,
        calculate_type=calculate_type,
        percentile=percentile,
    )

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@facility_router.get(
    "/kpi-summaries/charts/line/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="KPI 라인 차트 데이터 조회",
    description="시나리오의 특정 프로세스에 대한 KPI 라인 차트 데이터를 생성합니다. 시간대별 성능 지표 변화를 시각화할 수 있는 차트 데이터를 제공하여 프로세스 성능의 시간적 패턴을 분석할 수 있습니다.",
)
@inject
async def fetch_chart(
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    data = await facility_service.generate_ks_chart(
        process=process, scenario_id=scenario_id
    )

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@facility_router.get(
    "/kpi-summaries/charts/heat-map/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="KPI 히트맵 차트 데이터 조회",
    description="시나리오의 특정 프로세스에 대한 KPI 히트맵 차트 데이터를 생성합니다. 시간대와 위치별 성능 지표를 2차원 그리드로 시각화하여 병목 구간과 최적화 지점을 직관적으로 파악할 수 있습니다.",
)
@inject
async def fetch_heatmap(
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    data = await facility_service.generate_heatmap(
        process=process, scenario_id=scenario_id
    )

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@facility_router.get(
    "/passenger-analyses/charts/pie/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="승객 분석 파이 차트 데이터 조회",
    description="시나리오의 특정 프로세스에 대한 승객 분석 파이 차트 데이터를 생성합니다. 승객 유형별, 항공편별, 경로별 분포를 원형 차트로 시각화하여 승객 구성비와 특성을 한눈에 파악할 수 있습니다.",
)
@inject
async def fetch_pie_chart(
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    data = await facility_service.generate_pie_chart(
        process=process, scenario_id=scenario_id
    )

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@facility_router.get(
    "/passenger-analyses/charts/bar/scenario-id/{scenario_id}",
    response_model=SuccessResponse,
    status_code=status.HTTP_200_OK,
    summary="승객 분석 바 차트 데이터 조회",
    description="시나리오의 특정 프로세스에 대한 승객 분석 바 차트 데이터를 생성합니다. 시간대별, 구간별 승객 처리량과 대기시간을 막대 차트로 시각화하여 승객 흐름의 패턴과 피크 시간대를 분석할 수 있습니다.",
)
@inject
async def fetch_pa_chart(
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    data = await facility_service.generate_pa_chart(
        process=process, scenario_id=scenario_id
    )

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)
