import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request, status
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from src.containers import Container
from src.database import aget_supabase_session, get_boto3_session
from src.facility.application.service import FacilityService
from src.exceptions import BadRequestException

facility_router = APIRouter(prefix="/facilities")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


# @facility_router.get(
#     "/sample",
#     summary="샘플코드",
# )
# @inject
# async def fetch_scenario(
#     # process: str,
#     facility_service: FacilityService = Depends(Provide[Container.facility_service]),
#     db: AsyncSession = Depends(aget_supabase_session),
#     session: boto3.Session = Depends(get_boto3_session),
# ):

#     # await facility_service.test(session=session, process=process)
#     data = await facility_service.fetch_process_list(session=session)

#     return "테스트 성공"


@facility_router.get(
    "/processes/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="최초 시나리오를 불러올 시 process list를 응답하는 엔드포인트",
)
@inject
async def fetch_process_list(
    request: Request,
    scenario_id: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):
    if not scenario_id:
        raise BadRequestException("Scenario ID is required")

    result = await facility_service.fetch_process_list(
        session=session, user_id=request.state.user_id, scenario_id=scenario_id
    )

    return result


@facility_router.get(
    "/kpi-summaries/tables/kpi/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="kpi 테이블을 위한 데이터",
)
@inject
async def fetch_kpi(
    request: Request,
    scenario_id: str,
    process: str,
    func: str | None = "mean",
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):

    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    result = await facility_service.generate_kpi(
        session=session,
        process=process,
        func=func,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
    )

    return result


@facility_router.get(
    "/kpi-summaries/charts/line/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="kpi-summary chart를 위한 데이터",
)
@inject
async def fetch_chart(
    request: Request,
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    result = await facility_service.generate_ks_chart(
        session=session,
        process=process,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
    )

    return result


@facility_router.get(
    "/kpi-summaries/charts/heat-map/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="heat-map을 위한 데이터",
)
@inject
async def fetch_heatmap(
    request: Request,
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    result = await facility_service.generate_heatmap(
        session=session,
        process=process,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
    )

    return result


@facility_router.get(
    "/passenger-analyses/charts/pie/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="pie-chart를 위한 데이터",
)
@inject
async def fetch_pie_chart(
    request: Request,
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    result = await facility_service.generate_pie_chart(
        session=session,
        process=process,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
    )

    return result


@facility_router.get(
    "/passenger-analyses/charts/bar/scenario-id/{scenario_id}",
    status_code=status.HTTP_200_OK,
    summary="passenger-analysis chart를 위한 데이터",
)
@inject
async def fetch_pa_chart(
    request: Request,
    scenario_id: str,
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):
    if not scenario_id or not process:
        raise BadRequestException("Scenario ID and Process is required")

    result = await facility_service.generate_pa_chart(
        session=session,
        process=process,
        user_id=request.state.user_id,
        scenario_id=scenario_id,
    )

    return result
