import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request, status
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from src.containers import Container
from src.database import aget_supabase_session, get_boto3_session
from src.facility.application.service import FacilityService

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
#     process: str,
#     facility_service: FacilityService = Depends(Provide[Container.facility_service]),
#     db: AsyncSession = Depends(aget_supabase_session),
#     session: boto3.Session = Depends(get_boto3_session),
# ):

#     await facility_service.test(session=session, process=process)

#     return "테스트 성공"


@facility_router.get(
    "/kpi",
    status_code=status.HTTP_200_OK,
    summary="kpi 테이블을 위한 데이터",
)
@inject
async def fetch_kpi(
    process: str,
    func: str | None = "mean",
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):

    result = await facility_service.generate_kpi(
        session=session, process=process, func=func
    )

    return result


@facility_router.get(
    "/chart",
    status_code=status.HTTP_200_OK,
    summary="chart를 위한 데이터",
)
@inject
async def fetch_chart(
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):

    result = await facility_service.generate_chart(session=session, process=process)

    return result


@facility_router.get(
    "/heat-map",
    status_code=status.HTTP_200_OK,
    summary="heat-map을 위한 데이터",
)
@inject
async def fetch_heatmap(
    process: str,
    facility_service: FacilityService = Depends(Provide[Container.facility_service]),
    session: boto3.Session = Depends(get_boto3_session),
):

    result = await facility_service.generate_heatmap(session=session, process=process)

    return result
