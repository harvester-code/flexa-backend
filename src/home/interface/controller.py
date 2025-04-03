import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status
from src.containers import Container
from src.home.application.service import HomeService
from src.database import get_boto3_session
from src.response import SuccessResponse

home_router = APIRouter(prefix="/homes")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


@home_router.get(
    "/line-queue/{scenario_id}",
    status_code=200,
    summary="line-queue를 응답하는 엔드포인트",
)
@inject
async def fetch_line_queue(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
):
    result = await home_service.fetch_line_queue(session, scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/summaries/{scenario_id}",
    status_code=200,
    summary="최상단 Summary result list를 응답하는 엔드포인트",
)
@inject
async def fetch_summary(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
    calculate_type: str = "mean",
    percentile: int | None = None,
):
    result = await home_service.fetch_summary(
        session, scenario_id, calculate_type, percentile
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/alert-issues/{scenario_id}",
    status_code=200,
    summary="alert-issues를 응답하는 엔드포인트",
)
@inject
async def fetch_alert_issues(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
):
    result = await home_service.fetch_alert_issues(session, scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/facility-details/{scenario_id}",
    status_code=200,
    summary="facility-details를 응답하는 엔드포인트",
)
@inject
async def fetch_facility_details(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
    calculate_type: str = "mean",
    percentile: int | None = None,
):
    result = await home_service.fetch_facility_details(
        session, scenario_id, calculate_type, percentile
    )
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/charts/flow-chart/{scenario_id}",
    status_code=200,
    summary="flow-chart를 응답하는 엔드포인트",
)
@inject
async def fetch_flow_chart(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
):
    result = await home_service.fetch_flow_chart(session, scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/charts/histogram/{scenario_id}",
    status_code=200,
    summary="histogram를 응답하는 엔드포인트",
)
@inject
async def fetch_histogram(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
):
    result = await home_service.fetch_histogram(session, scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)


@home_router.get(
    "/charts/sankey-diagram/{scenario_id}",
    status_code=200,
    summary="sankey-diagram를 응답하는 엔드포인트",
)
@inject
async def fetch_sankey_diagram(
    home_service: HomeService = Depends(Provide[Container.home_service]),
    session: boto3.Session = Depends(get_boto3_session),
    scenario_id: str | None = None,
):
    result = await home_service.fetch_sankey_diagram(session, scenario_id)
    return SuccessResponse(status_code=status.HTTP_200_OK, data=result)
