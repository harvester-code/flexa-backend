import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from src.containers import Container
from src.database import aget_supabase_session, get_boto3_session
from src.home.application.service import HomeService

home_router = APIRouter(prefix="/homes")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


@home_router.get(
    "/sample",
    status_code=200,
    summary="샘플코드",
)
@inject
async def fetch_scenario(
    request: Request,
    home_service: HomeService = Depends(Provide[Container.home_service]),
    db: AsyncSession = Depends(aget_supabase_session),
    session: boto3.Session = Depends(get_boto3_session),
):

    # user_id = request.state.user_id
    # await home_service.test_method(db, session, user_id, ...)

    return "테스트 성공"
