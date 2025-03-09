import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request, HTTPException
from src.containers import Container
from src.home.application.service import HomeService
from pydantic import BaseModel


class LoginRequest(BaseModel):
    email: str = "yibyungha@gmail.com"
    password: str = "yibyungha1"


home_router = APIRouter(prefix="/homes")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


@home_router.post(
    "/login",
    status_code=200,
    summary="Supabase 로그인",
)
@inject
async def login_supabase(
    login_data: LoginRequest,
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    try:
        return await home_service.login_supabase(login_data.email, login_data.password)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@home_router.get(
    "/supabase-test",
    status_code=200,
    summary="Supabase 연결 테스트",
)
@inject
async def fetch_supabase_data(
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    return await home_service.fetch_supabase_data()


@home_router.get(
    "/simulation-files",
    status_code=200,
    summary="시뮬레이션 파일 목록 조회",
)
@inject
async def fetch_simulation_files(
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    return await home_service.fetch_simulation_files()


@home_router.get(
    "/simulation-files/{file_id}",
    status_code=200,
    summary="선택된 시뮬레이션 파일의 요약 정보 추출",
)
@inject
async def fetch_simulation_summary(
    file_id: str = "simulations/tommie/test.parquet",
    home_service: HomeService = Depends(Provide[Container.home_service]),
):
    return await home_service.fetch_simulation_summary(file_id)
