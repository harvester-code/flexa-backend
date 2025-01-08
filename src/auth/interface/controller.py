from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from supabase._async.client import AsyncClient as Client

from src.auth.application.service import AuthService
from src.auth.interface.schema import (
    CreateCertificationBody,
    CreateUserBody,
    CreateUserAccessRequestBody,
    LoginUser,
    ResetPassword,
)
from src.containers import Container
from src.database import (
    aget_supabase_client,
    aget_supabase_auth_client,
    aget_supabase_session,
)

auth_router = APIRouter(prefix="/auth_test")


# 이메일 인증 생성
@auth_router.post("/certification", status_code=201)
@inject
async def create_certification(
    certification: CreateCertificationBody,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await auth_service.create_certification(db, certification.email)


# 이메일 인증 조회
@auth_router.get("/certification", status_code=200)
@inject
async def fetch_certification(
    id: str,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    return await auth_service.fetch_certification(db, id)


# 사용자 승인 요청
@auth_router.post("/access", status_code=201)
@inject
async def create_user_access_request(
    access: CreateUserAccessRequestBody,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    return await auth_service.create_user_access_request(
        db, access.user_email, access.admin_email, access.request_mg
    )


# 사용자 회원가입
@auth_router.post("/user/create", status_code=201)
@inject
async def create_user(
    CreateUserBody: CreateUserBody,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    sb: Client = Depends(aget_supabase_client),
):

    return await auth_service.create_user(
        sb,
        CreateUserBody.email,
        CreateUserBody.first_name,
        CreateUserBody.last_name,
        CreateUserBody.password,
    )


# 사용자 로그인
@auth_router.post("/user/login", status_code=201)
@inject
async def login_user(
    login: LoginUser,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    db: AsyncSession = Depends(aget_supabase_session),
    sb: Client = Depends(aget_supabase_client),
):
    return await auth_service.login_user(db, sb, login.email, login.password)


# 사용자 로그아웃
@auth_router.post("/user/logout", status_code=201)
@inject
async def logout_user(
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    sb: Client = Depends(aget_supabase_client),
):
    return await auth_service.logout_user(sb)


# 사용자 비밀번호 변경
@auth_router.post("/user/reset_password", status_code=201)
@inject
async def reset_password(
    reset: ResetPassword,
    auth_service: AuthService = Depends(Provide[Container.auth_service]),
    sb: Client = Depends(aget_supabase_auth_client),
):
    return await auth_service.reset_password(sb, reset.user_id, reset.password)
