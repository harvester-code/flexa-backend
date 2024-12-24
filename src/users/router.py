from fastapi import APIRouter, HTTPException, Depends
from typing import Annotated
from sqlalchemy.orm import Session
from src.users.schema import RequestAccess, Certification, UserCreate
from src.users.service import UserService
from src.database import context_get_supabase_conn

users_router = APIRouter(prefix="/users")
users_service = UserService()

# TODO: 추후 리다이렉트 URL변경 필요

# TODO: 타임존 고정하는 작업 필요. -> 싱글턴패턴 참고

sessions = Annotated[Session, Depends(context_get_supabase_conn)]


# 이메일 인증
@users_router.post("/certification")
async def create_certification(item: Certification, db: sessions):

    return users_service.create_certification(item=item, db=db)


@users_router.get("/certification")
def fetch_certification(id: str, db: sessions):

    return users_service.fetch_certification(id, db)


# 계정 생성
@users_router.post("/create")
def create_user(item: UserCreate):

    if item.first_name is None or item.last_name is None or item.password is None:
        raise HTTPException(status_code=400, detail="Check Your info")

    return users_service.create_user(item)


# 어드민에게 승인 요청
@users_router.post("/request")
def requset_access(item: RequestAccess, db: sessions):

    return users_service.requset_access(item, db)


# 로그인
@users_router.post("/login")
def login_user(item):

    if item.password is None:
        raise HTTPException(status_code=400, detail="Check Your password")

    return users_service.login_user(item)


# 로그아웃
@users_router.post("/logout")
def logout_users():

    return users_service.logout_user()


# # 초기화면 비밀번호 찾기
# @users_router.post("/password/redirect")
# def redirect_reset_password(item: UserInfo):

#     return users_service.redirect_reset_password(item)
