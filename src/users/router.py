from fastapi import APIRouter, HTTPException

from src.users.schema import RequestAccess, UserInfo
from src.users.service import UserService

users_router = APIRouter()
users_service = UserService()

# TODO: 추후 리다이렉트 URL변경 필요

# TODO: 타임존 고정하는 작업 필요. -> 싱글턴패턴 참고


# 이메일 인증
@users_router.post("/users/certification")
def create_certification(item: UserInfo):

    return users_service.create_certification(item)


@users_router.get("/users/certification")
def fetch_certification(id: str):

    return users_service.fetch_certification(id)


# 계정 생성
@users_router.post("/users/create")
def create_user(item: UserInfo):

    if item.first_name is None or item.last_name is None or item.password is None:
        raise HTTPException(status_code=400, detail="Check Your info")

    return users_service.create_user(item)


# 어드민에게 승인 요청
@users_router.post("/users/request")
def requset_access(item: RequestAccess):

    return users_service.requset_access(item)


# 로그인
@users_router.post("/users/login")
def login_user(item: UserInfo):

    if item.password is None:
        raise HTTPException(status_code=400, detail="Check Your password")

    return users_service.login_user(item)


# 로그아웃
@users_router.post("/users/logout")
def logout_users():

    return users_service.logout_user()


# # 초기화면 비밀번호 찾기
# @users_router.post("/users/password/redirect")
# def redirect_reset_password(item: UserInfo):

#     return users_service.redirect_reset_password(item)
