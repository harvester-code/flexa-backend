from typing import Optional

from fastapi import APIRouter, HTTPException

from src.admins.schema import (
    FullResponseModel,
    OutRequestUserInfo,
    OutUserManagement,
    RequestUser,
)
from src.admins.service import AdminService

admins_router = APIRouter()
admins_service = AdminService()


# 사용자 요청 목록 확인
@admins_router.get(
    "/admins/request", response_model=FullResponseModel[OutRequestUserInfo]
)
def fetch_request_user_info(amdin_id: str):
    return admins_service.fetch_request_user_info(amdin_id)


# 사용자 요청 거절
@admins_router.put("/admins/request/reject")
def update_request_deactive(item: RequestUser):

    return admins_service.update_request_deactive(item)


# 사용자 요청 승인
@admins_router.put("/admins/request/approve")
def approve_user(item: RequestUser):

    if item.admin_id is None or item.user_permissions is None:
        raise HTTPException(status_code=404, detail="Not found item")

    response = admins_service.update_request_deactive(item)

    user_id = response["message"]["result"]["user_id"]
    permission_result = admins_service.update_user_permission(item, user_id)

    return admins_service.approve_user_sign_up(item, permission_result)


# 사용자 관리
@admins_router.get("/admins/user", response_model=FullResponseModel[OutUserManagement])
def fetch_user_management(admin_id: str):
    return admins_service.fetch_user_management(admin_id)


# 사용자 권한 수정
@admins_router.put("/admins/user/edit")
def update_user_permission(item: RequestUser, user_id: Optional[str] = None):

    return admins_service.update_user_permission(item, user_id)


# 사용자 비활성화
@admins_router.put("/admins/user/deactive")
def deactive_user(user_email):

    return admins_service.deactive_user(user_email)
