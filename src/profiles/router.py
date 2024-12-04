from fastapi import APIRouter, Depends
from src.profiles.schema import (
    FullResponseModel,
    InUserInfo,
    OutUserInfo,
    OutUserHistory,
)
from src.profiles.service import ProfilesService

profiles_router = APIRouter()
profiles_service = ProfilesService()


# 사용자 프로필 정보
@profiles_router.get("/profiles/user", response_model=FullResponseModel[OutUserInfo])
def fetch_user_info(user_id):

    return profiles_service.fetch_user_info(user_id)


# 사용자 프로필 정보 업데이트
@profiles_router.put("/profiles/user")
def update_user_info_profiles(item: InUserInfo):

    return profiles_service.update_user_info(item)


# 사용자 접속 정보
@profiles_router.get(
    "/profiles/session", response_model=FullResponseModel[OutUserHistory]
)
def fetch_user_login_history(user_id):

    return profiles_service.fetch_user_login_history(user_id)


# 사용자 프로필 이미지 업로드
@profiles_router.post("/profiles/image")
async def upload_image(image: dict = Depends(profiles_service.upload_image)):

    return image
