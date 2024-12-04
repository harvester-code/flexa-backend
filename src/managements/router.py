from fastapi import APIRouter
from typing import Optional
from src.managements.service import ManagementService
from src.managements.schema import (
    FullResponseModel,
    UpdateFileName,
    OutFilterList,
    OutSimulationList,
)

managements_router = APIRouter()
management_service = ManagementService()


# 시뮬레이션 정보
@managements_router.get(
    "/managements/simulation", response_model=FullResponseModel[OutSimulationList]
)
def fetch_simulation_info(user_id: str, simulated_at: list = None):

    return management_service.fetch_simulation_info(user_id, simulated_at)


# 필터 정보
@managements_router.get(
    "/managements/filter", response_model=FullResponseModel[OutFilterList]
)
def fetch_filter_info(
    user_id: str,
    page: Optional[str] = None,
    category: Optional[str] = None,
    terminal: Optional[str] = None,
):

    return management_service.fetch_filter_info(user_id, page, category, terminal)


# 파일 삭제
@managements_router.delete("/managements/file")
def delete_file(category: str, name: str):

    return management_service.delete_file(category, name)


# 파일 이름 업데이트
@managements_router.put("/managements/file/name")
def update_file_name(item: UpdateFileName):

    return management_service.update_file_name(item)


# 파일 이동
@managements_router.put("/managements/file/move")
def move_file(item):

    return
