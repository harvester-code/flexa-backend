import boto3
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, Request, status
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession

from src.containers import Container
from src.database import aget_supabase_session, get_boto3_session
from src.admin.application.service import AdminService
from src.admin.interface.schema import (
    CreateOperationSettingBody,
    UpdateOperationSettingBody,
)

admin_router = APIRouter(prefix="/admins")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


# @admin_router.get(
#     "/sample",
#     summary="샘플코드",
# )
# @inject
# async def fetch_scenario(
#     # process: str,
#     admin_service: AdminService = Depends(Provide[Container.admin_service]),
#     db: AsyncSession = Depends(aget_supabase_session),
#     session: boto3.Session = Depends(get_boto3_session),
# ):

#     # await facility_service.test(session=session, process=process)
#     # data = await admin_service.fetch_process_list(session=session)

#     return "테스트 성공"


@admin_router.get(
    "/operation-settings/group-id/{group_id}",
    status_code=status.HTTP_201_CREATED,
    summary="운영세팅",
)
@inject
async def create_operation_setting(
    group_id: str,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    result = await admin_service.fetch_operation_setting(db=db, group_id=group_id)

    return result


@admin_router.post(
    "/operation-settings/group-id/{group_id}",
    status_code=status.HTTP_201_CREATED,
    summary="운영세팅",
)
@inject
async def fetch_operation_setting(
    group_id: str,
    operation_setting: CreateOperationSettingBody,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    await admin_service.create_operation_setting(
        db=db, group_id=group_id, terminal_name=operation_setting.terminal_name
    )

    return "success"


@admin_router.patch(
    "/operation-settings/operation-setting-id/{operation_setting_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="운영세팅",
)
@inject
async def update_operation_setting(
    operation_setting_id: str,
    operation_setting: UpdateOperationSettingBody,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):

    await admin_service.update_operation_setting(
        db=db,
        id=operation_setting_id,
        terminal_name=operation_setting.terminal_name,
        terminal_process=operation_setting.terminal_process,
        processing_procedure=operation_setting.processing_procedure,
        terminal_layout=operation_setting.terminal_layout,
        terminal_layout_image_url=operation_setting.terminal_layout_image_url,
    )
