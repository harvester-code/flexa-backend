from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.libs.containers import Container
from app.libs.exceptions import BadRequestException
from app.routes.admin.application.service import AdminService
from app.routes.admin.interface.schema import (
    CreateOperationSettingBody,
    UpdateGroupNameBody,
    UpdateOperationSettingBody,
)
from packages.database import aget_supabase_session
from packages.response import SuccessResponse

admin_router = APIRouter(prefix="/admins")

"""
status 코드 정리
200: 요청이 성공적으로 처리되었고, 응답 본문에 업데이트된 데이터를 포함할 경우
201: 새로운 리소스를 생성할 때 사용
204: 요청이 성공적으로 처리되었지만, 응답 본문이 필요 없을 경우
400: 요청이 올바르지 않을 경우
"""


@admin_router.get(
    "/operation-settings/group-id/{group_id}",
    status_code=status.HTTP_200_OK,
    summary="운영 설정 조회",
    description="그룹별 운영 설정 정보를 조회합니다. 터미널 구성, 프로세스별 운영 방식, 시설 용량 등 공항 운영에 필요한 모든 설정 정보를 반환합니다.",
)
@inject
async def fetch_operation_setting(
    group_id: str,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not group_id:
        raise BadRequestException("Group ID is required")

    data = await admin_service.fetch_operation_setting(db=db, group_id=group_id)

    return SuccessResponse(status_code=status.HTTP_200_OK, data=data)


@admin_router.post(
    "/operation-settings/group-id/{group_id}",
    status_code=status.HTTP_201_CREATED,
    summary="운영 설정 생성",
    description="새로운 그룹의 운영 설정을 생성합니다. 터미널 이름을 포함한 기본 운영 설정을 초기화하여 새로운 공항 환경을 구성할 수 있습니다.",
)
@inject
async def create_operation_setting(
    group_id: str,
    operation_setting: CreateOperationSettingBody,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not group_id:
        raise BadRequestException("Group ID is required")

    await admin_service.create_operation_setting(
        db=db, group_id=group_id, terminal_name=operation_setting.terminal_name
    )

    return SuccessResponse(status_code=status.HTTP_201_CREATED)


@admin_router.patch(
    "/operation-settings/operation-setting-id/{operation_setting_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="운영 설정 수정",
    description="기존 운영 설정의 상세 정보를 수정합니다. 터미널 이름, 프로세스 구성, 처리 절차, 레이아웃 및 이미지 URL 등 운영에 필요한 모든 설정을 업데이트할 수 있습니다.",
)
@inject
async def update_operation_setting(
    operation_setting_id: str,
    operation_setting: UpdateOperationSettingBody,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not operation_setting_id:
        raise BadRequestException("Operation Setting ID is required")

    await admin_service.update_operation_setting(
        db=db,
        id=operation_setting_id,
        terminal_name=operation_setting.terminal_name,
        terminal_process=operation_setting.terminal_process,
        processing_procedure=operation_setting.processing_procedure,
        terminal_layout=operation_setting.terminal_layout,
        terminal_layout_image_url=operation_setting.terminal_layout_image_url,
    )

    return SuccessResponse(status_code=status.HTTP_204_NO_CONTENT)


@admin_router.patch(
    "/operation-settings/operation-setting-id/{operation_setting_id}/deactivate",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="운영 설정 비활성화",
    description="운영 설정을 소프트 삭제(비활성화)합니다. 데이터는 보존되지만 시스템에서 더 이상 사용되지 않도록 설정하여 안전하게 운영 환경에서 제거할 수 있습니다.",
)
@inject
async def deactivate_operation_setting(
    operation_setting_id: str,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not operation_setting_id:
        raise BadRequestException("Operation Setting ID is required")

    await admin_service.deactivate_operation_setting(
        db=db,
        id=operation_setting_id,
    )

    return SuccessResponse(status_code=status.HTTP_204_NO_CONTENT)


@admin_router.patch(
    "/groups/group-id/{group_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="그룹 공항명 변경",
    description="그룹의 공항명을 변경합니다. 관리 목적으로 공항의 표시명을 업데이트하여 더 명확한 식별과 관리를 가능하게 합니다.",
)
@inject
async def update_group_name(
    group_id: str,
    group_name: UpdateGroupNameBody,
    admin_service: AdminService = Depends(Provide[Container.admin_service]),
    db: AsyncSession = Depends(aget_supabase_session),
):
    if not group_id:
        raise BadRequestException("Group ID is required")

    await admin_service.update_group_name(
        db=db, id=group_id, group_name=group_name.group_name
    )

    return SuccessResponse(status_code=status.HTTP_204_NO_CONTENT)
