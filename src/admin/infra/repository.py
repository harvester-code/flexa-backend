import awswrangler as wr
import boto3
import pandas as pd

from typing import Union, List
from sqlalchemy import Connection, update, true, desc, bindparam
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.inspection import inspect

from src.database import S3_SAVE_PATH
from src.admin.domain.repository import IAdminRepository
from src.admin.domain.admin import OperationSetting as OperationSettingVO
from src.admin.infra.models import OperationSetting


class AdminRepository(IAdminRepository):

    async def fetch_operation_setting(self, db: AsyncSession, group_id: str):

        result = await db.execute(
            select(OperationSetting).where(OperationSetting.group_id == int(group_id))
        )

        operation_setting = result.scalars().all()

        return operation_setting

    async def create_operation_setting(
        self, db: AsyncSession, operation_setting: OperationSettingVO
    ):

        new_operation_setting = OperationSetting(
            id=operation_setting.id,
            group_id=operation_setting.group_id,
            terminal_name=operation_setting.terminal_name,
            terminal_process=operation_setting.terminal_process,
            terminal_layout=operation_setting.terminal_layout,
            terminal_layout_image_url=operation_setting.terminal_layout_image_url,
            processing_procedure=operation_setting.processing_procedure,
        )

        db.add(new_operation_setting)
        await db.commit()

    async def update_operation_setting(
        self,
        db: AsyncSession,
        id: str,
        terminal_name: str | None,
        terminal_process: dict | None,
        processing_procedure: dict | None,
        terminal_layout: dict | None,
        terminal_layout_image_url: str | None,
    ):
        values_to_update = {}

        if terminal_name:
            values_to_update[OperationSetting.terminal_name] = terminal_name
        if terminal_process:
            values_to_update[OperationSetting.terminal_process] = terminal_process
        if processing_procedure:
            values_to_update[OperationSetting.processing_procedure] = (
                processing_procedure
            )
        if terminal_layout:
            values_to_update[OperationSetting.terminal_layout] = terminal_layout
        if terminal_layout_image_url:
            values_to_update[OperationSetting.terminal_layout_image_url] = (
                terminal_layout_image_url
            )

        await db.execute(
            update(OperationSetting)
            .where(OperationSetting.id == id)
            .values(values_to_update)
        )
        await db.commit()

    async def deactivate_operation_setting(
        self,
        db: AsyncSession,
    ):

        await db.execute(
            update(OperationSetting)
            .where(OperationSetting.id == id)
            .values(is_active=False)
        )
        await db.commit()
