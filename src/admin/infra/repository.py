from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from src.admin.domain.admin import OperationSetting as OperationSettingVO
from src.admin.domain.repository import IAdminRepository
from src.admin.infra.models import Groups, OperationSetting


class AdminRepository(IAdminRepository):

    async def fetch_operation_setting(self, db: AsyncSession, group_id: str):

        async with db.begin():
            result = await db.execute(
                select(OperationSetting).where(
                    OperationSetting.group_id == int(group_id)
                )
            )

            operation_setting = result.scalars().all()

            result = await db.execute(
                select(Groups.group_name).where(Groups.id == int(group_id))
            )

            group_name = result.mappings().first()

        return {"operation_setting": operation_setting, "group_name": group_name}

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

    async def deactivate_operation_setting(self, db: AsyncSession, id: str):

        await db.execute(
            update(OperationSetting)
            .where(OperationSetting.id == id)
            .values(is_active=False)
        )
        await db.commit()

    async def update_group_name(self, db: AsyncSession, id: str, group_name: str):

        await db.execute(
            update(Groups)
            .where(Groups.id == int(id))
            .values({Groups.group_name: group_name})
        )
        await db.commit()
