from dependency_injector.wiring import inject
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from src.admin.domain.admin import OperationSetting
from src.admin.domain.repository import IAdminRepository


class AdminService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        admin_repo: IAdminRepository,
    ):
        self.admin_repo = admin_repo

    # ======================================
    # NOTE: Operation Setting

    async def fetch_operation_setting(self, db: AsyncSession, group_id: str):

        settings = await self.admin_repo.fetch_operation_setting(db, group_id)

        return settings

    async def create_operation_setting(
        self,
        db: AsyncSession,
        group_id: str,
        terminal_name: str,
    ):
        id = str(ULID())

        operation_setting: OperationSetting = OperationSetting(
            id=id,
            group_id=int(group_id),
            terminal_name=terminal_name,
            terminal_process=None,
            processing_procedure=None,
            terminal_layout=None,
            terminal_layout_image_url=None,
        )

        await self.admin_repo.create_operation_setting(db, operation_setting)

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
        await self.admin_repo.update_operation_setting(
            db,
            id,
            terminal_name,
            terminal_process,
            processing_procedure,
            terminal_layout,
            terminal_layout_image_url,
        )

    async def deactivate_operation_setting(
        self,
        db: AsyncSession,
        id: str,
    ):
        await self.admin_repo.deactivate_operation_setting(db, id)

    # ======================================
    async def test(self):
        """"""
