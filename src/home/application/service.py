from dependency_injector.wiring import inject
from fastapi import Request
from src.home.domain.repository import IHomeRepository


class HomeService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        home_repo: IHomeRepository,
    ):
        self.home_repo = home_repo

    async def login_supabase(self, email: str, password: str):
        return await self.home_repo.login_supabase(email, password)

    async def fetch_supabase_data(self):
        return await self.home_repo.fetch_supabase_data()

    async def fetch_simulation_files(self):
        return await self.home_repo.fetch_simulation_files()

    async def fetch_simulation_summary(self, file_id: str):
        return await self.home_repo.fetch_simulation_summary(file_id)
