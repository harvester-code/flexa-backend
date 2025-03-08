from dependency_injector.wiring import inject
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

    async def fetch_supabase_data(self):
        return await self.home_repo.fetch_supabase_data()

    async def fetch_simulation_files(self):
        return await self.home_repo.fetch_simulation_files()

    async def fetch_simulation_summary(self, file_id: str):
        return await self.home_repo.fetch_simulation_summary(file_id)
