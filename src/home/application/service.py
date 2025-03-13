from dependency_injector.wiring import inject
from src.home.domain.repository import IHomeRepository
from src.home.application.core.calculator import HomeCalculator


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
        self.calculator = HomeCalculator()

    async def login_supabase(self, email: str, password: str):
        return await self.home_repo.login_supabase(email, password)

    async def fetch_supabase_data(self):
        return await self.home_repo.fetch_supabase_data()

    async def fetch_simulation_files(self):
        return await self.home_repo.fetch_simulation_files()

    # =====================================
    # NOTE: Home 화면 요약 정보

    async def fetch_simulation_summary(self, file_id: str):
        df = await self.home_repo.fetch_simulation_summary(file_id)
        return {
            "status": "success",
            "data": {
                "time_range": self.calculator.get_time_range(df),
                "summary": {
                    "flights": self.calculator.get_flight_summary(df),
                    "pax": self.calculator.get_pax_summary(df),
                    "kpi": self.calculator.get_kpi(df),
                },
            },
        }
