from dependency_injector.wiring import inject
from src.home.domain.repository import IHomeRepository
from src.home.application.core.calculator import HomeCalculator
import boto3


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

    async def fetch_summary(
        self,
        session: boto3.Session,
        scenario_id: str | None,
        calculate_type: str,
        percentile: int | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df, calculate_type, percentile)
        return {
            "status": "success",
            "data": {
                "time_range": calculator.get_time_range(),
                "summary": {
                    "flights": calculator.get_flight_summary(),
                    "pax": calculator.get_pax_summary(),
                    "kpi": calculator.get_kpi(),
                },
            },
        }

    async def fetch_alert_issues(
        self,
        session: boto3.Session,
        scenario_id: str | None,
        calculate_type: str,
        percentile: int | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df, calculate_type, percentile)
        return {
            "status": "success",
            "data": {
                "facility_times_with_peak": calculator.get_facility_times_with_peak(),
            },
        }
