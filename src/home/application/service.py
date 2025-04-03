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

    async def fetch_line_queue(
        self,
        session: boto3.Session,
        scenario_id: str | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df)
        return calculator.get_line_queue()

    async def fetch_summary(
        self,
        session: boto3.Session,
        scenario_id: str | None,
        calculate_type: str,
        percentile: int | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df, calculate_type, percentile)
        return calculator.get_summary()

    async def fetch_alert_issues(
        self,
        session: boto3.Session,
        scenario_id: str | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df)
        return calculator.get_alert_issues()

    async def fetch_facility_details(
        self,
        session: boto3.Session,
        scenario_id: str | None,
        calculate_type: str,
        percentile: int | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df, calculate_type, percentile)
        return calculator.get_facility_details()

    async def fetch_flow_chart(
        self,
        session: boto3.Session,
        scenario_id: str | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df)
        return calculator.get_flow_chart_data()

    async def fetch_histogram(
        self,
        session: boto3.Session,
        scenario_id: str | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df)
        return calculator.get_histogram_data()

    async def fetch_sankey_diagram(
        self,
        session: boto3.Session,
        scenario_id: str | None,
    ):
        pax_df = await self.home_repo.download_from_s3(session, scenario_id)
        calculator = HomeCalculator(pax_df)
        return calculator.get_sankey_diagram_data()
