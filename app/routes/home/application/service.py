from dependency_injector.wiring import inject

from packages.calculator.calculator import Calculator
from app.routes.home.infra.repository import HomeRepository


class HomeService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(self, home_repo: HomeRepository):
        self.home_repo = home_repo

    async def fetch_line_queue(self, scenario_id: str | None):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        facility_info = await self.home_repo.download_facility_json_from_s3(scenario_id)
        calculator = Calculator(pax_df, facility_info)
        return calculator.get_terminal_overview_line_queue()

    async def fetch_summary(
        self,
        scenario_id: str | None,
        calculate_type: str,
        percentile: int | None,
    ):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        facility_info = await self.home_repo.download_facility_json_from_s3(scenario_id)
        calculator = Calculator(pax_df, facility_info, calculate_type, percentile)
        return calculator.get_summary()

    async def fetch_alert_issues(self, scenario_id: str | None):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        facility_info = await self.home_repo.download_facility_json_from_s3(scenario_id)
        calculator = Calculator(pax_df, facility_info)
        return calculator.get_alert_issues()

    async def fetch_facility_details(
        self, scenario_id: str | None, calculate_type: str, percentile: int | None
    ):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        facility_info = await self.home_repo.download_facility_json_from_s3(scenario_id)
        calculator = Calculator(pax_df, facility_info, calculate_type, percentile)
        return calculator.get_facility_details()

    async def fetch_flow_chart(self, scenario_id: str | None):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        calculator = Calculator(pax_df)
        return calculator.get_flow_chart_data()

    async def fetch_histogram(self, scenario_id: str | None):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        calculator = Calculator(pax_df)
        return calculator.get_histogram_data()

    async def fetch_sankey_diagram(self, scenario_id: str | None):
        pax_df = await self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        calculator = Calculator(pax_df)
        return calculator.get_sankey_diagram_data()
