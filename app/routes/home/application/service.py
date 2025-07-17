from typing import Optional, Dict, Any
from dependency_injector.wiring import inject

from packages.calculator.calculator import Calculator
from app.routes.home.infra.repository import HomeRepository


class HomeService:

    @inject
    def __init__(self, home_repo: HomeRepository):
        self.home_repo = home_repo

    def _get_cached_data(
        self, scenario_id: Optional[str]
    ) -> tuple[Optional[Any], Optional[dict]]:
        if scenario_id is None:
            return None, None

        pax_df = self.home_repo.download_simulation_parquet_from_s3(scenario_id)
        if pax_df is None:
            return None, None

        facility_info = self.home_repo.download_facility_json_from_s3(scenario_id)
        if facility_info is None:
            return None, None

        return pax_df, facility_info

    def _create_calculator(
        self,
        pax_df: Any,
        calculate_type: str,
        facility_info: Optional[dict] = None,
        percentile: Optional[int] = None,
    ) -> Calculator:
        return Calculator(pax_df, facility_info, calculate_type, percentile)

    def fetch_common_home_data(
        self, scenario_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """KPI와 무관한 공통 데이터 반환"""
        pax_df, facility_info = self._get_cached_data(scenario_id)
        if pax_df is None or facility_info is None:
            return None

        calculator = self._create_calculator(pax_df, "mean", facility_info)

        return {
            "topview_service_point": calculator.get_topview_service_point_data(),
            "topview_data": calculator.get_topview_data(),
            "alert_issues": calculator.get_alert_issues(),
            "flow_chart": calculator.get_flow_chart_data(),
            "histogram": calculator.get_histogram_data(),
            "sankey_diagram": calculator.get_sankey_diagram_data(),
        }

    def fetch_kpi_home_data(
        self,
        scenario_id: Optional[str],
        calculate_type: str,
        percentile: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """KPI 의존적 데이터 반환"""
        pax_df, facility_info = self._get_cached_data(scenario_id)
        if pax_df is None or facility_info is None:
            return None

        calculator = self._create_calculator(
            pax_df, calculate_type, facility_info, percentile
        )

        return {
            "summary": calculator.get_summary(),
            "facility_details": calculator.get_facility_details(),
        }

    def fetch_aemos_template(self, scenario_id: str | None):
        pax_df, facility_info = self._get_cached_data(scenario_id)
        if pax_df is None or facility_info is None:
            return None

        calculator = self._create_calculator(pax_df, facility_info)
        return calculator.get_aemos_template()
