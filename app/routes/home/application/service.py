from typing import Any, Dict, Optional

from dependency_injector.wiring import inject

from app.routes.home.infra.repository import HomeRepository
from app.routes.home.application.core.home_analyzer import HomeAnalyzer
from packages.aws.s3.s3_manager import S3Manager


class HomeService:

    @inject
    def __init__(self, home_repo: HomeRepository, s3_manager: S3Manager):
        self.home_repo = home_repo
        self.s3_manager = s3_manager  # 새로운 S3Manager 추가

    async def _get_cached_data(
        self, scenario_id: Optional[str]
    ) -> Optional[Any]:
        if scenario_id is None:
            return None

        # S3Manager를 통한 데이터 로드
        pax_df = await self.s3_manager.get_parquet_async(scenario_id, "simulation-pax.parquet")
        if pax_df is not None:
            print(f"✅ S3Manager로 성공적으로 데이터 로드: {len(pax_df)} rows")
            return pax_df
            
        print("❌ 데이터 로드 실패")
        return None

    def _create_calculator(
        self,
        pax_df: Any,
        calculate_type: str,
        percentile: Optional[int] = None,
    ) -> HomeAnalyzer:
        return HomeAnalyzer(pax_df, None, calculate_type, percentile)

    async def fetch_common_home_data(
        self, scenario_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """KPI와 무관한 공통 데이터 반환"""

        pax_df = await self._get_cached_data(scenario_id)
        if pax_df is None:
            return None

        calculator = self._create_calculator(pax_df, "mean")

        return {
            "alert_issues": calculator.get_alert_issues(),
            "flow_chart": calculator.get_flow_chart_data(),
            "histogram": calculator.get_histogram_data(),
            "sankey_diagram": calculator.get_sankey_diagram_data(),
            "etc_info": calculator.get_etc_info(),
        }

    async def fetch_kpi_home_data(
        self,
        scenario_id: Optional[str],
        calculate_type: str,
        percentile: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """KPI 의존적 데이터 반환"""

        pax_df = await self._get_cached_data(scenario_id)
        if pax_df is None:
            return None

        calculator = self._create_calculator(
            pax_df, calculate_type, percentile
        )

        return {
            "summary": calculator.get_summary(),
            "facility_details": calculator.get_facility_details(),
        }

    async def fetch_aemos_template(
        self, scenario_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """AEMOS 데이터 반환"""

        pax_df = await self._get_cached_data(scenario_id)

        if pax_df is None:
            return None

        calculator = self._create_calculator(pax_df, "mean")

        return calculator.get_aemos_template()
