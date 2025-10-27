from typing import Any, Dict, Optional, List

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

    async def _load_process_flow(
        self, scenario_id: Optional[str]
    ) -> Optional[List[dict]]:
        if scenario_id is None:
            return None

        metadata = await self.s3_manager.get_json_async(
            scenario_id=scenario_id,
            filename="metadata-for-frontend.json",
        )
        if not metadata:
            return None

        process_flow = metadata.get("process_flow")
        if isinstance(process_flow, list):
            return process_flow
        return None

    def _create_calculator(
        self,
        pax_df: Any,
        percentile: Optional[int] = None,
        process_flow: Optional[List[dict]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> HomeAnalyzer:
        return HomeAnalyzer(pax_df, percentile, process_flow=process_flow, metadata=metadata)

    async def fetch_static_data(
        self, scenario_id: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """KPI와 무관한 정적 데이터 반환"""

        pax_df = await self._get_cached_data(scenario_id)
        if pax_df is None:
            return None

        process_flow = await self._load_process_flow(scenario_id)
        calculator = self._create_calculator(pax_df, process_flow=process_flow)

        return {
            "alert_issues": calculator.get_alert_issues(),
            "flow_chart": calculator.get_flow_chart_data(),
            "histogram": calculator.get_histogram_data(),
            "sankey_diagram": calculator.get_sankey_diagram_data(),
        }

    async def fetch_metrics_data(
        self,
        scenario_id: Optional[str],
        percentile: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        """KPI 의존적 메트릭 데이터 반환"""

        pax_df = await self._get_cached_data(scenario_id)
        if pax_df is None:
            return None

        # metadata 로드 (facility_metrics 계산을 위해)
        metadata = await self.s3_manager.get_json_async(
            scenario_id=scenario_id,
            filename="metadata-for-frontend.json",
        )

        calculator = self._create_calculator(
            pax_df,
            percentile,
            metadata=metadata,
        )

        return {
            "summary": calculator.get_summary(),
            "facility_details": calculator.get_facility_details(),
        }
