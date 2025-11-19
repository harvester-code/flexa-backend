import os
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from loguru import logger

from app.routes.home.application.core.home_analyzer import HomeAnalyzer
from app.routes.home.infra.repository import HomeRepository


class HomeService:
    def __init__(self, home_repo: HomeRepository):
        self.home_repo = home_repo
        self.country_to_airports_path = os.getenv(
            "COUNTRY_TO_AIRPORTS_PATH",
            os.path.join(os.path.dirname(__file__), "country_to_airports.json"),
        )

    async def _get_pax_dataframe(self, scenario_id: str):
        if not scenario_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="scenario_id is required.",
            )
        pax_df = await self.home_repo.load_simulation_parquet(scenario_id)
        if pax_df is None:
            logger.warning(f"Simulation parquet not found for scenario_id={scenario_id}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Simulation data not found for the requested scenario.",
            )
        return pax_df

    async def _get_metadata(
        self, scenario_id: str, required: bool = False
    ) -> Optional[Dict[str, Any]]:
        metadata = await self.home_repo.load_metadata(
            scenario_id, "metadata-for-frontend.json"
        )

        if metadata is None:
            missing_msg = f"Metadata not found for scenario_id={scenario_id}"
            if required:
                logger.warning(missing_msg)
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Metadata not found for the requested scenario.",
                )
            logger.debug(missing_msg)
        return metadata

    async def _load_process_flow(self, scenario_id: str) -> Optional[List[dict]]:
        metadata = await self._get_metadata(scenario_id)
        if not metadata:
            return None

        process_flow = metadata.get("process_flow")
        return process_flow if isinstance(process_flow, list) else None

    def _create_calculator(
        self,
        pax_df: Any,
        percentile: Optional[int] = None,
        process_flow: Optional[List[dict]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        interval_minutes: int = 60,
    ) -> HomeAnalyzer:
        return HomeAnalyzer(
            pax_df,
            percentile,
            process_flow=process_flow,
            metadata=metadata,
            country_to_airports_path=self.country_to_airports_path,
            interval_minutes=interval_minutes,
        )

    async def fetch_static_data(
        self, scenario_id: str, interval_minutes: int = 60
    ) -> Dict[str, Any]:
        """KPI와 무관한 정적 데이터 반환 (S3 캐싱 지원)
        
        로직:
        1. S3에서 캐시된 응답 파일 확인
        2. 캐시가 있고 유효하면 (parquet보다 최신) → 캐시 반환
        3. 캐시가 없거나 오래되었으면 → 새로 계산 + S3에 저장
        """
        cache_filename = "home-static-response.json"
        
        # 1. 캐시가 유효한지 확인 (parquet 수정일 비교)
        is_valid = await self.home_repo.is_cache_valid(scenario_id, cache_filename)
        
        if is_valid:
            # 2. 유효한 캐시가 있으면 바로 반환
            cached_data = await self.home_repo.load_cached_response(scenario_id, cache_filename)
            if cached_data:
                logger.info(f"🚀 Returning cached static data for {scenario_id}")
                return cached_data
        
        # 3. 캐시가 없거나 오래됨 → 새로 계산
        logger.info(f"⚙️ Computing static data for {scenario_id}")
        pax_df = await self._get_pax_dataframe(scenario_id)
        process_flow = await self._load_process_flow(scenario_id)
        calculator = self._create_calculator(
            pax_df, process_flow=process_flow, interval_minutes=interval_minutes
        )

        result = {
            "flow_chart": calculator.get_flow_chart_data(),
            "histogram": calculator.get_histogram_data(),
            "sankey_diagram": calculator.get_sankey_diagram_data(),
        }
        
        # 4. 계산된 결과를 S3에 캐시로 저장
        await self.home_repo.save_cached_response(scenario_id, cache_filename, result)
        
        return result

    async def fetch_metrics_data(
        self, scenario_id: str, percentile: Optional[int] = None
    ) -> Dict[str, Any]:
        """KPI 의존적 메트릭 데이터 반환"""

        pax_df = await self._get_pax_dataframe(scenario_id)
        metadata = await self._get_metadata(scenario_id, required=True)
        calculator = self._create_calculator(
            pax_df,
            percentile,
            metadata=metadata,
        )

        return {
            "summary": calculator.get_summary(),
            "facility_details": calculator.get_facility_details(),
        }
