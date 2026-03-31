import hashlib
import glob
import os
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, status
from loguru import logger

from app.routes.home.application.core.home_analyzer import HomeAnalyzer
from app.routes.home.application.core.timeline_builder import build_passenger_timelines
from app.routes.home.infra.repository import HomeRepository


def _compute_code_hash() -> str:
    """계산 로직 관련 소스 파일들의 해시를 생성합니다.
    
    서버 시작 시 1회 계산되며, 코드가 변경되면 해시가 바뀌어
    캐시가 자동으로 무효화됩니다.
    """
    hasher = hashlib.md5()
    
    # 캐시에 영향을 주는 소스 파일들의 디렉토리
    base_dir = os.path.dirname(__file__)
    target_dirs = [
        base_dir,                           # application/ (service.py 등)
        os.path.join(base_dir, "core"),     # application/core/ (home_analyzer.py 등)
    ]
    
    file_paths = []
    for target_dir in target_dirs:
        file_paths.extend(sorted(glob.glob(os.path.join(target_dir, "*.py"))))
    
    for file_path in file_paths:
        with open(file_path, "rb") as f:
            hasher.update(f.read())
    
    code_hash = hasher.hexdigest()[:8]  # 앞 8자리만 사용
    logger.info(f"[CACHE] Code hash computed: {code_hash} (from {len(file_paths)} files)")
    return code_hash


# 서버 시작 시 1회 계산 (모듈 로드 시점)
_CODE_HASH = _compute_code_hash()


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
        percentile_mode: str = "cumulative",
    ) -> HomeAnalyzer:
        return HomeAnalyzer(
            pax_df,
            percentile,
            process_flow=process_flow,
            metadata=metadata,
            country_to_airports_path=self.country_to_airports_path,
            interval_minutes=interval_minutes,
            percentile_mode=percentile_mode,
        )

    async def fetch_static_data(
        self, scenario_id: str, interval_minutes: int = 60
    ) -> Dict[str, Any]:
        """KPI와 무관한 정적 데이터 반환 (S3 캐싱 지원)
        
        로직:
        1. S3에서 캐시된 응답 파일 확인 (코드 해시 포함 파일명)
        2. 캐시가 있고 유효하면 (parquet보다 최신 + 코드 해시 일치) → 캐시 반환
        3. 캐시가 없거나 오래되었으면 → 새로 계산 + S3에 저장
        """
        logger.info(f"=" * 80)
        logger.info(f"🔍 [CACHE CHECK START] scenario_id={scenario_id}")
        cache_filename = f"home-static-response-{_CODE_HASH}.json"
        
        # 1. 캐시가 유효한지 확인 (parquet 수정일 비교)
        logger.info(f"📋 Checking cache validity for: {cache_filename} (code_hash={_CODE_HASH})")
        is_valid = await self.home_repo.is_cache_valid(scenario_id, cache_filename)
        logger.info(f"📊 Cache validation result: is_valid={is_valid}")
        
        if is_valid:
            # 2. 유효한 캐시가 있으면 바로 반환
            logger.info(f"✅ Cache is valid! Attempting to load cached data...")
            cached_data = await self.home_repo.load_cached_response(scenario_id, cache_filename)
            if cached_data:
                logger.info(f"🚀 [CACHE HIT] Returning cached static data (NO COMPUTATION)")
                logger.info(f"=" * 80)
                return cached_data
            else:
                logger.warning(f"⚠️ Cache was valid but failed to load data")
        
        # 3. 캐시가 없거나 오래됨 → 새로 계산
        logger.info(f"❌ [CACHE MISS] Cache invalid or not found")
        logger.info(f"⚙️ [COMPUTING] Starting heavy computation (parquet + analysis)...")
        pax_df = await self._get_pax_dataframe(scenario_id)
        logger.info(f"📦 Loaded parquet data: {len(pax_df)} rows")
        
        process_flow = await self._load_process_flow(scenario_id)
        logger.info(f"🔄 Creating calculator and computing charts...")
        calculator = self._create_calculator(
            pax_df, process_flow=process_flow, interval_minutes=interval_minutes
        )

        logger.info(f"📊 Computing flow_chart...")
        flow_chart = calculator.get_flow_chart_data()
        logger.info(f"📊 Computing histogram...")
        histogram = calculator.get_histogram_data()
        logger.info(f"📊 Computing sankey_diagram...")
        sankey = calculator.get_sankey_diagram_data()
        
        result = {
            "flow_chart": flow_chart,
            "histogram": histogram,
            "sankey_diagram": sankey,
        }
        
        logger.info(f"✅ Computation complete! Saving to cache...")
        # 4. 계산된 결과를 S3에 캐시로 저장
        save_success = await self.home_repo.save_cached_response(scenario_id, cache_filename, result)
        logger.info(f"💾 Cache save result: success={save_success}")
        
        # 5. 이전 버전 캐시 파일 정리
        if save_success:
            deleted = await self.home_repo.delete_old_caches(
                scenario_id,
                prefix="home-static-response-",
                keep_filename=cache_filename,
            )
            if deleted:
                logger.info(f"🗑️ Deleted {len(deleted)} old cache(s): {deleted}")
        
        logger.info(f"=" * 80)
        
        return result

    async def fetch_passenger_timelines(self, scenario_id: str) -> Dict[str, Any]:
        """승객별 타임라인 데이터 반환 (3D 뷰어용, S3 캐싱 지원)"""
        logger.info(f"={'=' * 79}")
        logger.info(f"[TIMELINE] scenario_id={scenario_id}")

        cache_filename = f"passenger-timelines-{_CODE_HASH}.json"

        is_valid = await self.home_repo.is_cache_valid(scenario_id, cache_filename)
        if is_valid:
            cached = await self.home_repo.load_cached_response(scenario_id, cache_filename)
            if cached:
                logger.info("[TIMELINE] Cache hit – returning cached data")
                return cached

        logger.info("[TIMELINE] Cache miss – computing timelines from parquet")
        pax_df = await self._get_pax_dataframe(scenario_id)
        metadata = await self._get_metadata(scenario_id)

        result = build_passenger_timelines(pax_df, metadata)

        save_ok = await self.home_repo.save_cached_response(scenario_id, cache_filename, result)
        if save_ok:
            await self.home_repo.delete_old_caches(
                scenario_id,
                prefix="passenger-timelines-",
                keep_filename=cache_filename,
            )

        logger.info(f"={'=' * 79}")
        return result

    async def fetch_metrics_data(
        self,
        scenario_id: str,
        percentile: Optional[int] = None,
        percentile_mode: str = "cumulative",
    ) -> Dict[str, Any]:
        """KPI 의존적 메트릭 데이터 반환
        
        Args:
            percentile_mode: "cumulative" (Top N% 평균) 또는 "quantile" (정확한 분위값)
        """

        pax_df = await self._get_pax_dataframe(scenario_id)
        metadata = await self._get_metadata(scenario_id, required=True)
        process_flow = await self._load_process_flow(scenario_id)
        calculator = self._create_calculator(
            pax_df,
            percentile,
            process_flow=process_flow,
            metadata=metadata,
            percentile_mode=percentile_mode,
        )

        return {
            "summary": calculator.get_summary(),
            "facility_details": calculator.get_facility_details(),
        }
