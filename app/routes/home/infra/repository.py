from typing import Optional

import pandas as pd
from loguru import logger

from app.routes.home.domain.repository import IHomeRepository
from packages.aws.s3.s3_manager import S3Manager


class HomeRepository(IHomeRepository):
    def __init__(self, s3_manager: S3Manager):
        self.s3_manager = s3_manager

    async def load_simulation_parquet(self, scenario_id: str) -> Optional[pd.DataFrame]:
        return await self.s3_manager.get_parquet_async(
            scenario_id, "simulation-pax.parquet"
        )

    async def load_metadata(self, scenario_id: str, filename: str) -> Optional[dict]:
        return await self.s3_manager.get_json_async(scenario_id=scenario_id, filename=filename)

    async def is_cache_valid(self, scenario_id: str, cache_filename: str) -> bool:
        """캐시가 유효한지 확인 (simulation-pax.parquet 수정일과 비교)
        
        캐시가 parquet 파일보다 최신이면 유효, 오래되었으면 무효
        """
        # 1. 캐시 파일 메타데이터 조회
        cache_metadata = await self.s3_manager.get_metadata_async(scenario_id, cache_filename)
        if not cache_metadata:
            logger.debug(f"Cache file not found: {cache_filename}")
            return False
        
        # 2. parquet 파일 메타데이터 조회
        parquet_metadata = await self.s3_manager.get_metadata_async(scenario_id, "simulation-pax.parquet")
        if not parquet_metadata:
            logger.warning(f"Parquet file not found for scenario_id={scenario_id}")
            return False
        
        cache_modified = cache_metadata.get('last_modified')
        parquet_modified = parquet_metadata.get('last_modified')
        
        if not cache_modified or not parquet_modified:
            logger.warning(f"Missing modification timestamps for scenario_id={scenario_id}")
            return False
        
        # 3. 타임스탬프 비교: 캐시가 parquet보다 최신이면 유효
        is_valid = cache_modified > parquet_modified
        
        if is_valid:
            logger.info(f"✅ Cache valid for {scenario_id}: cache={cache_modified}, parquet={parquet_modified}")
        else:
            logger.info(f"🔄 Cache outdated for {scenario_id}: cache={cache_modified}, parquet={parquet_modified}")
        
        return is_valid

    async def load_cached_response(self, scenario_id: str, cache_filename: str) -> Optional[dict]:
        """캐시된 응답 로드"""
        cached_data = await self.s3_manager.get_json_async(scenario_id, cache_filename)
        if cached_data:
            logger.info(f"📦 Loaded cached response for {scenario_id}")
        return cached_data

    async def save_cached_response(self, scenario_id: str, cache_filename: str, data: dict) -> bool:
        """계산된 응답을 캐시에 저장"""
        success = await self.s3_manager.save_json_async(scenario_id, cache_filename, data)
        if success:
            logger.info(f"💾 Saved cache for {scenario_id}: {cache_filename}")
        else:
            logger.error(f"❌ Failed to save cache for {scenario_id}")
        return success
