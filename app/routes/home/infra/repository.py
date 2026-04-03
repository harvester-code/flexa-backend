from typing import List, Optional

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
        """캐시가 유효한지 확인 (simulation-pax.parquet 수정일과 비교)"""
        cache_metadata = await self.s3_manager.get_metadata_async(scenario_id, cache_filename)
        if not cache_metadata:
            logger.debug(f"[REPO] Cache not found: {scenario_id}/{cache_filename}")
            return False

        parquet_metadata = await self.s3_manager.get_metadata_async(scenario_id, "simulation-pax.parquet")
        if not parquet_metadata:
            logger.warning(f"[REPO] Parquet not found for {scenario_id}")
            return False

        cache_modified = cache_metadata.get('last_modified')
        parquet_modified = parquet_metadata.get('last_modified')

        if not cache_modified or not parquet_modified:
            logger.warning(f"[REPO] Missing timestamps for {scenario_id}")
            return False

        return cache_modified > parquet_modified

    async def load_cached_response(self, scenario_id: str, cache_filename: str) -> Optional[dict]:
        """캐시된 응답 로드"""
        cached_data = await self.s3_manager.get_json_async(scenario_id, cache_filename)
        if not cached_data:
            logger.warning(f"[REPO] Failed to load cache: {scenario_id}/{cache_filename}")
        return cached_data

    async def save_cached_response(self, scenario_id: str, cache_filename: str, data: dict) -> bool:
        """계산된 응답을 캐시에 저장"""
        success = await self.s3_manager.save_json_async(scenario_id, cache_filename, data)
        if not success:
            logger.error(f"[REPO] Failed to save cache: {scenario_id}/{cache_filename}")
        return success

    async def delete_old_caches(self, scenario_id: str, prefix: str, keep_filename: str) -> List[str]:
        """현재 버전을 제외한 이전 캐시 파일 삭제"""
        all_files = await self.s3_manager.list_files_async(scenario_id)
        old_caches = [
            f for f in all_files
            if f.startswith(prefix) and f != keep_filename
        ]

        deleted = []
        for old_file in old_caches:
            success = await self.s3_manager.delete_json_async(scenario_id, old_file)
            if success:
                deleted.append(old_file)
                logger.debug(f"[REPO] Deleted old cache: {scenario_id}/{old_file}")
            else:
                logger.warning(f"[REPO] Failed to delete: {scenario_id}/{old_file}")

        return deleted
