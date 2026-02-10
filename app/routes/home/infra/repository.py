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
        """캐시가 유효한지 확인 (simulation-pax.parquet 수정일과 비교)
        
        캐시가 parquet 파일보다 최신이면 유효, 오래되었으면 무효
        """
        logger.info(f"  🔎 [REPO] Checking cache metadata for: {cache_filename}")
        
        # 1. 캐시 파일 메타데이터 조회
        cache_metadata = await self.s3_manager.get_metadata_async(scenario_id, cache_filename)
        if not cache_metadata:
            logger.info(f"  ❌ [REPO] Cache file NOT FOUND in S3: {cache_filename}")
            return False
        
        logger.info(f"  ✅ [REPO] Cache file exists in S3")
        
        # 2. parquet 파일 메타데이터 조회
        parquet_metadata = await self.s3_manager.get_metadata_async(scenario_id, "simulation-pax.parquet")
        if not parquet_metadata:
            logger.warning(f"  ⚠️ [REPO] Parquet file not found for scenario_id={scenario_id}")
            return False
        
        cache_modified = cache_metadata.get('last_modified')
        parquet_modified = parquet_metadata.get('last_modified')
        
        if not cache_modified or not parquet_modified:
            logger.warning(f"  ⚠️ [REPO] Missing modification timestamps")
            return False
        
        # 3. 타임스탬프 비교: 캐시가 parquet보다 최신이면 유효
        is_valid = cache_modified > parquet_modified
        
        if is_valid:
            logger.info(f"  ✅ [REPO] Cache is NEWER than parquet")
            logger.info(f"      📅 Cache modified:   {cache_modified}")
            logger.info(f"      📅 Parquet modified: {parquet_modified}")
        else:
            logger.info(f"  ❌ [REPO] Cache is OLDER than parquet (needs refresh)")
            logger.info(f"      📅 Cache modified:   {cache_modified}")
            logger.info(f"      📅 Parquet modified: {parquet_modified}")
        
        return is_valid

    async def load_cached_response(self, scenario_id: str, cache_filename: str) -> Optional[dict]:
        """캐시된 응답 로드"""
        logger.info(f"  📥 [REPO] Loading cached JSON from S3...")
        cached_data = await self.s3_manager.get_json_async(scenario_id, cache_filename)
        if cached_data:
            logger.info(f"  ✅ [REPO] Successfully loaded cached response")
        else:
            logger.warning(f"  ⚠️ [REPO] Failed to load cached response")
        return cached_data

    async def save_cached_response(self, scenario_id: str, cache_filename: str, data: dict) -> bool:
        """계산된 응답을 캐시에 저장"""
        logger.info(f"  💾 [REPO] Saving computed result to S3: {cache_filename}")
        success = await self.s3_manager.save_json_async(scenario_id, cache_filename, data)
        if success:
            logger.info(f"  ✅ [REPO] Cache saved successfully to S3")
        else:
            logger.error(f"  ❌ [REPO] FAILED to save cache to S3")
        return success

    async def delete_old_caches(self, scenario_id: str, prefix: str, keep_filename: str) -> List[str]:
        """현재 버전을 제외한 이전 캐시 파일 삭제
        
        Args:
            scenario_id: 시나리오 ID
            prefix: 캐시 파일 prefix (예: "home-static-response-")
            keep_filename: 삭제하지 않을 현재 캐시 파일명
            
        Returns:
            삭제된 파일명 리스트
        """
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
                logger.info(f"  🗑️ [REPO] Deleted old cache: {old_file}")
            else:
                logger.warning(f"  ⚠️ [REPO] Failed to delete old cache: {old_file}")
        
        return deleted
