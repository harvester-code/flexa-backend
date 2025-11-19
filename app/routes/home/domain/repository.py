from abc import ABCMeta, abstractmethod
from typing import Any, Optional

import pandas as pd


class IHomeRepository(metaclass=ABCMeta):
    """홈 화면에서 사용하는 저장소 인터페이스"""

    @abstractmethod
    async def load_simulation_parquet(self, scenario_id: str) -> Optional[pd.DataFrame]:
        """시나리오별 승객 데이터를 parquet에서 로드"""
        raise NotImplementedError

    @abstractmethod
    async def load_metadata(self, scenario_id: str, filename: str) -> Optional[dict]:
        """시나리오별 메타데이터(JSON)를 로드"""
        raise NotImplementedError

    @abstractmethod
    async def is_cache_valid(self, scenario_id: str, cache_filename: str) -> bool:
        """캐시가 유효한지 확인 (parquet 수정일과 비교)"""
        raise NotImplementedError

    @abstractmethod
    async def load_cached_response(self, scenario_id: str, cache_filename: str) -> Optional[dict]:
        """캐시된 응답 로드"""
        raise NotImplementedError

    @abstractmethod
    async def save_cached_response(self, scenario_id: str, cache_filename: str, data: dict) -> bool:
        """계산된 응답을 캐시에 저장"""
        raise NotImplementedError
