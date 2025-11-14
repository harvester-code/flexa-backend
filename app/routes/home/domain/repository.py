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
