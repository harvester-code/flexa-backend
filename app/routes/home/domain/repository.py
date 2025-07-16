from abc import ABCMeta, abstractmethod
from typing import Optional, Tuple

import pandas as pd


class IHomeRepository(metaclass=ABCMeta):
    """
    홈 화면
    """

    @abstractmethod
    def download_simulation_parquet_from_s3(self, scenario_id: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def download_facility_json_from_s3(self, scenario_id: str) -> Optional[dict]:
        raise NotImplementedError
