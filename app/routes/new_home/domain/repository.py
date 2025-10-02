from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Optional, Dict, Any

import pandas as pd


class INewHomeRepository(metaclass=ABCMeta):
    """Data access contract for new home analytics."""

    @abstractmethod
    async def load_passenger_dataframe(self, scenario_id: str) -> Optional[pd.DataFrame]:
        raise NotImplementedError

    @abstractmethod
    async def load_metadata(self, scenario_id: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError
