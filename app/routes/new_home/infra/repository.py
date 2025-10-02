from __future__ import annotations

from typing import Optional, Dict, Any

import pandas as pd

from app.routes.new_home.domain.repository import INewHomeRepository
from packages.aws.s3.s3_manager import S3Manager


class NewHomeRepository(INewHomeRepository):
    def __init__(self, s3_manager: S3Manager):
        self.s3_manager = s3_manager

    async def load_passenger_dataframe(self, scenario_id: str) -> Optional[pd.DataFrame]:
        return await self.s3_manager.get_parquet_async(
            scenario_id=scenario_id,
            filename="simulation-pax.parquet",
        )

    async def load_metadata(self, scenario_id: str) -> Optional[Dict[str, Any]]:
        return await self.s3_manager.get_json_async(
            scenario_id=scenario_id,
            filename="metadata-for-frontend.json",
        )
