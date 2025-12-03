from typing import Optional

import pandas as pd

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
        return await self.s3_manager.get_json_async(
            scenario_id=scenario_id, filename=filename
        )
