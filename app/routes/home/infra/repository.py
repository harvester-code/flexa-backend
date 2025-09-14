from app.routes.home.domain.repository import IHomeRepository
from packages.aws.s3.s3_manager import S3Manager


class HomeRepository(IHomeRepository):
    def __init__(self, s3_manager: S3Manager):
        self.s3_manager = s3_manager

    async def download_simulation_parquet_from_s3(self, scenario_id: str):
        return await self.s3_manager.get_parquet_async(scenario_id, "simulation-pax.parquet")
