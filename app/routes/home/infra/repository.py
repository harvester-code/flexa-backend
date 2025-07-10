from app.routes.home.domain.repository import IHomeRepository
from packages.aws.s3.s3_downloader import S3Downloader


class HomeRepository(IHomeRepository):
    def __init__(self, s3_downloader: S3Downloader):
        self.s3_downloader = s3_downloader

    async def download_simulation_parquet_from_s3(self, scenario_id: str):
        return await self.s3_downloader.download_simulation_parquet_from_s3(scenario_id)

    async def download_facility_json_from_s3(self, scenario_id: str):
        return await self.s3_downloader.download_facility_json_from_s3(scenario_id)
