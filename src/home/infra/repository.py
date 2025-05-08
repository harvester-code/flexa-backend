import awswrangler as wr

from src.home.domain.repository import IHomeRepository


class HomeRepository(IHomeRepository):
    async def download_from_s3(self, scenario_id: str):

        return wr.s3.read_parquet(
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/simulation-results-raw-data/{scenario_id}.parquet",
        )
