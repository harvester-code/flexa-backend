import awswrangler as wr

from app.routes.facility.domain.repository import IFacilityRepository
from packages.boto3_session import boto3_session


class FacilityRepository(IFacilityRepository):

    async def download_from_s3(self, scenario_id: str):
        df = wr.s3.read_parquet(
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/simulation-results-raw-data/{scenario_id}.parquet",
            boto3_session=boto3_session,
        )

        return df
