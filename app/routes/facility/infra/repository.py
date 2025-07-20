import awswrangler as wr

from app.routes.facility.domain.repository import IFacilityRepository
from packages.boto3_session import boto3_session
from packages.constants import S3_BUCKET_NAME


class FacilityRepository(IFacilityRepository):

    async def download_from_s3(self, scenario_id: str):
        df = wr.s3.read_parquet(
            path=f"s3://{S3_BUCKET_NAME}/simulations/simulation-results-raw-data/{scenario_id}.parquet",
            boto3_session=boto3_session,
        )

        return df
